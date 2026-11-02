from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable

from delta_sharing_mirror.errors import SchemaMismatchError
from delta_sharing_mirror.writer import (
    NO_WATERMARK,
    CommitMetadata,
    DeltaWriter,
    align_to_target,
    merge_predicate,
    read_watermark,
)


def meta(version: int, mode: str = "snapshot") -> CommitMetadata:
    return CommitMetadata(version, "s.sch.t", mode, "run-1", "2026-01-01T00:00:00+00:00")


def test_read_watermark_prefers_newest_commit_with_key() -> None:
    history = [
        {"version": 0, "mirror.source_version": "3"},
        {"version": 2, "operation": "OPTIMIZE"},
        {"version": 1, "userMetadata": {"mirror.source_version": "5"}},
    ]
    assert read_watermark(history) == 5


def test_read_watermark_none() -> None:
    assert read_watermark([]) == NO_WATERMARK
    assert read_watermark([{"version": 0, "operation": "WRITE"}]) == NO_WATERMARK


def test_read_watermark_skips_unparseable() -> None:
    history = [
        {"version": 1, "mirror.source_version": "x"},
        {"version": 0, "mirror.source_version": 2},
    ]
    assert read_watermark(history) == 2


def test_merge_predicate_quotes_identifiers() -> None:
    assert merge_predicate(["id", "Region"]) == 't."id" = s."id" AND t."Region" = s."Region"'


def test_align_safe_cast_and_new_column() -> None:
    target = pa.schema([pa.field("id", pa.int32()), pa.field("v", pa.string())])
    source = pa.table({"id": pa.array([1], pa.int64()), "v": ["a"], "new": [1.5]})
    aligned = align_to_target(source, target)
    assert aligned.schema.field("id").type == pa.int32()
    assert aligned.schema.field("new").type == pa.float64()


def test_align_rejects_lossy_cast() -> None:
    target = pa.schema([pa.field("id", pa.int32())])
    with pytest.raises(SchemaMismatchError, match="column type change"):
        align_to_target(pa.table({"id": pa.array([2**40], pa.int64())}), target)
    with pytest.raises(SchemaMismatchError):
        align_to_target(pa.table({"id": pa.array(["x"], pa.string())}), target)


def test_snapshot_then_merge_round_trip(tmp_path: Path) -> None:
    uri = str(tmp_path / "t")
    writer = DeltaWriter({})
    assert writer.open(uri) is None
    snap = pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "qty": pa.array([1, None, 3], pa.int32()),
            "ingest_date": pa.array([dt.date(2026, 1, 1)] * 3, pa.date32()),
        }
    )
    res = writer.write_snapshot(
        uri, snap, partition_by=["ingest_date"], schema_mode="overwrite", metadata=meta(4)
    )
    assert res.rows_written == 3 and res.target_version == 0
    table = writer.open(uri)
    assert table is not None and writer.watermark(table) == 4
    assert (tmp_path / "t" / "ingest_date=2026-01-01").is_dir()

    changes = pa.table(
        {
            "id": pa.array([1, 2, 9], pa.int64()),
            "qty": pa.array([None, 22, 9], pa.int64()),  # int64 -> safe cast to int32
            "ingest_date": pa.array([dt.date(2026, 1, 2)] * 3, pa.date32()),
            "_change_type": ["delete", "upsert", "upsert"],
            "source_commit_version": pa.array([5, 6, 6], pa.int64()),
        }
    )
    res = writer.merge_changes(table, changes, primary_keys=["id"], metadata=meta(6, "cdf"))
    assert res.rows_written == 3 and res.operation == "MERGE"
    out = DeltaTable(uri)
    rows = sorted(out.to_pyarrow_table().to_pylist(), key=lambda r: r["id"])
    assert [r["id"] for r in rows] == [2, 3, 9]
    assert rows[0]["qty"] == 22 and rows[0]["source_commit_version"] == 6
    assert rows[1]["source_commit_version"] is None
    assert "_change_type" not in out.schema().to_arrow().names
    assert read_watermark(out.history()) == 6
    latest = out.history(1)[0]
    assert latest["mirror.mode"] == "cdf" and latest["mirror.run_id"] == "run-1"
