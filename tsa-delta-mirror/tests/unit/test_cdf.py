from __future__ import annotations

import datetime as dt

import pyarrow as pa
import pytest

from tsa_delta_mirror.cdf import LINEAGE_TIMESTAMP, LINEAGE_VERSION, collapse_changes
from tsa_delta_mirror.errors import ConfigError

TS = 1_700_000_000_000


def batch(rows: list[tuple[int, str, str, int]]) -> pa.Table:
    """rows: (id, value, _change_type, _commit_version)"""
    return pa.table(
        {
            "id": pa.array([r[0] for r in rows], pa.int64()),
            "value": pa.array([r[1] for r in rows], pa.string()),
            "_change_type": pa.array([r[2] for r in rows], pa.string()),
            "_commit_version": pa.array([r[3] for r in rows], pa.int64()),
            "_commit_timestamp": pa.array([TS + r[3] for r in rows], pa.int64()),
        }
    )


def final(table: pa.Table) -> dict[int, tuple[str, str | None]]:
    out: dict[int, tuple[str, str | None]] = {}
    for row in table.to_pylist():
        out[row["id"]] = (row["_change_type"], row["value"])
    return out


def test_update_preimage_dropped_and_last_version_wins() -> None:
    rows = [(1, "a", "insert", 1), (1, "a", "update_preimage", 2), (1, "b", "update_postimage", 2)]
    result = collapse_changes(batch(rows), ["id"])
    assert result.inputs == 3
    assert result.upserts == 1 and result.deletes == 0
    assert final(result.rows) == {1: ("upsert", "b")}


def test_insert_then_delete_is_delete() -> None:
    result = collapse_changes(batch([(1, "a", "insert", 1), (1, "a", "delete", 3)]), ["id"])
    assert final(result.rows) == {1: ("delete", "a")}
    assert result.deletes == 1


def test_delete_then_insert_is_upsert() -> None:
    result = collapse_changes(batch([(1, "a", "delete", 1), (1, "z", "insert", 2)]), ["id"])
    assert final(result.rows) == {1: ("upsert", "z")}


def test_delete_and_insert_in_same_version_resolves_to_present() -> None:
    # An overwrite/replaceWhere emits delete + insert for the same key in one commit.
    rows = [(1, "new", "insert", 5), (1, "old", "delete", 5)]
    result = collapse_changes(batch(rows), ["id"])
    assert final(result.rows) == {1: ("upsert", "new")}


def test_many_keys_and_composite_primary_key() -> None:
    table = pa.table(
        {
            "k1": pa.array([1, 1, 2, 2], pa.int64()),
            "k2": pa.array(["x", "y", "x", "x"], pa.string()),
            "v": pa.array([1, 2, 3, 4], pa.int64()),
            "_change_type": ["insert", "insert", "insert", "update_postimage"],
            "_commit_version": pa.array([1, 1, 1, 2], pa.int64()),
            "_commit_timestamp": pa.array([TS] * 4, pa.int64()),
        }
    )
    result = collapse_changes(table, ["k1", "k2"])
    got = {(r["k1"], r["k2"]): r["v"] for r in result.rows.to_pylist()}
    assert got == {(1, "x"): 1, (1, "y"): 2, (2, "x"): 4}
    assert result.upserts == 3


def test_lineage_columns_renamed_and_typed() -> None:
    result = collapse_changes(batch([(1, "a", "insert", 7)]), ["id"])
    schema = result.rows.schema
    assert LINEAGE_VERSION in schema.names and LINEAGE_TIMESTAMP in schema.names
    assert "_commit_version" not in schema.names
    assert schema.field(LINEAGE_TIMESTAMP).type == pa.timestamp("us", tz="UTC")
    row = result.rows.to_pylist()[0]
    assert row[LINEAGE_VERSION] == 7
    assert row[LINEAGE_TIMESTAMP] == dt.datetime.fromtimestamp((TS + 7) / 1000, tz=dt.UTC)


def test_lineage_can_be_dropped() -> None:
    result = collapse_changes(batch([(1, "a", "insert", 7)]), ["id"], keep_lineage=False)
    assert set(result.rows.column_names) == {"id", "value", "_change_type"}


def test_empty_batch() -> None:
    result = collapse_changes(batch([]), ["id"])
    assert result.is_empty and result.inputs == 0


def test_requires_primary_keys() -> None:
    with pytest.raises(ConfigError, match="primary_keys"):
        collapse_changes(batch([(1, "a", "insert", 1)]), [])


def test_missing_key_column_is_an_error() -> None:
    with pytest.raises(ConfigError, match="missing required columns"):
        collapse_changes(batch([(1, "a", "insert", 1)]), ["nope"])


def test_unknown_change_type_is_an_error() -> None:
    with pytest.raises(ConfigError, match="unknown _change_type"):
        collapse_changes(batch([(1, "a", "truncate", 1)]), ["id"])
