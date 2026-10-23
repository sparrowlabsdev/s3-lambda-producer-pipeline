"""End-to-end: real delta-sharing client + real deltalake writer, fake provider, moto AWS."""

from __future__ import annotations

from typing import Any

import boto3
import pyarrow as pa
import pytest
from deltalake import DeltaTable

from tests.integration.conftest import Stack, people
from tsa_delta_mirror.config import Settings
from tsa_delta_mirror.main import RunSummary, run
from tsa_delta_mirror.writer import read_watermark

pytestmark = pytest.mark.integration


def settings_for(
    stack: Stack, lake_root: str, tables: list[dict[str, Any]], **extra: Any
) -> Settings:
    return Settings(
        profile_secret_arn=stack.secret_arn,
        tables=tables,  # type: ignore[arg-type]
        lake_bucket="unused",
        lake_root=lake_root,
        lock_table="mirror-locks",
        register_glue=True,
        glue_database="bronze_shared",
        metrics_namespace="TSA/DeltaMirror",
        **extra,
    )


CDF_TABLE = {
    "share": "gov",
    "schema": "ops",
    "table": "people_cdf",
    "mode": "auto",
    "primary_keys": ["id"],
}
PLAIN_TABLE = {"share": "gov", "schema": "ops", "table": "people_plain", "mode": "auto"}


def rows_of(uri: str, storage: dict[str, str] | None = None) -> list[dict[str, Any]]:
    table = DeltaTable(uri, storage_options=storage or {})
    return sorted(table.to_pyarrow_table().to_pylist(), key=lambda r: r["id"])


def run_ok(settings: Settings, stack: Stack) -> RunSummary:
    summary = run(settings, stack.clients)
    assert summary.exit_code == 0, [o.error for o in summary.failed]
    return summary


def test_full_lifecycle_on_cdf_table(stack: Stack, lake_root: str) -> None:
    settings = settings_for(stack, lake_root, [CDF_TABLE])
    uri = settings.target_uri(settings.tables[0])
    source = stack.share.tables[("gov", "ops", "people_cdf")]

    # 1. first run: snapshot
    first = run_ok(settings, stack).outcomes[0]
    assert first.mode == "snapshot" and first.rows_written == 3 and first.source_version == 0
    table = DeltaTable(uri)
    assert table.version() == 0
    history = table.history()
    assert history[0]["mirror.source_version"] == "0"
    assert (
        history[0]["mirror.source"] == "gov.ops.people_cdf"
        and history[0]["mirror.mode"] == "snapshot"
    )
    assert [r["id"] for r in rows_of(uri)] == [1, 2, 3]
    assert rows_of(uri)[1]["qty"] is None  # nullable int32 survived the pandas round trip
    schema = pa.schema(table.schema().to_arrow())
    assert schema.field("qty").type == pa.int32()
    assert schema.field("seen").type == pa.timestamp("us", tz="UTC")

    # 2. second run with no provider change: no-op, no new commit
    second = run_ok(settings, stack).outcomes[0]
    assert second.mode == "noop" and second.rows_written == 0
    assert DeltaTable(uri).version() == 0

    # 3. provider changes across several versions -> one MERGE commit
    source.upsert(people([(2, "bob2", 22), (4, "dee", 4)]))  # v1 update + insert
    source.delete(people([(1, "ann", 1)]).select(["id"]))  # v2 delete
    source.upsert(people([(4, "dee2", 44)]))  # v3 update of a row inserted in v1
    source.metadata_only_version()  # v4 no row changes
    third = run_ok(settings, stack).outcomes[0]
    assert third.mode == "cdf" and third.source_version == 4
    assert third.rows_written == 3  # 1 delete + 2 upserts after collapsing
    table = DeltaTable(uri)
    assert table.version() == 1  # exactly one new commit
    assert read_watermark(table.history()) == 4
    got = rows_of(uri)
    assert [(r["id"], r["name"], r["qty"]) for r in got] == [
        (2, "bob2", 22),
        (3, "cy", 3),
        (4, "dee2", 44),
    ]
    by_id = {r["id"]: r for r in got}
    assert by_id[2]["source_commit_version"] == 1 and by_id[4]["source_commit_version"] == 3
    assert by_id[3]["source_commit_version"] is None  # untouched snapshot row
    assert by_id[4]["source_commit_timestamp"] is not None
    assert "_change_type" not in table.schema().to_arrow().names
    assert any("/changes" in r for r in stack.share.requests)

    # 4. re-running a completed version is a no-op
    fourth = run_ok(settings, stack).outcomes[0]
    assert fourth.mode == "noop" and DeltaTable(uri).version() == 1

    # 5. a version with no row changes: nothing to commit, watermark stays put
    source.metadata_only_version()  # v5
    fifth = run_ok(settings, stack).outcomes[0]
    assert fifth.mode == "cdf" and fifth.rows_written == 0
    assert DeltaTable(uri).version() == 1
    assert read_watermark(DeltaTable(uri).history()) == 4

    # Glue registration happened and points at the Delta root.
    glue = stack.clients.glue
    assert glue is not None
    registered = glue.get_table(DatabaseName="bronze_shared", Name="people_cdf")["Table"]
    assert registered["StorageDescriptor"]["Location"].rstrip("/") == uri
    assert registered["Parameters"]["table_type"] == "DELTA"

    # Metrics were emitted with the Table dimension.
    cw = stack.clients.cloudwatch
    assert cw is not None
    names = {m["MetricName"] for m in cw.list_metrics(Namespace="TSA/DeltaMirror")["Metrics"]}
    assert {
        "Success",
        "RowsWritten",
        "DurationSeconds",
        "SourceVersion",
        "Mode",
        "TokenExpiringSoon",
    } <= names


def test_snapshot_path_for_table_without_cdf(stack: Stack, lake_root: str) -> None:
    settings = settings_for(stack, lake_root, [PLAIN_TABLE])
    uri = settings.target_uri(settings.tables[0])
    source = stack.share.tables[("gov", "ops", "people_plain")]
    first = run_ok(settings, stack).outcomes[0]
    assert first.mode == "snapshot" and first.rows_written == 2

    source.overwrite(people([(10, "x", 1), (12, "z", 3)]))  # provider replaced the table
    second = run_ok(settings, stack).outcomes[0]
    assert second.mode == "snapshot" and second.source_version == 1
    table = DeltaTable(uri)
    assert table.version() == 1 and read_watermark(table.history()) == 1
    assert [r["id"] for r in rows_of(uri)] == [10, 12]
    assert not any("/changes" in r for r in stack.share.requests)


def test_cdf_mode_fails_loudly_without_cdf(stack: Stack, lake_root: str) -> None:
    cfg = {**PLAIN_TABLE, "mode": "cdf", "primary_keys": ["id"]}
    summary = run(settings_for(stack, lake_root, [cfg]), stack.clients)
    assert summary.exit_code == 1
    assert "enableChangeDataFeed" in (summary.failed[0].error or "")


def test_auto_mode_falls_back_when_cdf_query_fails(stack: Stack, lake_root: str) -> None:
    settings = settings_for(stack, lake_root, [CDF_TABLE])
    source = stack.share.tables[("gov", "ops", "people_cdf")]
    run_ok(settings, stack)
    source.upsert(people([(9, "nine", 9)]))
    source.cdf_fail = True
    outcome = run_ok(settings, stack).outcomes[0]
    assert outcome.mode == "snapshot" and outcome.rows_written == 4
    assert [r["id"] for r in rows_of(settings.target_uri(settings.tables[0]))] == [1, 2, 3, 9]


def test_one_failing_table_does_not_stop_the_others(stack: Stack, lake_root: str) -> None:
    missing = {"share": "gov", "schema": "ops", "table": "nope"}
    summary = run(settings_for(stack, lake_root, [missing, PLAIN_TABLE]), stack.clients)
    assert summary.exit_code == 1
    assert [o.success for o in summary.outcomes] == [False, True]
    assert "nope" in (summary.outcomes[0].error or "")


def test_lock_contention_skips_table(stack: Stack, lake_root: str) -> None:
    ddb = stack.clients.dynamodb
    assert ddb is not None
    ddb.put_item(
        TableName="mirror-locks",
        Item={
            "table_key": {"S": "gov/ops/people_plain"},
            "owner": {"S": "x"},
            "expires_at": {"N": "9999999999"},
        },
    )
    try:
        summary = run(settings_for(stack, lake_root, [PLAIN_TABLE]), stack.clients)
    finally:
        ddb.delete_item(TableName="mirror-locks", Key={"table_key": {"S": "gov/ops/people_plain"}})
    assert summary.exit_code == 0 and summary.outcomes[0].skipped


def test_token_in_error_text_is_redacted(
    stack: Stack, lake_root: str, capsys: pytest.CaptureFixture[str]
) -> None:
    missing = {"share": "gov", "schema": "ops", "table": "nope"}
    run(settings_for(stack, lake_root, [missing]), stack.clients)
    assert "test-token-0123456789" not in capsys.readouterr().out


def test_against_s3_with_kms_style_options(stack: Stack) -> None:
    """Same flow with the lake on (moto) S3: the production code path for delta-rs."""
    s3 = boto3.client("s3", region_name="us-east-1", endpoint_url=stack.moto)
    s3.create_bucket(Bucket="tsa-data-lake-test")
    settings = Settings(
        profile_secret_arn=stack.secret_arn,
        tables=[CDF_TABLE],  # type: ignore[arg-type]
        lake_bucket="tsa-data-lake-test",
        lake_prefix="bronze/shared",
        s3_endpoint_url=stack.moto,
        s3_allow_http=True,
        metrics_enabled=False,
    )
    uri = settings.target_uri(settings.tables[0])
    assert uri == "s3://tsa-data-lake-test/bronze/shared/gov/ops/people_cdf"
    storage = settings.storage_options()
    run_ok(settings, stack)
    stack.share.tables[("gov", "ops", "people_cdf")].delete(people([(3, "cy", 3)]).select(["id"]))
    outcome = run_ok(settings, stack).outcomes[0]
    assert outcome.mode == "cdf" and outcome.rows_written == 1
    assert [r["id"] for r in rows_of(uri, storage)] == [1, 2]
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="tsa-data-lake-test")["Contents"]]
    assert "bronze/shared/gov/ops/people_cdf/_delta_log/00000000000000000001.json" in keys


def test_contract_plain_open_delta(stack: Stack, lake_root: str, tmp_path: Any) -> None:
    """The mirror is plain Delta: deltalake and DuckDB's delta extension both read it."""
    settings = settings_for(stack, lake_root, [CDF_TABLE])
    run_ok(settings, stack)
    uri = settings.target_uri(settings.tables[0])
    local = uri.removeprefix("file://")
    assert DeltaTable(local).protocol().min_reader_version <= 3
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    try:
        con.execute("INSTALL delta; LOAD delta;")
    except Exception as exc:  # extension download blocked (offline CI)
        pytest.skip(f"duckdb delta extension unavailable: {str(exc)[:80]}")
    count = con.execute("SELECT count(*) FROM delta_scan(?)", [local]).fetchone()
    assert count is not None and count[0] == 3
