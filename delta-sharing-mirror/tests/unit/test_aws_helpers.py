from __future__ import annotations

import time
from typing import Any

import boto3
import pyarrow as pa
import pytest
from moto import mock_aws

from delta_sharing_mirror.catalog import arrow_to_hive, glue_table_input, register_table
from delta_sharing_mirror.errors import LockHeldError
from delta_sharing_mirror.locks import RunLock
from delta_sharing_mirror.metrics import Metrics

REGION = "us-east-1"


def make_lock_table(ddb: Any, name: str = "locks") -> None:
    ddb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "table_key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "table_key", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )


@mock_aws
def test_run_lock_contention_and_release() -> None:
    ddb = boto3.client("dynamodb", region_name=REGION)
    make_lock_table(ddb)
    first = RunLock(ddb, "locks", "s/sch/t", "run-a", ttl_seconds=600)
    second = RunLock(ddb, "locks", "s/sch/t", "run-b", ttl_seconds=600)
    first.acquire()
    with pytest.raises(LockHeldError):
        second.acquire()
    first.release()
    second.acquire()  # free again
    assert (
        ddb.get_item(TableName="locks", Key={"table_key": {"S": "s/sch/t"}})["Item"]["owner"]["S"]
        == "run-b"
    )
    second.release()
    assert "Item" not in ddb.get_item(TableName="locks", Key={"table_key": {"S": "s/sch/t"}})


@mock_aws
def test_expired_lock_can_be_taken_and_release_is_owner_scoped() -> None:
    ddb = boto3.client("dynamodb", region_name=REGION)
    make_lock_table(ddb)
    stale = int(time.time()) - 10
    ddb.put_item(
        TableName="locks",
        Item={"table_key": {"S": "k"}, "owner": {"S": "dead"}, "expires_at": {"N": str(stale)}},
    )
    lock = RunLock(ddb, "locks", "k", "run-c", ttl_seconds=600)
    lock.acquire()
    # Someone else stole it after our TTL lapsed: release must not delete their item.
    ddb.put_item(
        TableName="locks",
        Item={"table_key": {"S": "k"}, "owner": {"S": "other"}, "expires_at": {"N": "9999999999"}},
    )
    lock.release()
    assert (
        ddb.get_item(TableName="locks", Key={"table_key": {"S": "k"}})["Item"]["owner"]["S"]
        == "other"
    )


def test_run_lock_disabled_is_noop() -> None:
    with RunLock(None, None, "k", "run", 10) as lock:
        assert not lock.enabled


@mock_aws
def test_metrics_flush() -> None:
    cw = boto3.client("cloudwatch", region_name=REGION)
    metrics = Metrics(cw, "DeltaMirror")
    metrics.put("Success", 1, table="s.sch.t")
    metrics.put("DurationSeconds", 2.5, unit="Seconds", table="s.sch.t")
    metrics.flush()
    names = {m["MetricName"] for m in cw.list_metrics(Namespace="DeltaMirror")["Metrics"]}
    assert names == {"Success", "DurationSeconds"}


def test_metrics_without_client_and_with_failure() -> None:
    Metrics(None, "ns").put("Success", 1)

    class Boom:
        def put_metric_data(self, **_: Any) -> None:
            raise RuntimeError("cloudwatch down")

    metrics = Metrics(Boom(), "ns")  # type: ignore[arg-type]
    metrics.put("Success", 1)
    metrics.flush()  # swallowed, logged


def test_arrow_to_hive_mapping() -> None:
    assert arrow_to_hive(pa.int32()) == "int"
    assert arrow_to_hive(pa.int64()) == "bigint"
    assert arrow_to_hive(pa.decimal128(10, 2)) == "decimal(10,2)"
    assert arrow_to_hive(pa.timestamp("us", tz="UTC")) == "timestamp"
    assert arrow_to_hive(pa.list_(pa.string())) == "array<string>"
    assert (
        arrow_to_hive(pa.struct([("a", pa.int8()), ("b", pa.bool_())]))
        == "struct<a:tinyint,b:boolean>"
    )
    assert arrow_to_hive(pa.map_(pa.string(), pa.float64())) == "map<string,double>"


@mock_aws
def test_glue_register_create_then_update() -> None:
    glue = boto3.client("glue", region_name=REGION)
    glue.create_database(DatabaseInput={"Name": "bronze_shared"})
    schema = pa.schema([("id", pa.int64()), ("ingest_date", pa.date32())])
    register_table(
        glue, "bronze_shared", "t", "s3://lake/bronze/shared/s/sch/t", schema, ["ingest_date"]
    )
    schema2 = pa.schema([("id", pa.int64()), ("v", pa.string()), ("ingest_date", pa.date32())])
    register_table(
        glue, "bronze_shared", "t", "s3://lake/bronze/shared/s/sch/t", schema2, ["ingest_date"]
    )
    table = glue.get_table(DatabaseName="bronze_shared", Name="t")["Table"]
    assert table["Parameters"]["table_type"] == "DELTA"
    assert table["StorageDescriptor"]["Location"] == "s3://lake/bronze/shared/s/sch/t/"
    assert [c["Name"] for c in table["StorageDescriptor"]["Columns"]] == ["id", "v"]
    assert [c["Name"] for c in table["PartitionKeys"]] == ["ingest_date"]
    assert glue_table_input("x", "s3://b/p", schema, [])["TableType"] == "EXTERNAL_TABLE"
