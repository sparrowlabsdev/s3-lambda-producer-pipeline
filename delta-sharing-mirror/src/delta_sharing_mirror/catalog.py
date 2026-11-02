"""Optional Glue Data Catalog registration so Athena (engine v3) can query the mirror day one.

The Glue table is a pointer to the Delta table root with ``table_type=DELTA``; Athena reads the
schema from ``_delta_log``. Columns are also written for tools that read Glue directly. This is
the same shape Unity Catalog will later see as an *external* table on the same S3 path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa
from botocore.exceptions import ClientError

from delta_sharing_mirror.log import get_logger

if TYPE_CHECKING:
    from mypy_boto3_glue import GlueClient

log = get_logger(__name__)


def arrow_to_hive(dtype: pa.DataType) -> str:
    if pa.types.is_boolean(dtype):
        return "boolean"
    if pa.types.is_int8(dtype):
        return "tinyint"
    if pa.types.is_int16(dtype):
        return "smallint"
    if pa.types.is_int32(dtype):
        return "int"
    if pa.types.is_integer(dtype):
        return "bigint"
    if pa.types.is_float32(dtype):
        return "float"
    if pa.types.is_floating(dtype):
        return "double"
    if pa.types.is_decimal(dtype):
        return f"decimal({dtype.precision},{dtype.scale})"
    if pa.types.is_date(dtype):
        return "date"
    if pa.types.is_timestamp(dtype):
        return "timestamp"
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return "binary"
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return f"array<{arrow_to_hive(dtype.value_type)}>"
    if pa.types.is_struct(dtype):
        inner = ",".join(f"{f.name}:{arrow_to_hive(f.type)}" for f in dtype)
        return f"struct<{inner}>"
    if pa.types.is_map(dtype):
        return f"map<{arrow_to_hive(dtype.key_type)},{arrow_to_hive(dtype.item_type)}>"
    return "string"


def glue_table_input(
    name: str, location: str, schema: pa.Schema, partition_by: list[str]
) -> dict[str, Any]:
    columns = [
        {"Name": f.name, "Type": arrow_to_hive(f.type)}
        for f in schema
        if f.name not in partition_by
    ]
    partitions = [
        {"Name": f.name, "Type": arrow_to_hive(f.type)} for f in schema if f.name in partition_by
    ]
    return {
        "Name": name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "EXTERNAL": "TRUE",
            "table_type": "DELTA",
            "classification": "delta",
            "spark.sql.sources.provider": "delta",
            "managed_by": "delta-sharing-mirror",
        },
        "PartitionKeys": partitions,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": location.rstrip("/") + "/",
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": (
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                ),
                "Parameters": {"serialization.format": "1"},
            },
        },
    }


def register_table(
    client: GlueClient,
    database: str,
    name: str,
    location: str,
    schema: pa.Schema,
    partition_by: list[str],
) -> None:
    table_input = glue_table_input(name, location, schema, partition_by)
    try:
        client.update_table(DatabaseName=database, TableInput=table_input)  # type: ignore[arg-type]
        log.info("glue_table_updated", database=database, table=name)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "EntityNotFoundException":
            raise
        client.create_table(DatabaseName=database, TableInput=table_input)  # type: ignore[arg-type]
        log.info("glue_table_created", database=database, table=name)
