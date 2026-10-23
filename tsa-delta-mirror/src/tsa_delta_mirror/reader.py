"""Thin wrappers over the ``delta-sharing`` client (v1.4.2 API, verified against the source).

The client returns pandas frames. We convert them to Arrow tables typed from the share's own
schema string so integer/decimal/timestamp columns survive the pandas round trip.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd
import pyarrow as pa
from delta_sharing.protocol import CdfOptions, Metadata, Table
from delta_sharing.reader import DeltaSharingReader
from delta_sharing.rest_client import DataSharingRestClient

from tsa_delta_mirror.config import TableConfig
from tsa_delta_mirror.log import capture_client_stdout, get_logger
from tsa_delta_mirror.profile import SharingProfile

log = get_logger(__name__)

CDF_CHANGE_TYPE = "_change_type"
CDF_COMMIT_VERSION = "_commit_version"
CDF_COMMIT_TIMESTAMP = "_commit_timestamp"
CDF_COLUMNS = (CDF_CHANGE_TYPE, CDF_COMMIT_VERSION, CDF_COMMIT_TIMESTAMP)

_DECIMAL = re.compile(r"decimal\((\d+),\s*(\d+)\)")
_PRIMITIVE: dict[str, pa.DataType] = {
    "string": pa.string(),
    "long": pa.int64(),
    "integer": pa.int32(),
    "short": pa.int16(),
    "byte": pa.int8(),
    "double": pa.float64(),
    "float": pa.float32(),
    "boolean": pa.bool_(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "timestamp_ntz": pa.timestamp("us"),
    "binary": pa.binary(),
}


def spark_type_to_arrow(spark_type: Any) -> pa.DataType | None:
    """Map a Spark JSON schema type to Arrow. Returns None for types we let Arrow infer."""
    if isinstance(spark_type, str):
        if spark_type in _PRIMITIVE:
            return _PRIMITIVE[spark_type]
        match = _DECIMAL.match(spark_type)
        if match:
            return pa.decimal128(int(match.group(1)), int(match.group(2)))
    return None


def arrow_schema_from_share(schema_string: str, extra: list[pa.Field] | None = None) -> pa.Schema:
    """Build an Arrow schema from the share's ``schemaString`` (Spark JSON schema).

    Complex (struct/array/map) columns are left to Arrow inference by omitting them here;
    ``to_arrow_table`` fills those in from the data.
    """
    schema_json = json.loads(schema_string)
    fields: list[pa.Field] = []
    for field in schema_json["fields"]:
        arrow_type = spark_type_to_arrow(field["type"])
        if arrow_type is not None:
            fields.append(pa.field(field["name"], arrow_type, nullable=True))
    return pa.schema(fields + (extra or []))


def to_arrow_table(df: pd.DataFrame, declared: pa.Schema) -> pa.Table:
    """Convert a client frame to Arrow, using the declared types where we know them."""
    inferred = pa.Table.from_pandas(df, preserve_index=False)
    fields: list[pa.Field] = []
    for name in inferred.column_names:
        if name in declared.names:
            fields.append(declared.field(name))
        else:
            fields.append(inferred.schema.field(name))
    target = pa.schema(fields)
    try:
        return pa.Table.from_pandas(df, schema=target, preserve_index=False)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
        log.warning("schema_cast_fallback", reason=str(exc)[:200])
        return inferred.replace_schema_metadata(None)


@dataclass(frozen=True)
class SourceTable:
    """A share/schema/table triple plus the config that governs how it is read."""

    config: TableConfig

    @property
    def ref(self) -> Table:
        return Table(name=self.config.table, share=self.config.share, schema=self.config.schema_)


class SharingReader:
    """One REST client per run; one instance is reused for every table."""

    def __init__(self, profile: SharingProfile, num_retries: int = 10) -> None:
        self._client = DataSharingRestClient(profile.profile, num_retries=num_retries)

    def close(self) -> None:
        self._client.close()

    def table_version(self, source: SourceTable) -> int:
        version: int = self._client.query_table_version(source.ref).delta_table_version
        return version

    def table_metadata(self, source: SourceTable) -> Metadata:
        metadata: Metadata = self._client.query_table_metadata(source.ref).metadata
        return metadata

    @staticmethod
    def cdf_enabled(metadata: Metadata) -> bool:
        conf = metadata.configuration or {}
        for key in ("delta.enableChangeDataFeed", "enableChangeDataFeed"):
            if str(conf.get(key, "")).lower() == "true":
                return True
        return False

    def load_snapshot(self, source: SourceTable, version: int, metadata: Metadata) -> pa.Table:
        reader = DeltaSharingReader(
            table=source.ref,
            rest_client=self._client,
            version=version,
            use_delta_format=source.config.use_delta_format,
            convert_in_batches=True,
        )
        with capture_client_stdout(log):
            df = reader.to_pandas()
        declared = arrow_schema_from_share(metadata.schema_string or '{"fields": []}')
        return to_arrow_table(df, declared)

    def load_changes(
        self, source: SourceTable, starting_version: int, ending_version: int, metadata: Metadata
    ) -> pa.Table:
        reader = DeltaSharingReader(
            table=source.ref,
            rest_client=self._client,
            use_delta_format=None,  # parquet response format for CDF (client default)
            convert_in_batches=True,
        )
        options = CdfOptions(starting_version=starting_version, ending_version=ending_version)
        with capture_client_stdout(log):
            df = reader.table_changes_to_pandas(options)
        extra = [
            pa.field(CDF_CHANGE_TYPE, pa.string()),
            pa.field(CDF_COMMIT_VERSION, pa.int64()),
            pa.field(CDF_COMMIT_TIMESTAMP, pa.int64()),
        ]
        declared = arrow_schema_from_share(metadata.schema_string or '{"fields": []}', extra)
        return to_arrow_table(df, declared)
