"""``deltalake`` (delta-rs 1.6.x) wrappers: overwrite, CDF merge, watermark in commit metadata.

Verified against the installed signatures:
- ``write_deltalake(table_or_uri, data, *, partition_by, mode, schema_mode, storage_options,
  commit_properties=CommitProperties(custom_metadata=...))``
- ``DeltaTable.merge(source, predicate, source_alias, target_alias, merge_schema,
  commit_properties)``
  -> ``when_matched_delete / when_matched_update_all / when_not_matched_insert_all / execute``
- ``DeltaTable.history()`` returns commit dicts with custom metadata keys at the top level.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, write_deltalake

from tsa_delta_mirror.config import SchemaMode
from tsa_delta_mirror.errors import SchemaMismatchError
from tsa_delta_mirror.log import get_logger
from tsa_delta_mirror.reader import CDF_CHANGE_TYPE

log = get_logger(__name__)

WATERMARK_KEY = "mirror.source_version"
NO_WATERMARK = -1


@dataclass(frozen=True)
class CommitMetadata:
    source_version: int
    source: str
    mode: str
    run_id: str
    run_at: str

    def as_dict(self) -> dict[str, str]:
        return {
            WATERMARK_KEY: str(self.source_version),
            "mirror.source": self.source,
            "mirror.mode": self.mode,
            "mirror.run_id": self.run_id,
            "mirror.run_at": self.run_at,
        }


@dataclass(frozen=True)
class WriteResult:
    rows_written: int
    target_version: int
    operation: str


def read_watermark(history: list[dict[str, Any]]) -> int:
    """Newest commit carrying ``mirror.source_version`` wins. -1 when no commit has one.

    delta-rs exposes custom commit metadata as top-level keys of each history entry; Spark
    writers put it under ``userMetadata``. We accept both so a table touched by another engine
    still yields a watermark where one exists.
    """
    for entry in sorted(history, key=lambda e: int(e.get("version", -1)), reverse=True):
        value = entry.get(WATERMARK_KEY)
        if value is None:
            user_meta = entry.get("userMetadata")
            if isinstance(user_meta, dict):
                value = user_meta.get(WATERMARK_KEY)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                log.warning("watermark_unparseable", version=entry.get("version"), value=str(value))
    return NO_WATERMARK


def merge_predicate(
    primary_keys: list[str], source_alias: str = "s", target_alias: str = "t"
) -> str:
    return " AND ".join(f'{target_alias}."{k}" = {source_alias}."{k}"' for k in primary_keys)


def align_to_target(source: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """Cast columns the target already has to the target's types (safe cast only).

    New columns pass through untouched (schema evolution adds them). A cast that would lose
    data or change a column's type family raises ``SchemaMismatchError`` with a clear message.
    """
    fields: list[pa.Field] = []
    for name in source.column_names:
        if name in target_schema.names:
            fields.append(target_schema.field(name).with_nullable(True))
        else:
            fields.append(source.schema.field(name))
    try:
        return source.cast(pa.schema(fields), safe=True)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
        mismatches = [
            f"{name}: target={target_schema.field(name).type} "
            f"incoming={source.schema.field(name).type}"
            for name in source.column_names
            if name in target_schema.names
            and not source.schema.field(name).type.equals(target_schema.field(name).type)
        ]
        raise SchemaMismatchError(
            "incoming data needs a column type change the mirror will not apply automatically "
            f"({'; '.join(mismatches) or str(exc)[:200]}). Re-run in snapshot mode with "
            "snapshot_schema_mode=overwrite to rebuild the table."
        ) from None


class DeltaWriter:
    def __init__(self, storage_options: dict[str, str]) -> None:
        self._storage_options = storage_options

    def open(self, uri: str) -> DeltaTable | None:
        if not DeltaTable.is_deltatable(uri, storage_options=self._storage_options):
            return None
        return DeltaTable(uri, storage_options=self._storage_options)

    def watermark(self, table: DeltaTable | None) -> int:
        if table is None:
            return NO_WATERMARK
        return read_watermark(table.history())

    def target_schema(self, table: DeltaTable) -> pa.Schema:
        return pa.schema(table.schema().to_arrow())

    def write_snapshot(
        self,
        uri: str,
        data: pa.Table,
        *,
        partition_by: list[str],
        schema_mode: SchemaMode,
        metadata: CommitMetadata,
    ) -> WriteResult:
        write_deltalake(
            uri,
            data,
            mode="overwrite",
            schema_mode=schema_mode,
            partition_by=partition_by or None,
            storage_options=self._storage_options,
            commit_properties=CommitProperties(custom_metadata=metadata.as_dict()),
        )
        table = DeltaTable(uri, storage_options=self._storage_options)
        return WriteResult(data.num_rows, table.version(), "WRITE")

    def merge_changes(
        self,
        table: DeltaTable,
        changes: pa.Table,
        *,
        primary_keys: list[str],
        metadata: CommitMetadata,
    ) -> WriteResult:
        """Apply collapsed CDF rows in one atomic MERGE commit.

        ``changes`` carries ``_change_type`` in {'upsert', 'delete'} (see ``cdf.collapse_changes``).
        """
        aligned = align_to_target(changes, self.target_schema(table))
        merger = table.merge(
            source=aligned,
            predicate=merge_predicate(primary_keys),
            source_alias="s",
            target_alias="t",
            merge_schema=True,
            commit_properties=CommitProperties(custom_metadata=metadata.as_dict()),
        )
        is_delete = f"s.\"{CDF_CHANGE_TYPE}\" = 'delete'"
        not_delete = f"s.\"{CDF_CHANGE_TYPE}\" <> 'delete'"
        result = (
            merger.when_matched_delete(predicate=is_delete)
            .when_matched_update_all(predicate=not_delete, except_cols=[CDF_CHANGE_TYPE])
            .when_not_matched_insert_all(predicate=not_delete, except_cols=[CDF_CHANGE_TYPE])
            .execute()
        )
        rows = int(
            result.get("num_target_rows_inserted", 0)
            + result.get("num_target_rows_updated", 0)
            + result.get("num_target_rows_deleted", 0)
        )
        log.info(
            "merge_metrics",
            inserted=result.get("num_target_rows_inserted"),
            updated=result.get("num_target_rows_updated"),
            deleted=result.get("num_target_rows_deleted"),
            source_rows=result.get("num_source_rows"),
        )
        return WriteResult(rows, table.version(), "MERGE")
