"""Change Data Feed -> merge semantics.

Input rows carry ``_change_type`` in {insert, update_preimage, update_postimage, delete},
``_commit_version`` and ``_commit_timestamp``. We collapse the batch to the final state per
primary key, which is equivalent to replaying versions in order because, for a given key, only
the last change determines the end state. Within one commit version a delete and an insert of
the same key (e.g. an overwrite) resolve to "present". See docs/decisions/0004-cdf-collapse.md.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

from delta_sharing_mirror.errors import ConfigError
from delta_sharing_mirror.reader import CDF_CHANGE_TYPE, CDF_COMMIT_TIMESTAMP, CDF_COMMIT_VERSION

LINEAGE_VERSION = "source_commit_version"
LINEAGE_TIMESTAMP = "source_commit_timestamp"

# Higher rank wins when the same key changes more than once in one commit version.
_RANK = {"delete": 0, "insert": 1, "update_postimage": 2}


@dataclass(frozen=True)
class CollapsedChanges:
    """Final state per key. ``rows`` keeps ``_change_type`` as 'delete' or 'upsert'."""

    rows: pa.Table
    inputs: int
    upserts: int
    deletes: int

    @property
    def is_empty(self) -> bool:
        return bool(self.rows.num_rows == 0)


def collapse_changes(
    changes: pa.Table, primary_keys: list[str], keep_lineage: bool = True
) -> CollapsedChanges:
    """Reduce a CDF batch to one row per key, tagged 'upsert' or 'delete'."""
    if not primary_keys:
        raise ConfigError("CDF merge requires primary_keys")
    required = (CDF_CHANGE_TYPE, CDF_COMMIT_VERSION, *primary_keys)
    missing = [c for c in required if c not in changes.column_names]
    if missing:
        raise ConfigError(f"CDF batch is missing required columns: {missing}")

    total = changes.num_rows
    if total == 0:
        return CollapsedChanges(_finish(changes, keep_lineage), 0, 0, 0)

    kept = changes.filter(pc.not_equal(changes[CDF_CHANGE_TYPE], "update_preimage"))
    rank = pc.index_in(kept[CDF_CHANGE_TYPE], value_set=pa.array(list(_RANK)))
    rank = pc.fill_null(rank, -1)
    kept = kept.append_column("__rank", rank).append_column(
        "__row", pa.array(range(kept.num_rows), pa.int64())
    )
    unknown = kept.filter(pc.equal(kept["__rank"], -1))
    if unknown.num_rows:
        bad = pc.unique(unknown[CDF_CHANGE_TYPE]).to_pylist()
        raise ConfigError(f"unknown _change_type values in CDF batch: {bad}")

    # Last change per key wins: sort ascending, keep the max row position per key.
    ordered = kept.sort_by([(CDF_COMMIT_VERSION, "ascending"), ("__rank", "ascending")])
    ordered = ordered.set_column(
        ordered.schema.get_field_index("__row"),
        "__row",
        pa.array(range(ordered.num_rows), pa.int64()),
    )
    last = ordered.group_by(primary_keys).aggregate([("__row", "max")])
    winners = ordered.take(last["__row_max"])
    winners = winners.sort_by([(CDF_COMMIT_VERSION, "ascending")])

    change_type = pc.if_else(
        pc.equal(winners[CDF_CHANGE_TYPE], "delete"),
        pa.scalar("delete"),
        pa.scalar("upsert"),
    )
    winners = winners.set_column(
        winners.schema.get_field_index(CDF_CHANGE_TYPE), CDF_CHANGE_TYPE, change_type
    )
    winners = winners.drop_columns(["__rank", "__row"])
    deletes = int(pc.sum(pc.equal(winners[CDF_CHANGE_TYPE], "delete")).as_py() or 0)
    rows = _finish(winners, keep_lineage)
    return CollapsedChanges(rows, total, winners.num_rows - deletes, deletes)


def _finish(table: pa.Table, keep_lineage: bool) -> pa.Table:
    """Rename/convert lineage columns (or drop them) and keep ``_change_type`` for the merge."""
    if keep_lineage:
        if CDF_COMMIT_VERSION in table.column_names:
            table = table.rename_columns(
                [LINEAGE_VERSION if c == CDF_COMMIT_VERSION else c for c in table.column_names]
            )
        if CDF_COMMIT_TIMESTAMP in table.column_names:
            idx = table.schema.get_field_index(CDF_COMMIT_TIMESTAMP)
            ts = pc.cast(table[CDF_COMMIT_TIMESTAMP], pa.timestamp("ms", tz="UTC"))
            ts = pc.cast(ts, pa.timestamp("us", tz="UTC"))
            table = table.set_column(idx, LINEAGE_TIMESTAMP, ts)
    else:
        table = table.drop_columns(
            [c for c in (CDF_COMMIT_VERSION, CDF_COMMIT_TIMESTAMP) if c in table.column_names]
        )
    return table
