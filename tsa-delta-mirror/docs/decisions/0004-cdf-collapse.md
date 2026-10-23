# ADR 0004: CDF is collapsed to final state per key and applied as one MERGE

**Status:** accepted · 2026-10

## Context
A CDF range (`last_version+1 .. remote_version`) may touch the same key several times. Replaying
each commit version as its own merge would produce one Delta commit per source version and leave a
partially-applied range if the task dies between commits.

## Decision
`cdf.collapse_changes` drops `update_preimage`, orders rows by `_commit_version` then by change
rank (`delete < insert < update_postimage`) and keeps the last row per primary key. This is
equivalent to replaying the versions in order because, for a given key, only its last change
determines the end state. In one commit version, delete + insert of the same key (an overwrite or
REPLACE WHERE) resolves to "present".

The collapsed batch is applied with a single `DeltaTable.merge`:
`when_matched_delete(_change_type = 'delete')`, `when_matched_update_all(...)`,
`when_not_matched_insert_all(...)`, `except_cols=['_change_type']`, `merge_schema=True`.
One atomic commit carries the watermark `mirror.source_version = remote_version`.

`_commit_version` / `_commit_timestamp` are kept as `source_commit_version` /
`source_commit_timestamp` (configurable per table with `keep_cdf_lineage`).

## Consequences
- If the provider's `primary_keys` are not actually unique, collapsing silently keeps the last
  row per key. Primary keys must be confirmed with the provider (open question in README).
- A source range with no row changes (OPTIMIZE, property changes) yields an empty batch. delta-rs
  does not commit a MERGE with zero source rows, so the watermark is **not** advanced; the next run
  re-queries the same (empty) range. This is logged as `cdf_no_row_changes` and is harmless.
- Incoming columns already on the target are safe-cast to the target type; a cast that would lose
  data raises `SchemaMismatchError` with instructions (rebuild in snapshot mode).
