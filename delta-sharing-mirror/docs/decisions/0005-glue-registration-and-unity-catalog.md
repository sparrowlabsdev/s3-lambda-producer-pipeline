# ADR 0005: Optional Glue registration as a Delta pointer table; Unity Catalog later

**Status:** accepted · 2026-10

## Decision
When `MIRROR_REGISTER_GLUE=true`, each run upserts a Glue table with `TableType=EXTERNAL_TABLE`,
`Parameters.table_type=DELTA` and `Location` = the Delta table root. Athena engine v3 reads the
schema from `_delta_log`; columns are also written (Hive types) for tools that only read Glue.

Day one on Databricks, the same path is registered as a Unity Catalog **external** table:

```sql
CREATE TABLE bronze_shared.<share>_<schema>.<table>
USING DELTA
LOCATION 's3://<lake-bucket>/bronze/shared/<share>/<schema>/<table>/';
```

No data is moved or rewritten. The mirror can keep running until the Databricks-native ingestion
replaces it; both write plain Delta.

## Consequences
- The Glue table is a pointer, not the source of truth. Deleting it loses nothing.
- Partition columns are declared on the Glue table only when `partition_by` is configured.
