# ADR 0001: Python client + delta-rs, no Spark, no Databricks SDK

**Status:** accepted · 2026-10

## Context
The recipient is in a "Databricks ready" phase: Databricks is not procured yet. Data is already reachable over
the open Delta Sharing protocol. The platform team runs Fargate and Terraform under an existing ATO.

## Decision
- Read with the `delta-sharing` Python client (REST + pre-signed parquet URLs). No Spark connector.
- Write with `deltalake` (delta-rs). No Spark, EMR, Glue ETL or EKS anywhere in the dependency tree.
- One container, scheduled on ECS Fargate. The same image is Lambda-compatible (`lambda_handler`),
  but Lambda is not deployed in v1.

## Consequences
- Every table produced is plain, open Delta on S3 (`_delta_log/` + parquet). Unity Catalog can
  register it as an external table on the same path with no data movement.
- The client materialises a table (or a CDF range) in memory as pandas, then we convert to Arrow.
  Task memory must be sized for the largest snapshot (see README "Raising task size").
- The client's `convert_in_batches=True` lowers peak memory while reading but still returns one
  frame; it does not stream. Streaming writes would require a different client.
