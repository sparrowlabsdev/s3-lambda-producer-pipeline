# ADR 0002: S3 commit atomicity via conditional writes; DynamoDB only for the run lock

**Status:** accepted · 2026-10

## Context
Older delta-rs versions needed a DynamoDB lock table (`AWS_S3_LOCKING_PROVIDER=dynamodb`) because
S3 had no conditional PUT. The pinned `deltalake==1.6.6` drops that log store entirely (its binary
reports "delta-rs no longer supports S3DynamoDBLogStores") and commits `_delta_log/N.json` with
`If-None-Match: *`, which S3 (and GovCloud S3) supports natively.

## Decision
- Rely on delta-rs's default S3 conditional-put commits. No `AWS_S3_LOCKING_PROVIDER`, no
  `AWS_S3_ALLOW_UNSAFE_RENAME`.
- Keep a small DynamoDB table as a **run lock** (conditional `PutItem` on `table_key` with a TTL)
  so two tasks never work on the same target table at once (scheduler retry, manual run during a
  scheduled one). This is cheap and removes the overlap risk entirely, as the handoff asked.
- Additionally one EventBridge schedule per table, with no flexible window and no retries.

## Consequences
- A crash mid-run leaves the previous commit intact; the next run re-reads the watermark from the
  table's history and continues. No state outside the Delta table itself.
- If a stale lock is ever left behind (task killed before `DeleteItem`), it expires after
  `MIRROR_RUN_LOCK_TTL_SECONDS` (default 3 h). Until then the table is skipped with `LockContended=1`.
