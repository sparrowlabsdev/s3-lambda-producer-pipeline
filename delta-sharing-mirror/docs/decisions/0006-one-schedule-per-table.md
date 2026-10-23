# ADR 0006: One EventBridge schedule per table, each with a single-table container override

**Status:** accepted · 2026-10

## Decision
Terraform creates one `aws_scheduler_schedule` per configured table. Each schedule starts the same
task definition with `MIRROR_TABLES` overridden to that one table. The task definition's default
`MIRROR_TABLES` holds the full list, so an ad-hoc `ecs run-task` with no override mirrors everything.

## Consequences
- Tables run independently and in parallel; a slow initial load does not delay the others.
- Different cadences per table (`schedule` in the `tables` variable).
- Per-table CloudWatch alarms (`run-failed`, `missed-run`) key off the `Table` metric dimension.
- Fargate task cost is per table per run; for many tiny feeds, group them later or move them to
  the Lambda deployment the image already supports.
