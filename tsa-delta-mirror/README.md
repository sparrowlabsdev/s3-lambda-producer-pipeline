# tsa-delta-mirror

Mirror tables from a **Delta Sharing** provider into **plain Delta Lake tables on S3** in TSA's own
GovCloud account. No Spark, no cluster, no Databricks dependency. A scheduled ECS Fargate task runs
a small Python service that reads shares with the open `delta-sharing` client and writes Delta with
`deltalake` (delta-rs). Every table it writes can be registered in Unity Catalog as an external table,
on the same S3 path, the day a Databricks workspace exists.

> Second reference architecture in this repo. The first one (root of the repo) is an S3 → Lambda →
> Glue producer pipeline. This one is a *consumer*: it lands data someone else already shares.

## Open questions for the customer (config, not blockers)

- [ ] Provider's sharing endpoint host and pre-signed storage host(s) → `provider_endpoint_host`, `provider_storage_hosts` (egress allowlist).
- [ ] Is Change Data Feed enabled on the shared tables? Without it deletes are invisible and snapshot overwrite is the only safe mode. Ask the provider to enable it.
- [ ] Recipient token lifetime and who owns rotation (see *Rotating the token*).
- [ ] Target bucket, KMS key, VPC/subnets/route tables; Glue registration on day one? (`register_glue`).
- [ ] Primary keys per table (required for CDF merges) → `tables[].primary_keys`.
- [ ] Initial load sizes, to set `task_memory` (the mirror holds one table snapshot in memory).

## Architecture

```
EventBridge Scheduler (one schedule per table, no overlap window, no retries)
        │  ecs:RunTask with MIRROR_TABLES=[one table]
        ▼
ECS Fargate task  (python:3.12-slim, non-root, read-only rootfs, /tmp volume)
        ├─ Secrets Manager ── Delta Sharing profile {endpoint, bearerToken, expirationTime}
        ├─ DynamoDB ───────── per-table run lock (conditional put + TTL)
        ├─ delta-sharing ──▶ provider sharing server ──▶ provider pre-signed parquet URLs
        ├─ deltalake ──────▶ s3://<lake-bucket>/bronze/shared/<share>/<schema>/<table>/  (SSE-KMS)
        ├─ CloudWatch ────── JSON logs + metrics (Success, RowsWritten, SourceVersion, Mode, …)
        └─ Glue (optional) ─ external Delta table so Athena can query it day one
```

Networking: private subnets only, HTTPS egress only. VPC endpoints for S3, DynamoDB (gateway) and
ECR, Secrets Manager, Logs, STS, CloudWatch, Glue (interface). The only non-AWS destinations are the
provider's sharing endpoint and its pre-signed storage host(s); the platform team allowlists both on
the existing egress path.

## How it works (per table, per run)

1. Load the profile from Secrets Manager. If `expirationTime` is within 7 days: `TokenExpiringSoon=1` and a WARN.
2. `remote_version` = the share's current table version; `metadata` = its schema and table properties.
3. `last_version` = watermark read from the **target table's own commit history**
   (`mirror.source_version` in the newest commit that has one; `-1` if the table does not exist).
4. `remote_version <= last_version` → log `up_to_date`, `RowsWritten=0`, **no commit**.
5. Otherwise:
   - **CDF path** (`mode` is `cdf` or `auto`, the share reports `delta.enableChangeDataFeed=true`,
     the target exists with a watermark, and `primary_keys` are set): load changes
     `last_version+1 … remote_version`, collapse them to the final state per key
     ([ADR 0004](docs/decisions/0004-cdf-collapse.md)), apply **one** `MERGE`
     (delete / update / insert by primary key). Lineage columns `source_commit_version`,
     `source_commit_timestamp` are kept by default.
   - **Snapshot path** (everything else, including the very first load): load the snapshot at
     `remote_version` and `overwrite` the target (`snapshot_schema_mode` = `overwrite` or `merge`).
     `ingest_date` (UTC) is added only when `partition_by` asks for it.
   - `mode: auto` falls back from CDF to snapshot with a WARN if the CDF query fails (e.g. the range
     is no longer retained). `mode: cdf` fails loudly instead.
6. Every commit carries `mirror.source_version`, `mirror.source`, `mirror.mode`, `mirror.run_id`,
   `mirror.run_at` as Delta commit metadata. That is the only state the mirror keeps.
7. Optionally upsert the Glue table. Emit metrics. Exit non-zero if any table failed.

**Idempotent and crash-safe.** Delta commits are atomic; a crash mid-run leaves the previous commit
and watermark intact, and a re-run of an already-mirrored version is a no-op. One schedule per table
plus the DynamoDB run lock guarantee a single writer per target table
([ADR 0002](docs/decisions/0002-s3-commit-atomicity.md)).

## What a Delta Sharing profile is

A small JSON document the *provider* gives the *recipient*. It is a credential:

```json
{"shareCredentialsVersion": 1,
 "endpoint": "https://sharing.provider.example.gov/delta-sharing",
 "bearerToken": "<secret>",
 "expirationTime": "2027-01-01T00:00:00Z"}
```

The mirror reads it from Secrets Manager into memory and never writes it to disk, env vars or logs
([ADR 0003](docs/decisions/0003-profile-in-memory.md)). Only the endpoint host is logged.

## Repository layout

```
tsa-delta-mirror/
  src/tsa_delta_mirror/   config.py profile.py reader.py writer.py cdf.py catalog.py locks.py metrics.py log.py main.py
  tests/unit/             pure logic (CDF collapse, watermark, config, redaction, locks, metrics, Glue)
  tests/integration/      real client + real writer against an in-process fake sharing server and a moto S3 server
                          test_compose_stack.py: the same against the reference server + MinIO (opt-in, -m compose)
  terraform/              ECR, ECS, Scheduler, DynamoDB, Secrets Manager, IAM, VPC endpoints, alarms
  docker/sharing-server/  reference server config for the compose stack
  scripts/                seed_share.py (compose fixtures), smoke.sh (real deployment)
  docs/decisions/         ADRs
  Dockerfile  docker-compose.test.yml  requirements.lock.txt (hash-pinned)  pyproject.toml
```

## Local development

```bash
cd tsa-delta-mirror
uv venv --python 3.12 .venv && source .venv/bin/activate
uv pip install -e ".[dev]"

ruff format --check src tests scripts && ruff check src tests scripts
mypy                                   # strict on src/
pytest                                 # unit + in-process integration (no Docker needed)
pytest -m compose tests/integration    # needs the compose stack below
```

The in-process integration tests start a fake Delta Sharing server (`tests/integration/fake_sharing_server.py`)
and a `moto` server for S3/Secrets Manager/DynamoDB/CloudWatch/Glue, then drive the real client and
the real writer through: first-run snapshot → no-op re-run → CDF insert/update/delete across several
versions → one MERGE with correct counts and watermark → no-op re-run → snapshot overwrite for a
non-CDF table → `mode: cdf` on a non-CDF table fails loudly → lock contention skips → tokens are
redacted → lake on S3 with the production delta-rs path → DuckDB reads the result as plain Delta.

### Reference server + MinIO (docker-compose)

```bash
docker compose -f docker-compose.test.yml up -d --wait
python scripts/seed_share.py                 # writes gov.ops.people_cdf (CDF on) and gov.ops.people_plain to MinIO
pytest -m compose tests/integration/test_compose_stack.py
python scripts/seed_share.py --change        # provider-side update/insert/delete + overwrite, then re-run the mirror
docker compose -f docker-compose.test.yml down -v
```

`docker/sharing-server/delta-sharing-server.yaml` is the reference server config (bearer token
`compose-test-token`, `cdfEnabled: true` on one table). The server reads MinIO through S3A
(`docker/sharing-server/conf/core-site.xml`).

### Run the service locally against any share

```bash
export AWS_REGION=us-gov-west-1
export MIRROR_PROFILE_SECRET_ARN=arn:aws-us-gov:secretsmanager:us-gov-west-1:111122223333:secret:tsa-delta-mirror-dev/delta-sharing-profile
export MIRROR_LAKE_BUCKET=tsa-data-lake-dev
export MIRROR_TABLES='[{"share":"s","schema":"sch","table":"t","mode":"auto","primary_keys":["id"]}]'
python -m tsa_delta_mirror            # mirror once
python -m tsa_delta_mirror --history 5   # print the last 5 commits of each target table
```

## Build the image

```bash
docker build -t tsa-delta-mirror:local .
docker run --rm --entrypoint id tsa-delta-mirror:local   # uid=10001
```

The base image is pinned by digest in the `Dockerfile`; runtime dependencies are installed from
`requirements.lock.txt` with `--require-hashes`. Regenerate the lock after changing `pyproject.toml`:

```bash
uv pip compile pyproject.toml --generate-hashes --python-version 3.12 --python-platform linux -o requirements.lock.txt
```

CI (`.github/workflows/tsa-delta-mirror-ci.yml` at the repo root) runs ruff, mypy, pytest,
`terraform validate`, the image build, a CycloneDX SBOM (syft) stored as a build artifact, and
`pip-audit` against the lock. The compose integration job is opt-in via `workflow_dispatch`.

## Deploy with Terraform (GovCloud)

Everything uses `data.aws_partition` (`aws-us-gov`); nothing hardcodes `arn:aws:`.

1. `cp terraform/environments/dev.tfvars.example terraform/environments/dev.tfvars` and fill it in
   (VPC, subnets, route tables, lake bucket + KMS key, provider hosts, tables).
2. Create the ECR repository first so the image can be pushed:
   ```bash
   cd terraform && terraform init
   terraform apply -var-file=environments/dev.tfvars -target=aws_ecr_repository.this
   ```
3. Build, tag with an immutable tag (the git SHA) and push:
   ```bash
   REPO=$(terraform output -raw ecr_repository_url); TAG=sha-$(git rev-parse --short HEAD)
   aws ecr get-login-password | docker login --username AWS --password-stdin "${REPO%%/*}"
   docker build -t "$REPO:$TAG" .. && docker push "$REPO:$TAG"
   ```
4. Full apply with `image_tag = "<TAG>"`:
   ```bash
   terraform apply -var-file=environments/dev.tfvars -var image_tag=$TAG
   ```
5. Put the real profile into the secret (created with a placeholder; Terraform ignores later changes):
   ```bash
   aws secretsmanager put-secret-value --secret-id "$(terraform output -raw profile_secret_arn)" \
     --secret-string file://profile.json   # then shred profile.json
   ```
6. Ask the platform team to allowlist `terraform output egress_allowlist` on the egress path.
7. Smoke test one table and print its Delta history:
   ```bash
   ../scripts/smoke.sh . '[{"share":"provider_share","schema":"provider_schema","table":"incidents","mode":"auto","primary_keys":["incident_id"]}]'
   ```
   If `register_glue = true`, the table is now queryable in Athena (engine v3):
   `SELECT count(*) FROM bronze_shared.incidents;`. From a laptop with a role that can read the
   prefix: `DeltaTable("s3://tsa-data-lake-dev/bronze/shared/provider_share/provider_schema/incidents").to_pandas()`.

What Terraform creates: ECR (immutable tags, scan on push, KMS), ECS cluster + Fargate task
definition (1 vCPU / 4 GB default, read-only root filesystem, non-root), execution/task/scheduler
roles (least privilege: S3 on the lake prefix only, the one secret, the lock table, `PutMetricData`
scoped to the namespace, Glue on the one database, KMS on the one key), one EventBridge schedule per
table, the DynamoDB run-lock table (TTL), the Secrets Manager secret (placeholder value), the log
group (90-day retention), alarms (`run-failed` per table, `missed-run` per table over 2× the
interval, `token-expiring-soon`), security groups and VPC endpoints. Optionally the lake bucket
itself (`create_lake_bucket = true`: versioning, SSE-KMS, deny unencrypted/insecure puts).

Secrets never appear in the task definition, the environment, the logs or Terraform state beyond
the secret resource (whose stored value is the placeholder).

## Configuration

All configuration is environment variables validated by pydantic (`config.py`).

| Variable | Default | Meaning |
|---|---|---|
| `MIRROR_PROFILE_SECRET_ARN` | required | Secrets Manager secret holding the profile JSON |
| `MIRROR_TABLES` | required | JSON list of tables (below) |
| `MIRROR_LAKE_BUCKET` | required | lake bucket |
| `MIRROR_LAKE_PREFIX` | `bronze/shared` | `s3://bucket/<prefix>/<share>/<schema>/<table>/` |
| `MIRROR_KMS_KEY_ID` | unset | CMK for SSE-KMS on every write |
| `MIRROR_LOCK_TABLE` | unset | DynamoDB run-lock table (unset = no lock) |
| `MIRROR_RUN_LOCK_TTL_SECONDS` | `10800` | stale-lock expiry |
| `MIRROR_REGISTER_GLUE` / `MIRROR_GLUE_DATABASE` | `false` / `bronze_shared` | Glue registration |
| `MIRROR_METRICS_NAMESPACE` / `MIRROR_METRICS_ENABLED` | `TSA/DeltaMirror` / `true` | CloudWatch metrics |
| `MIRROR_LOG_LEVEL` | `INFO` | JSON logs to stdout; no row-level data is ever logged |
| `MIRROR_TOKEN_EXPIRY_WARN_DAYS` | `7` | `TokenExpiringSoon` threshold |
| `MIRROR_LARGE_SNAPSHOT_ROW_WARN` | `5000000` | WARN above this many rows in one snapshot |
| `AWS_REGION` | `us-gov-west-1` | region for delta-rs and boto3 |
| `MIRROR_S3_ENDPOINT_URL`, `MIRROR_S3_ALLOW_HTTP`, `MIRROR_LAKE_ROOT` | unset | **tests only** (MinIO / moto / local lake) |

`MIRROR_TABLES` entry:

```json
{
  "share": "provider_share", "schema": "provider_schema", "table": "provider_table",
  "mode": "auto | cdf | snapshot",
  "primary_keys": ["id"],               // required for cdf; needed for auto to use CDF
  "partition_by": ["ingest_date"],      // optional; ingest_date is synthesised in snapshot mode
  "target_name": "optional_override",
  "snapshot_schema_mode": "overwrite | merge",   // default overwrite (bronze mirror)
  "use_delta_format": null,             // null = let the client negotiate parquet vs delta responses
  "keep_cdf_lineage": true              // source_commit_version / source_commit_timestamp columns
}
```

## Operations

**Rotating the token.** The provider issues a new recipient profile. Put it in the secret:
`aws secretsmanager put-secret-value --secret-id <arn> --secret-string file://profile.json`.
The next run picks it up; nothing restarts. `TokenExpiringSoon` alarms 7 days before
`expirationTime`; the run itself keeps succeeding until the provider rejects the old token.

**Adding a table.** Append to `tables` in the tfvars (share, schema, table, mode, primary keys,
schedule) and `terraform apply`. A new schedule and alarms appear; the first run is a snapshot.

**Raising task size.** Set `task_cpu` / `task_memory` (Fargate pairs: 1024/2048–8192,
2048/4096–16384, 4096/8192–30720, 8192/16384–61440, 16384/32768–122880). The mirror logs
`large_snapshot` when a snapshot exceeds `MIRROR_LARGE_SNAPSHOT_ROW_WARN` rows; size memory at
roughly 3–5× the parquet size of the largest table. Once the initial load is done, CDF runs are small.

**Forcing a full reload.** Set the table's `mode` to `snapshot` for one run (or delete the target
prefix); the next run overwrites the table at the current provider version and the watermark resets.

**Reading the logs.** One JSON object per line in `/ecs/<name>`. Useful events: `table_state`
(remote vs last version, CDF availability), `cdf_collapsed`, `merge_metrics`, `table_written`,
`up_to_date`, `cdf_fallback_to_snapshot`, `table_failed`, `run_end`.

**Metrics** (namespace `TSA/DeltaMirror`, dimension `Table=<share.schema.table>`): `Success` (0/1),
`RowsWritten`, `SourceVersion`, `DurationSeconds`, `Mode` (0 noop, 1 cdf, 2 snapshot),
`LockContended`, and `TokenExpiringSoon` (no dimension).

## Databricks later: registering the tables in Unity Catalog

No data moves. Create a storage credential and external location for `s3://<lake-bucket>/bronze/shared/`,
then register each table as an **external** Delta table on its existing path:

```sql
CREATE TABLE bronze_shared.provider_share_provider_schema.incidents
USING DELTA
LOCATION 's3://tsa-data-lake-prod/bronze/shared/provider_share/provider_schema/incidents/';
```

The mirror can keep running alongside (single writer per table still holds, since Databricks only
reads) until native ingestion replaces it. Commit metadata (`mirror.source_version`) is visible in
`DESCRIBE HISTORY`. See [ADR 0005](docs/decisions/0005-glue-registration-and-unity-catalog.md).

**Lambda later.** The image exposes `tsa_delta_mirror.main.lambda_handler`; a Lambda deployment
would add `awslambdaric` as the entrypoint and pass `{"tables": [...]}` in the event. Not shipped in v1.

## Security and ATO notes

- Token only in Secrets Manager; exception text and log fields pass through a redactor.
- Image: digest-pinned base, hash-pinned dependencies, non-root UID 10001, read-only root
  filesystem, `/tmp` volume, SBOM + `pip-audit` in CI, ECR scan on push.
- All S3 writes use SSE-KMS with the customer-managed key; the optional bucket policy denies
  unencrypted and non-TLS puts.
- Outbound: AWS service endpoints (via VPC endpoints), the provider's sharing endpoint, the
  provider's pre-signed storage host(s). Nothing else.
- Logs carry counts, versions and schemas only; never row values.

## Known limitations

- The `delta-sharing` client materialises each snapshot / CDF range in memory as pandas before we
  convert to Arrow; `convert_in_batches` reduces peak memory but does not stream. Size the task.
- Types travel through pandas; we re-type from the share's schema (ints with nulls, decimals,
  dates, UTC timestamps are preserved). Complex types (struct/array/map) are inferred.
- CDF needs the provider to enable it; `primary_keys` must be truly unique.
- Column **type changes** are not applied in CDF mode (clear error; rebuild with snapshot mode).
  Added columns are merged automatically.
- Shares that require deletion vectors or column mapping are served in "delta" response format;
  the client handles them via `delta-kernel-rust` when `use_delta_format` is `null` or `true`.
- One writer per target table. Do not point another job at the same S3 path.

## Decisions

See `docs/decisions/`: [no Spark / delta-rs](docs/decisions/0001-no-spark-delta-rs.md),
[S3 commit atomicity + run lock](docs/decisions/0002-s3-commit-atomicity.md),
[profile in memory](docs/decisions/0003-profile-in-memory.md),
[CDF collapse](docs/decisions/0004-cdf-collapse.md),
[Glue + Unity Catalog](docs/decisions/0005-glue-registration-and-unity-catalog.md),
[one schedule per table](docs/decisions/0006-one-schedule-per-table.md).
