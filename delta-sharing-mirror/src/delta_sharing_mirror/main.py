"""Entrypoint: iterate the configured tables, mirror each one, emit metrics, set the exit code.

Per table (see README "How it works"):
  1. remote_version = share's current version; metadata = share's schema/config
  2. last_version   = watermark from the target table's own commit history (-1 if none)
  3. up to date?    -> RowsWritten=0, no commit
  4. CDF possible?  -> load changes (last+1 .. remote), collapse, one MERGE commit
     otherwise     -> load snapshot at remote_version, one overwrite commit
  5. every commit carries mirror.source_version (the watermark) in its metadata
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import boto3
import pyarrow as pa
import structlog

from delta_sharing_mirror import __version__
from delta_sharing_mirror.catalog import register_table
from delta_sharing_mirror.cdf import collapse_changes
from delta_sharing_mirror.config import Settings, TableConfig
from delta_sharing_mirror.errors import CdfUnavailableError, LockHeldError, MirrorError
from delta_sharing_mirror.locks import RunLock
from delta_sharing_mirror.log import configure_logging, get_logger, redact
from delta_sharing_mirror.metrics import Metrics
from delta_sharing_mirror.profile import SharingProfile, load_profile
from delta_sharing_mirror.reader import SharingReader, SourceTable
from delta_sharing_mirror.writer import NO_WATERMARK, CommitMetadata, DeltaWriter, WriteResult

if TYPE_CHECKING:
    from mypy_boto3_cloudwatch import CloudWatchClient
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_secretsmanager import SecretsManagerClient

log = get_logger("delta_sharing_mirror")

INGEST_DATE = "ingest_date"
MODE_METRIC = {"noop": 0, "cdf": 1, "snapshot": 2}


@dataclass
class Clients:
    secretsmanager: SecretsManagerClient
    dynamodb: DynamoDBClient | None = None
    cloudwatch: CloudWatchClient | None = None
    glue: GlueClient | None = None

    @classmethod
    def from_settings(cls, settings: Settings) -> Clients:
        session = boto3.session.Session(region_name=settings.aws_region)
        return cls(
            secretsmanager=session.client("secretsmanager"),
            dynamodb=session.client("dynamodb") if settings.lock_table else None,
            cloudwatch=session.client("cloudwatch") if settings.metrics_enabled else None,
            glue=session.client("glue") if settings.register_glue else None,
        )


@dataclass
class TableOutcome:
    table: str
    mode: str
    rows_written: int = 0
    source_version: int = NO_WATERMARK
    target_version: int | None = None
    success: bool = True
    skipped: bool = False
    error: str | None = None


@dataclass
class RunSummary:
    run_id: str
    outcomes: list[TableOutcome] = field(default_factory=list)

    @property
    def failed(self) -> list[TableOutcome]:
        return [o for o in self.outcomes if not o.success]

    @property
    def exit_code(self) -> int:
        return 1 if self.failed else 0


def _add_ingest_date(data: pa.Table) -> pa.Table:
    today = datetime.now(UTC).date()
    column = pa.array([today] * data.num_rows, pa.date32())
    return data.append_column(INGEST_DATE, column)


def mirror_table(
    cfg: TableConfig,
    settings: Settings,
    reader: SharingReader,
    writer: DeltaWriter,
    metrics: Metrics,
    clients: Clients,
    run_id: str,
) -> TableOutcome:
    """Mirror one table. Raises ``MirrorError``/other exceptions to the caller."""
    source = SourceTable(cfg)
    uri = settings.target_uri(cfg)
    tlog = log.bind(table=cfg.source_name, target=uri)
    outcome = TableOutcome(table=cfg.source_name, mode="noop")

    remote_version = reader.table_version(source)
    metadata = reader.table_metadata(source)
    cdf_enabled = SharingReader.cdf_enabled(metadata)
    target = writer.open(uri)
    last_version = writer.watermark(target)
    outcome.source_version = remote_version
    tlog.info(
        "table_state",
        remote_version=remote_version,
        last_version=last_version,
        target_exists=target is not None,
        cdf_enabled=cdf_enabled,
        requested_mode=cfg.mode,
    )

    if cfg.mode == "cdf" and not cdf_enabled:
        raise CdfUnavailableError(
            f"{cfg.source_name}: mode=cdf but the share does not report "
            "delta.enableChangeDataFeed=true. Ask the provider to enable CDF or use mode=snapshot."
        )
    if target is not None and last_version == NO_WATERMARK:
        tlog.warning("target_without_watermark", detail="snapshot reload will overwrite it")
    if remote_version <= last_version:
        tlog.info("up_to_date", remote_version=remote_version)
        outcome.target_version = target.version() if target is not None else None
        return outcome

    result: WriteResult | None = None
    commit = CommitMetadata(
        source_version=remote_version,
        source=cfg.source_name,
        mode="cdf",
        run_id=run_id,
        run_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )

    can_cdf = cfg.mode in ("cdf", "auto") and cdf_enabled and target is not None
    can_cdf = can_cdf and last_version != NO_WATERMARK and bool(cfg.primary_keys)
    if cfg.mode == "auto" and cdf_enabled and target is not None and not cfg.primary_keys:
        tlog.warning("cdf_skipped_no_primary_keys", detail="falling back to snapshot overwrite")

    if can_cdf and target is not None:
        try:
            changes = reader.load_changes(source, last_version + 1, remote_version, metadata)
        except Exception as exc:
            if cfg.mode == "cdf":
                raise CdfUnavailableError(
                    f"{cfg.source_name}: CDF query for versions "
                    f"{last_version + 1}-{remote_version} failed: {redact(str(exc))[:300]}"
                ) from None
            tlog.warning("cdf_fallback_to_snapshot", reason=redact(str(exc))[:300])
        else:
            collapsed = collapse_changes(changes, cfg.primary_keys, cfg.keep_cdf_lineage)
            tlog.info(
                "cdf_collapsed",
                input_rows=collapsed.inputs,
                upserts=collapsed.upserts,
                deletes=collapsed.deletes,
            )
            outcome.mode = "cdf"
            if collapsed.is_empty:
                # The provider advanced (OPTIMIZE, property change) without row changes.
                # delta-rs does not commit a MERGE with no source rows, so the watermark stays
                # where it is; the next run re-queries this (empty) range, which is cheap.
                tlog.info("cdf_no_row_changes", detail="no commit; watermark unchanged")
                outcome.target_version = target.version()
                return outcome
            result = writer.merge_changes(
                target, collapsed.rows, primary_keys=cfg.primary_keys, metadata=commit
            )

    if result is None:
        outcome.mode = "snapshot"
        commit = CommitMetadata(
            source_version=remote_version,
            source=cfg.source_name,
            mode="snapshot",
            run_id=run_id,
            run_at=commit.run_at,
        )
        data = reader.load_snapshot(source, remote_version, metadata)
        if INGEST_DATE in cfg.partition_by and INGEST_DATE not in data.column_names:
            data = _add_ingest_date(data)
        if data.num_rows > settings.large_snapshot_row_warn:
            tlog.warning(
                "large_snapshot",
                rows=data.num_rows,
                detail="consider a larger task size (ecs_task_memory) or asking for CDF",
            )
        result = writer.write_snapshot(
            uri,
            data,
            partition_by=cfg.partition_by,
            schema_mode=cfg.snapshot_schema_mode,
            metadata=commit,
        )

    outcome.rows_written = result.rows_written
    outcome.target_version = result.target_version
    tlog.info(
        "table_written",
        operation=result.operation,
        rows_written=result.rows_written,
        target_version=result.target_version,
        source_version=remote_version,
    )

    if settings.register_glue and clients.glue is not None:
        written = writer.open(uri)
        if written is not None:
            register_table(
                clients.glue,
                settings.glue_database,
                cfg.target_table,
                uri,
                writer.target_schema(written),
                cfg.partition_by,
            )
    return outcome


def _emit(metrics: Metrics, outcome: TableOutcome, duration: float) -> None:
    t = outcome.table
    metrics.put("Success", 1 if outcome.success else 0, table=t)
    metrics.put("RowsWritten", outcome.rows_written, table=t)
    metrics.put("DurationSeconds", duration, unit="Seconds", table=t)
    metrics.put("Mode", MODE_METRIC.get(outcome.mode, -1), unit="None", table=t)
    if outcome.source_version >= 0:
        metrics.put("SourceVersion", outcome.source_version, unit="None", table=t)
    if outcome.skipped:
        metrics.put("LockContended", 1, table=t)


def check_token_expiry(profile: SharingProfile, settings: Settings, metrics: Metrics) -> None:
    days = profile.days_until_expiry()
    expiring = profile.expiring_within(settings.token_expiry_warn_days)
    metrics.put("TokenExpiringSoon", 1 if expiring else 0)
    if expiring:
        log.warning(
            "token_expiring_soon",
            endpoint_host=profile.endpoint_host,
            days_remaining=round(days, 1) if days is not None else None,
            detail="rotate the recipient token in Secrets Manager",
        )
    else:
        log.info(
            "profile_loaded",
            endpoint_host=profile.endpoint_host,
            days_remaining=round(days, 1) if days is not None else None,
        )


def run(settings: Settings, clients: Clients | None = None) -> RunSummary:
    run_id = str(uuid.uuid4())
    structlog.contextvars.bind_contextvars(run_id=run_id)
    summary = RunSummary(run_id=run_id)
    clients = clients or Clients.from_settings(settings)
    metrics = Metrics(clients.cloudwatch, settings.metrics_namespace)
    log.info(
        "run_start",
        version=__version__,
        tables=[t.source_name for t in settings.tables],
        lake=settings.lake_uri(),
        region=settings.aws_region,
    )

    profile = load_profile(settings.profile_secret_arn, clients.secretsmanager)
    check_token_expiry(profile, settings, metrics)
    reader = SharingReader(profile)
    writer = DeltaWriter(settings.storage_options())

    try:
        for cfg in settings.tables:
            started = time.monotonic()
            lock = RunLock(
                clients.dynamodb,
                settings.lock_table,
                cfg.lock_key,
                run_id,
                settings.run_lock_ttl_seconds,
            )
            outcome = TableOutcome(table=cfg.source_name, mode="noop")
            try:
                with lock:
                    outcome = mirror_table(cfg, settings, reader, writer, metrics, clients, run_id)
            except LockHeldError as exc:
                outcome.skipped = True
                log.warning("table_skipped_lock_held", table=cfg.source_name, detail=str(exc))
            except MirrorError as exc:
                outcome.success = False
                outcome.error = redact(str(exc))
                log.error("table_failed", table=cfg.source_name, error=outcome.error)
            except Exception as exc:  # one table must not stop the others
                outcome.success = False
                outcome.error = redact(f"{type(exc).__name__}: {exc}")
                log.exception("table_failed", table=cfg.source_name, error=outcome.error)
            _emit(metrics, outcome, time.monotonic() - started)
            summary.outcomes.append(outcome)
    finally:
        metrics.flush()
        reader.close()

    log.info(
        "run_end",
        succeeded=[o.table for o in summary.outcomes if o.success and not o.skipped],
        skipped=[o.table for o in summary.outcomes if o.skipped],
        failed=[o.table for o in summary.failed],
        exit_code=summary.exit_code,
    )
    return summary


def print_history(settings: Settings, limit: int) -> None:
    """Used by scripts/smoke.sh: show the commit history of each target table."""
    writer = DeltaWriter(settings.storage_options())
    for cfg in settings.tables:
        table = writer.open(settings.target_uri(cfg))
        history = table.history(limit) if table is not None else []
        log.info(
            "target_history",
            table=cfg.source_name,
            history=json.loads(json.dumps(history, default=str)),
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="delta-sharing-mirror")
    parser.add_argument(
        "--history", type=int, metavar="N", help="print last N commits per table and exit"
    )
    args = parser.parse_args(argv)
    settings = Settings()  # populated from the environment
    configure_logging(settings.log_level)
    if args.history is not None:
        print_history(settings, args.history)
        return 0
    return run(settings).exit_code


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """Lambda-compatible entrypoint (not deployed in v1). ``event['tables']`` may override."""
    overrides: dict[str, Any] = {}
    if event and "tables" in event:
        overrides["tables"] = event["tables"]
    settings = Settings(**overrides)
    configure_logging(settings.log_level)
    summary = run(settings)
    if summary.exit_code:
        raise RuntimeError(
            f"{len(summary.failed)} table(s) failed: {[o.table for o in summary.failed]}"
        )
    return {"run_id": summary.run_id, "tables": [o.__dict__ for o in summary.outcomes]}


if __name__ == "__main__":
    sys.exit(main())
