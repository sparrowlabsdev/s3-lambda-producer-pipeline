"""Typed configuration. Everything comes from environment variables (12-factor).

Secrets never live here: the Delta Sharing profile is fetched from Secrets Manager at run time.
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import AliasChoices, BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Mode = Literal["auto", "cdf", "snapshot"]
SchemaMode = Literal["overwrite", "merge"]

_IDENT = re.compile(r"^[A-Za-z0-9_.-]+$")


class TableConfig(BaseModel):
    """One shared table to mirror. Mirrors the ``MIRROR_TABLES`` JSON entry schema."""

    share: str
    schema_: str = Field(alias="schema")
    table: str
    mode: Mode = "auto"
    primary_keys: list[str] = Field(default_factory=list)
    partition_by: list[str] = Field(default_factory=list)
    target_name: str | None = None
    # Snapshot-mode schema handling. "overwrite" is acceptable for bronze mirrors (default).
    snapshot_schema_mode: SchemaMode = "overwrite"
    # None = let the client auto-negotiate parquet vs delta response format for snapshots.
    use_delta_format: bool | None = None
    # Keep _commit_version/_commit_timestamp from CDF as lineage columns on the target.
    keep_cdf_lineage: bool = True

    model_config = {"populate_by_name": True, "extra": "forbid"}

    @field_validator("share", "schema_", "table", "target_name")
    @classmethod
    def _safe_identifier(cls, value: str | None) -> str | None:
        if value is not None and not _IDENT.match(value):
            msg = f"identifier {value!r} contains characters not allowed in an S3 path"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _cdf_requires_keys(self) -> TableConfig:
        if self.mode == "cdf" and not self.primary_keys:
            msg = f"{self.source_name}: mode=cdf requires non-empty primary_keys"
            raise ValueError(msg)
        return self

    @property
    def source_name(self) -> str:
        return f"{self.share}.{self.schema_}.{self.table}"

    @property
    def target_table(self) -> str:
        return self.target_name or self.table

    @property
    def target_relpath(self) -> str:
        return f"{self.share}/{self.schema_}/{self.target_table}"

    @property
    def lock_key(self) -> str:
        return self.target_relpath


class Settings(BaseSettings):
    """Process-wide settings. Prefix ``MIRROR_`` except for the standard AWS variables."""

    model_config = SettingsConfigDict(env_prefix="MIRROR_", extra="ignore")

    profile_secret_arn: str
    tables: list[TableConfig]
    lake_bucket: str
    lake_prefix: str = "bronze/shared"
    lock_table: str | None = None
    run_lock_ttl_seconds: int = 3 * 3600
    register_glue: bool = False
    glue_database: str = "bronze_shared"
    metrics_namespace: str = "DeltaMirror"
    metrics_enabled: bool = True
    log_level: str = "INFO"
    kms_key_id: str | None = None
    token_expiry_warn_days: int = 7
    large_snapshot_row_warn: int = 5_000_000
    # Test / local-stack knobs. Never set in production.
    s3_endpoint_url: str | None = None
    s3_allow_http: bool = False
    # Full URI root (e.g. file:///tmp/lake) that replaces s3://<bucket>/<prefix>. Tests only.
    lake_root: str | None = None
    aws_region: str = Field(
        default="us-gov-west-1",
        validation_alias=AliasChoices("AWS_REGION", "AWS_DEFAULT_REGION", "MIRROR_AWS_REGION"),
    )

    @field_validator("lake_prefix")
    @classmethod
    def _strip_slashes(cls, value: str) -> str:
        return value.strip("/")

    @field_validator("tables")
    @classmethod
    def _unique_targets(cls, tables: list[TableConfig]) -> list[TableConfig]:
        if not tables:
            msg = "MIRROR_TABLES must contain at least one table"
            raise ValueError(msg)
        seen: set[str] = set()
        for t in tables:
            if t.target_relpath in seen:
                msg = f"two tables resolve to the same target path {t.target_relpath}"
                raise ValueError(msg)
            seen.add(t.target_relpath)
        return tables

    def lake_uri(self) -> str:
        if self.lake_root:
            return self.lake_root.rstrip("/")
        prefix = f"/{self.lake_prefix}" if self.lake_prefix else ""
        return f"s3://{self.lake_bucket}{prefix}"

    def target_uri(self, table: TableConfig) -> str:
        return f"{self.lake_uri()}/{table.target_relpath}"

    def storage_options(self) -> dict[str, str]:
        """Options handed to delta-rs for S3 access.

        delta-rs 1.x commits with S3 conditional writes (If-None-Match), so no DynamoDB
        lock table is configured here. See docs/decisions/0002-s3-commit-atomicity.md.
        """
        opts: dict[str, str] = {"AWS_REGION": self.aws_region}
        if self.kms_key_id:
            opts["AWS_SERVER_SIDE_ENCRYPTION"] = "aws:kms"
            opts["AWS_SSE_KMS_KEY_ID"] = self.kms_key_id
        if self.s3_endpoint_url:
            opts["AWS_ENDPOINT_URL"] = self.s3_endpoint_url
        if self.s3_allow_http:
            opts["AWS_ALLOW_HTTP"] = "true"
        return opts
