from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from delta_sharing_mirror.config import Settings, TableConfig

BASE = {
    "profile_secret_arn": "arn:aws-us-gov:secretsmanager:us-gov-west-1:1:secret:x",
    "lake_bucket": "lake",
}


def test_cdf_mode_requires_primary_keys() -> None:
    with pytest.raises(ValidationError, match="primary_keys"):
        TableConfig(share="s", schema="sch", table="t", mode="cdf")  # type: ignore[call-arg]
    ok = TableConfig(share="s", schema="sch", table="t", mode="cdf", primary_keys=["id"])  # type: ignore[call-arg]
    assert ok.source_name == "s.sch.t" and ok.target_relpath == "s/sch/t"


def test_identifiers_must_be_path_safe() -> None:
    with pytest.raises(ValidationError, match="not allowed"):
        TableConfig(share="s", schema="sch", table="../t")  # type: ignore[call-arg]


def test_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MIRROR_PROFILE_SECRET_ARN", BASE["profile_secret_arn"])
    monkeypatch.setenv("MIRROR_LAKE_BUCKET", "example-data-lake-dev")
    monkeypatch.setenv("MIRROR_LAKE_PREFIX", "/bronze/shared/")
    monkeypatch.setenv("MIRROR_KMS_KEY_ID", "arn:aws-us-gov:kms:us-gov-west-1:1:key/k")
    monkeypatch.setenv("AWS_REGION", "us-gov-west-1")
    monkeypatch.setenv(
        "MIRROR_TABLES",
        json.dumps(
            [
                {"share": "a", "schema": "b", "table": "c", "mode": "auto", "primary_keys": ["id"]},
                {"share": "a", "schema": "b", "table": "d", "target_name": "d_mirror"},
            ]
        ),
    )
    settings = Settings()
    assert settings.lake_prefix == "bronze/shared"
    assert (
        settings.target_uri(settings.tables[1])
        == "s3://example-data-lake-dev/bronze/shared/a/b/d_mirror"
    )
    opts = settings.storage_options()
    assert opts["AWS_REGION"] == "us-gov-west-1"
    assert opts["AWS_SERVER_SIDE_ENCRYPTION"] == "aws:kms"
    assert opts["AWS_SSE_KMS_KEY_ID"].endswith("key/k")
    assert "AWS_ENDPOINT_URL" not in opts


def test_duplicate_targets_rejected() -> None:
    tables = [
        {"share": "a", "schema": "b", "table": "c"},
        {"share": "a", "schema": "b", "table": "x", "target_name": "c"},
    ]
    with pytest.raises(ValidationError, match="same target path"):
        Settings(tables=tables, **BASE)  # type: ignore[arg-type]


def test_empty_tables_rejected() -> None:
    with pytest.raises(ValidationError, match="at least one"):
        Settings(tables=[], **BASE)
