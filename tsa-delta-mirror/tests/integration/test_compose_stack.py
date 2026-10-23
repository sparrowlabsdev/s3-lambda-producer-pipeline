"""Runs against docker-compose.test.yml (reference server + MinIO). Opt in with ``-m compose``.

docker compose -f docker-compose.test.yml up -d --wait && python scripts/seed_share.py
pytest -m compose tests/integration/test_compose_stack.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import boto3
import pytest
from deltalake import DeltaTable

from tsa_delta_mirror.config import Settings
from tsa_delta_mirror.main import Clients, run
from tsa_delta_mirror.writer import read_watermark

pytestmark = pytest.mark.compose

SHARING = os.environ.get("SHARING_ENDPOINT", "http://localhost:8080/delta-sharing")
MINIO = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def clients(moto_endpoint: str) -> Clients:
    session = boto3.session.Session(region_name="us-east-1")
    sm = session.client("secretsmanager", endpoint_url=moto_endpoint)
    profile = {
        "shareCredentialsVersion": 1,
        "endpoint": SHARING,
        "bearerToken": "compose-test-token",
    }
    sm.create_secret(Name="compose/profile", SecretString=json.dumps(profile))
    return Clients(sm)


def make_settings(clients: Clients, tables: list[dict[str, object]]) -> Settings:
    arn = clients.secretsmanager.describe_secret(SecretId="compose/profile")["ARN"]
    return Settings(
        profile_secret_arn=arn,
        tables=tables,  # type: ignore[arg-type]
        lake_bucket="lake",
        lake_prefix="bronze/shared",
        s3_endpoint_url=MINIO,
        s3_allow_http=True,
        metrics_enabled=False,
    )


def test_reference_server_round_trip(clients: Clients, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.delenv("AWS_SECURITY_TOKEN", raising=False)
    tables = [
        {
            "share": "gov",
            "schema": "ops",
            "table": "people_cdf",
            "mode": "auto",
            "primary_keys": ["id"],
        },
        {"share": "gov", "schema": "ops", "table": "people_plain", "mode": "auto"},
    ]
    settings = make_settings(clients, tables)
    storage = settings.storage_options()

    first = run(settings, clients)
    assert first.exit_code == 0, [o.error for o in first.failed]
    assert all(o.mode == "snapshot" for o in first.outcomes)
    second = run(settings, clients)
    assert all(o.mode == "noop" for o in second.outcomes)

    subprocess.run(  # noqa: S603 - fixed argv, our own script
        [sys.executable, str(ROOT / "scripts" / "seed_share.py"), "--change"], check=True
    )
    third = run(settings, clients)
    assert third.exit_code == 0, [o.error for o in third.failed]
    cdf, plain = third.outcomes
    assert cdf.mode == "cdf" and plain.mode == "snapshot"
    uri = settings.target_uri(settings.tables[0])
    mirrored = DeltaTable(uri, storage_options=storage)
    ids = sorted(r["id"] for r in mirrored.to_pyarrow_table().to_pylist())
    assert ids == [2, 3, 4]
    assert read_watermark(mirrored.history()) == cdf.source_version
