from __future__ import annotations

import datetime as dt
import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import boto3
import pyarrow as pa
import pytest
import requests

from delta_sharing_mirror.main import Clients
from tests.integration.fake_sharing_server import FakeShare, FakeSharingServer, FakeTable

TOKEN = "test-token-0123456789"
REGION = "us-east-1"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope="session")
def moto_endpoint() -> Iterator[str]:
    """A moto server: S3 for delta-rs (which is not botocore) plus the other AWS APIs."""
    port = _free_port()
    proc = subprocess.Popen(  # noqa: S603
        [sys.executable, "-m", "moto.server", "-p", str(port), "-H", "127.0.0.1"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={**os.environ, "MOTO_ALLOW_NONEXISTENT_REGION": "true"},
    )
    url = f"http://127.0.0.1:{port}"
    for _ in range(600):  # cold starts (fresh install, slow CI runner) can take a while
        try:
            requests.get(url, timeout=0.5)
            break
        except requests.ConnectionError:
            time.sleep(0.1)
    else:
        proc.kill()
        pytest.fail("moto server did not start")
    yield url
    proc.kill()


PEOPLE = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("qty", pa.int32()),
        pa.field("seen", pa.timestamp("us", tz="UTC")),
        pa.field("day", pa.date32()),
    ]
)


def people(rows: list[tuple[int, str | None, int | None]]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array([r[0] for r in rows], pa.int64()),
            "name": pa.array([r[1] for r in rows], pa.string()),
            "qty": pa.array([r[2] for r in rows], pa.int32()),
            "seen": pa.array(
                [dt.datetime(2026, 1, 1, 12, tzinfo=dt.UTC)] * len(rows),
                pa.timestamp("us", tz="UTC"),
            ),
            "day": pa.array([dt.date(2026, 1, 1)] * len(rows), pa.date32()),
        }
    )


@dataclass
class Stack:
    share: FakeShare
    server: FakeSharingServer
    clients: Clients
    secret_arn: str
    endpoint: str
    moto: str


@pytest.fixture
def stack(moto_endpoint: str) -> Iterator[Stack]:
    share = FakeShare(token=TOKEN)
    share.add("gov", "ops", "people_cdf", FakeTable(PEOPLE, ["id"], cdf_enabled=True)).overwrite(
        people([(1, "ann", 1), (2, "bob", None), (3, "cy", 3)])
    )
    share.add("gov", "ops", "people_plain", FakeTable(PEOPLE, ["id"], cdf_enabled=False)).overwrite(
        people([(10, "x", 1), (11, "y", 2)])
    )
    server = FakeSharingServer(share).start()

    session = boto3.session.Session(region_name=REGION)
    kw = {"endpoint_url": moto_endpoint}
    sm = session.client("secretsmanager", **kw)
    ddb = session.client("dynamodb", **kw)
    cw = session.client("cloudwatch", **kw)
    glue = session.client("glue", **kw)
    profile = {
        "shareCredentialsVersion": 1,
        "endpoint": server.endpoint,
        "bearerToken": TOKEN,
        "expirationTime": "2030-01-01T00:00:00Z",
    }
    name = f"delta-sharing/test-{_free_port()}"
    arn = sm.create_secret(Name=name, SecretString=json.dumps(profile))["ARN"]
    try:
        ddb.create_table(
            TableName="mirror-locks",
            KeySchema=[{"AttributeName": "table_key", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "table_key", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
    except ddb.exceptions.ResourceInUseException:
        pass
    try:
        glue.create_database(DatabaseInput={"Name": "bronze_shared"})
    except glue.exceptions.AlreadyExistsException:
        pass
    yield Stack(share, server, Clients(sm, ddb, cw, glue), arn, server.endpoint, moto_endpoint)
    server.stop()


@pytest.fixture
def lake_root(tmp_path: Path) -> str:
    return (tmp_path / "lake").as_uri()
