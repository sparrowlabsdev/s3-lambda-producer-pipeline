from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from tsa_delta_mirror.log import configure_logging


@pytest.fixture(autouse=True, scope="session")
def _logging() -> None:
    configure_logging("INFO")


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Fake credentials so neither boto3 (moto) nor delta-rs ever reach a real account."""
    for key, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SECURITY_TOKEN": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_REGION": "us-east-1",
        "AWS_EC2_METADATA_DISABLED": "true",
    }.items():
        monkeypatch.setenv(key, value)
    for key in list(os.environ):
        if key.startswith("MIRROR_"):
            monkeypatch.delenv(key)
    yield
