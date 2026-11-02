from __future__ import annotations

import io
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import boto3
import pytest
from moto import mock_aws

from delta_sharing_mirror.errors import ConfigError
from delta_sharing_mirror.log import capture_client_stdout, configure_logging, get_logger, redact
from delta_sharing_mirror.profile import load_profile, parse_expiration, profile_from_json


@pytest.fixture
def debug_logging() -> Iterator[None]:
    """Client stdout is forwarded at DEBUG; raise the level for these tests only."""
    configure_logging("DEBUG")
    yield
    configure_logging("INFO")


TOKEN = "dapi1234567890abcdefSECRET"


def profile_json(expires: datetime | None) -> str:
    data = {
        "shareCredentialsVersion": 1,
        "endpoint": "https://sharing.example.gov/delta-sharing/",
        "bearerToken": TOKEN,
    }
    if expires:
        data["expirationTime"] = expires.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return json.dumps(data)


def test_profile_parses_and_strips_trailing_slash() -> None:
    soon = datetime.now(UTC) + timedelta(days=3)
    profile = profile_from_json(profile_json(soon))
    assert profile.endpoint_host == "sharing.example.gov"
    assert profile.profile.endpoint == "https://sharing.example.gov/delta-sharing"
    assert profile.expiring_within(7) and not profile.expiring_within(1)
    far = profile_from_json(profile_json(datetime.now(UTC) + timedelta(days=90)))
    assert not far.expiring_within(7)
    assert profile_from_json(profile_json(None)).days_until_expiry() is None


def test_parse_expiration_variants() -> None:
    assert parse_expiration("2027-01-01T00:00:00Z") == datetime(2027, 1, 1, tzinfo=UTC)
    assert parse_expiration("2027-01-01T00:00:00+00:00") == datetime(2027, 1, 1, tzinfo=UTC)
    assert parse_expiration("2027-01-01T00:00:00") == datetime(2027, 1, 1, tzinfo=UTC)
    assert parse_expiration(None) is None


def test_invalid_profile_never_echoes_token() -> None:
    with pytest.raises(ConfigError) as info:
        profile_from_json(json.dumps({"shareCredentialsVersion": 1, "bearerToken": TOKEN}))
    assert TOKEN not in str(info.value)
    with pytest.raises(ConfigError) as info:
        profile_from_json(
            json.dumps({"shareCredentialsVersion": 2, "endpoint": "x", "type": "nope"})
        )
    assert TOKEN not in str(info.value)


@mock_aws
def test_load_profile_from_secrets_manager() -> None:
    client = boto3.client("secretsmanager", region_name="us-east-1")
    arn = client.create_secret(Name="delta-sharing/provider", SecretString=profile_json(None))[
        "ARN"
    ]
    profile = load_profile(arn, client)
    assert profile.profile.bearer_token == TOKEN
    binary_arn = client.create_secret(Name="bin", SecretBinary=b"\x00")["ARN"]
    with pytest.raises(ConfigError, match="SecretString"):
        load_profile(binary_arn, client)


def test_redaction() -> None:
    text = f'Authorization: Bearer {TOKEN} and "bearerToken": "{TOKEN}" X-Amz-Signature=abcdef0123'
    out = redact(text)
    assert TOKEN not in out and "abcdef0123" not in out
    assert "[REDACTED]" in out


def test_capture_client_stdout(debug_logging: None, capsys: pytest.CaptureFixture[str]) -> None:
    log = get_logger("test")
    with capture_client_stdout(log):
        print("Received 3 batches of data.")  # what the delta-sharing client does
    captured = capsys.readouterr().out
    lines = [json.loads(line) for line in captured.strip().splitlines() if line.strip()]
    assert all(isinstance(line, dict) for line in lines)
    assert any(line.get("message") == "Received 3 batches of data." for line in lines)


def test_logger_redacts_fields(capsys: pytest.CaptureFixture[str]) -> None:
    get_logger("test").info("x", detail=f"Bearer {TOKEN}")
    assert TOKEN not in capsys.readouterr().out


def test_logger_writer_flushes_partial_line(
    debug_logging: None, capsys: pytest.CaptureFixture[str]
) -> None:
    log = get_logger("test")
    with capture_client_stdout(log):
        io.TextIOWrapper  # noqa: B018 - keep import used
        print("partial", end="")
    assert "partial" in capsys.readouterr().out
