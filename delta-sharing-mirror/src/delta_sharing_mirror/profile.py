"""Delta Sharing profile handling.

The profile JSON (endpoint + bearer token) is read from Secrets Manager and kept **in memory
only**. It is never written to disk, logged, or placed in an environment variable.
See docs/decisions/0003-profile-in-memory.md.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from delta_sharing.protocol import DeltaSharingProfile

from delta_sharing_mirror.errors import ConfigError

if TYPE_CHECKING:
    from mypy_boto3_secretsmanager import SecretsManagerClient


@dataclass(frozen=True)
class SharingProfile:
    profile: DeltaSharingProfile
    endpoint_host: str
    expiration_time: datetime | None

    def days_until_expiry(self, now: datetime | None = None) -> float | None:
        if self.expiration_time is None:
            return None
        now = now or datetime.now(UTC)
        return (self.expiration_time - now) / timedelta(days=1)

    def expiring_within(self, days: int, now: datetime | None = None) -> bool:
        remaining = self.days_until_expiry(now)
        return remaining is not None and remaining < days


def parse_expiration(value: str | None) -> datetime | None:
    """Parse the profile's ``expirationTime`` (ISO-8601; a trailing ``Z`` is common)."""
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def profile_from_json(payload: str | bytes | dict[str, Any]) -> SharingProfile:
    data: dict[str, Any] = json.loads(payload) if not isinstance(payload, dict) else payload
    required = ("shareCredentialsVersion", "endpoint")
    missing = [k for k in required if k not in data]
    if missing:
        raise ConfigError(f"sharing profile is missing keys: {', '.join(missing)}")
    try:
        profile = DeltaSharingProfile.from_json(data)
    except (KeyError, ValueError) as exc:  # never echo the payload: it holds the token
        raise ConfigError(f"sharing profile is not valid: {type(exc).__name__}") from None
    host = urlparse(profile.endpoint).hostname or "<unknown>"
    return SharingProfile(
        profile=profile,
        endpoint_host=host,
        expiration_time=parse_expiration(data.get("expirationTime")),
    )


def load_profile(secret_arn: str, client: SecretsManagerClient) -> SharingProfile:
    response = client.get_secret_value(SecretId=secret_arn)
    secret = response.get("SecretString")
    if secret is None:
        raise ConfigError("sharing profile secret must be a SecretString (JSON), not binary")
    return profile_from_json(secret)
