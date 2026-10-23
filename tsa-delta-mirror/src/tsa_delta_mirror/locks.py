"""Per-table run lock in DynamoDB: conditional put with a TTL, released at the end of the run.

This guards against two tasks mirroring the same table at once (an EventBridge retry, a manual
run during a scheduled one). delta-rs commits are atomic regardless; the lock avoids wasted
work and conflicting-commit errors. See docs/decisions/0002-s3-commit-atomicity.md.
"""

from __future__ import annotations

import time
from types import TracebackType
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from tsa_delta_mirror.errors import LockHeldError
from tsa_delta_mirror.log import get_logger

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient

log = get_logger(__name__)


class RunLock:
    def __init__(
        self,
        client: DynamoDBClient | None,
        table_name: str | None,
        key: str,
        owner: str,
        ttl_seconds: int,
    ) -> None:
        self._client = client
        self._table = table_name
        self._key = key
        self._owner = owner
        self._ttl = ttl_seconds
        self._held = False

    @property
    def enabled(self) -> bool:
        return self._client is not None and bool(self._table)

    def acquire(self) -> None:
        if not self.enabled:
            log.debug("run_lock_disabled", key=self._key)
            return
        assert self._client is not None and self._table is not None  # noqa: S101 - narrowed above
        now = int(time.time())
        try:
            self._client.put_item(
                TableName=self._table,
                Item={
                    "table_key": {"S": self._key},
                    "owner": {"S": self._owner},
                    "acquired_at": {"N": str(now)},
                    "expires_at": {"N": str(now + self._ttl)},
                },
                ConditionExpression="attribute_not_exists(table_key) OR expires_at < :now",
                ExpressionAttributeValues={":now": {"N": str(now)}},
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                raise LockHeldError(f"run lock for {self._key} is held by another run") from None
            raise
        self._held = True
        log.info("run_lock_acquired", key=self._key, ttl_seconds=self._ttl)

    def release(self) -> None:
        if not self._held or self._client is None or self._table is None:
            return
        try:
            self._client.delete_item(
                TableName=self._table,
                Key={"table_key": {"S": self._key}},
                ConditionExpression="#o = :owner",
                ExpressionAttributeNames={"#o": "owner"},
                ExpressionAttributeValues={":owner": {"S": self._owner}},
            )
        except ClientError as exc:
            # Lost the lock (TTL expired and someone else took it). Nothing to clean up.
            log.warning("run_lock_release_failed", key=self._key, error=str(exc))
        finally:
            self._held = False
            log.info("run_lock_released", key=self._key)

    def __enter__(self) -> RunLock:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()
