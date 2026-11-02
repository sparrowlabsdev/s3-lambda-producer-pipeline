"""CloudWatch custom metrics. Metric failures are logged, never fatal."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from delta_sharing_mirror.log import get_logger

if TYPE_CHECKING:
    from mypy_boto3_cloudwatch import CloudWatchClient

log = get_logger(__name__)


class Metrics:
    def __init__(self, client: CloudWatchClient | None, namespace: str) -> None:
        self._client = client
        self._namespace = namespace
        self._buffer: list[dict[str, Any]] = []

    def put(
        self, name: str, value: float, *, unit: str = "Count", table: str | None = None
    ) -> None:
        datum: dict[str, Any] = {
            "MetricName": name,
            "Value": float(value),
            "Unit": unit,
            "Timestamp": datetime.now(UTC),
        }
        if table:
            datum["Dimensions"] = [{"Name": "Table", "Value": table}]
        self._buffer.append(datum)
        log.info("metric", metric=name, value=value, unit=unit, table=table)
        if len(self._buffer) >= 20:  # PutMetricData accepts up to 1000; keep requests small
            self.flush()

    def flush(self) -> None:
        if not self._buffer or self._client is None:
            self._buffer.clear()
            return
        try:
            self._client.put_metric_data(Namespace=self._namespace, MetricData=self._buffer)  # type: ignore[arg-type]
        except Exception as exc:  # metrics must never fail the mirror
            log.warning("metrics_put_failed", error=str(exc)[:300])
        finally:
            self._buffer.clear()
