"""JSON logging to stdout, one line per event, with bearer-token redaction.

The delta-sharing client prints progress lines with ``print()``. ``capture_client_stdout``
routes those into the logger so stdout stays machine-parseable.
"""

from __future__ import annotations

import contextlib
import io
import logging
import re
import sys
from collections.abc import Iterator, MutableMapping
from typing import Any

import structlog

_REDACTIONS = (
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(r'(?i)("?bearerToken"?\s*[:=]\s*"?)[^"\s,}]+'),
    re.compile(r"(?i)(X-Amz-Signature=)[0-9a-f]+"),
    re.compile(r"(?i)(X-Amz-Credential=)[^&\s]+"),
    re.compile(r"(?i)(X-Amz-Security-Token=)[^&\s]+"),
)


def redact(text: str) -> str:
    """Remove bearer tokens and pre-signed URL credentials from free text."""
    for pattern in _REDACTIONS:
        text = pattern.sub(r"\1[REDACTED]", text)
    return text


def _redact_processor(
    _logger: logging.Logger, _name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    for key, value in list(event_dict.items()):
        if isinstance(value, str):
            event_dict[key] = redact(value)
    return event_dict


class _StdoutLogger:
    """Writes one line per event to the *real* stdout, even while client stdout is captured."""

    def msg(self, message: str) -> None:
        stream = _ORIGINAL_STDOUT or sys.stdout
        stream.write(message + "\n")
        stream.flush()

    debug = info = warning = warn = error = exception = critical = fatal = log = msg


class _StdoutLoggerFactory:
    def __call__(self, *_args: Any) -> _StdoutLogger:
        return _StdoutLogger()


_ORIGINAL_STDOUT: Any = None


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level.upper(), force=True)
    # Third-party libraries are noisy at INFO; keep them at WARNING unless debugging.
    for noisy in ("botocore", "boto3", "urllib3", "aiohttp", "fsspec"):
        logging.getLogger(noisy).setLevel(
            logging.DEBUG if level.upper() == "DEBUG" else logging.WARNING
        )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            _redact_processor,
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(sort_keys=True),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[level.upper()]
        ),
        logger_factory=_StdoutLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Lazy proxy: binds to the configuration active at first use, not at import time."""
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name, component=name)
    return logger


class _LoggerWriter(io.TextIOBase):
    def __init__(self, logger: structlog.stdlib.BoundLogger) -> None:
        super().__init__()
        self._logger = logger
        self._buf = ""

    def write(self, s: str) -> int:
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if line.strip():
                self._logger.debug("delta_sharing_client", message=line.strip())
        return len(s)

    def flush(self) -> None:
        if self._buf.strip():
            self._logger.debug("delta_sharing_client", message=self._buf.strip())
        self._buf = ""


@contextlib.contextmanager
def capture_client_stdout(logger: structlog.stdlib.BoundLogger) -> Iterator[None]:
    """Route anything the sharing client ``print()``s into the structured logger."""
    global _ORIGINAL_STDOUT  # noqa: PLW0603 - single-process CLI; see _StdoutLogger
    writer = _LoggerWriter(logger)
    previous = _ORIGINAL_STDOUT
    _ORIGINAL_STDOUT = sys.stdout
    try:
        with contextlib.redirect_stdout(writer):
            yield
    finally:
        writer.flush()
        _ORIGINAL_STDOUT = previous
