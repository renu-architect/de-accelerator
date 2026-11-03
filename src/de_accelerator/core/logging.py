"""Structured logging with secret redaction."""

from __future__ import annotations

import logging
import re

_PATTERNS = [
    re.compile(r"(?i)(password|pwd|secret|token|accountkey|sharedaccesskey|api[_-]?key)\s*[=:]\s*[^\s,;]+"),
    re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}"),  # JWT-like
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
]


_SECRET_VALUES: set[str] = set()


def register_secret(value: str) -> None:
    """Make an exact resolved secret value redacted everywhere `redact` is applied (logs, error messages)."""
    if value and len(value) >= 4:
        _SECRET_VALUES.add(value)


def redact(text: str) -> str:
    for secret in _SECRET_VALUES:
        text = text.replace(secret, "***")
    for pat in _PATTERNS:
        text = pat.sub(lambda m: (m.group(1) + "=***") if m.lastindex else "***", text)
    return text


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage())
        record.args = ()
        return True


_WIRE_LOGGERS = ("py4j", "py4j.clientserver", "py4j.java_gateway")


def quiet_wire_loggers() -> None:
    """py4j logs every JVM call (including JDBC credentials passed as Spark options) at DEBUG. Never let that reach a handler."""
    for name in _WIRE_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"de_accelerator.{name}")
    if not any(isinstance(f, RedactingFilter) for f in logger.filters):
        logger.addFilter(RedactingFilter())
    return logger
