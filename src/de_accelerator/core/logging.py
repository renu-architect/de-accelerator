"""Structured logging with secret redaction."""

from __future__ import annotations

import logging
import re

_PATTERNS = [
    re.compile(r"(?i)(password|pwd|secret|token|accountkey|sharedaccesskey|api[_-]?key)\s*[=:]\s*[^\s,;]+"),
    re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}"),  # JWT-like
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
]


def redact(text: str) -> str:
    for pat in _PATTERNS:
        text = pat.sub(lambda m: (m.group(1) + "=***") if m.lastindex else "***", text)
    return text


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage())
        record.args = ()
        return True


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"de_accelerator.{name}")
    if not any(isinstance(f, RedactingFilter) for f in logger.filters):
        logger.addFilter(RedactingFilter())
    return logger
