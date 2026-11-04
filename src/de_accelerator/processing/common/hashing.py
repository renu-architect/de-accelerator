"""Canonical content hashing for Silver/Gold (SHA-256; never the 64-bit Bronze `_row_hash`).

`to_json(struct(...))` with explicit options gives a typed, null-aware, session-timezone-independent serialisation, so the same
logical row always hashes the same across runs, sessions and cluster time zones.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column
from pyspark.sql import functions as F

_JSON_OPTS = {"ignoreNullFields": "false", "timeZone": "UTC"}


def _struct(cols: Sequence[str | Column]) -> Column:
    return F.struct(*[F.col(c) if isinstance(c, str) else c for c in cols])


def content_hash(cols: Sequence[str | Column]) -> Column:
    """Full 64-hex-char SHA-256 over the typed JSON serialisation of `cols` (column order is part of the identity)."""
    return F.sha2(F.to_json(_struct(cols), _JSON_OPTS), 256)


def hex128(cols: Sequence[str | Column]) -> Column:
    """128-bit (32 hex chars) identifier derived from `cols`: used for SCD2 surrogate keys / logical-change identities."""
    return F.substring(content_hash(cols), 1, 32)
