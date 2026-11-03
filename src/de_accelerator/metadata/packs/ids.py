"""Deterministic identifiers and canonical JSON/hashing (stable across reloads and machines)."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Sequence
from typing import Any

NAMESPACE = uuid.UUID("6f1c2a52-7c1e-4a56-9a35-2b8f5d9a1e01")  # fixed forever: changing it re-keys every entity


def entity_id(client_code: str, kind: str, project_code: str | None, key_values: Sequence[Any]) -> str:
    path = "|".join([client_code, kind, project_code or "", *map(str, key_values)])
    return str(uuid.uuid5(NAMESPACE, path))


def canonical_json(obj: Any) -> str:
    """Sorted keys, no whitespace, UTF-8 safe, no NaN. Same logical content => same bytes."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False, default=_default)


def _default(o: Any) -> Any:
    if isinstance(o, tuple | set | frozenset):
        return sorted(o) if isinstance(o, set | frozenset) else list(o)
    raise TypeError(f"not canonically serialisable: {type(o).__name__}")


def sha256_hex(data: str | bytes) -> str:
    return hashlib.sha256(data.encode("utf-8") if isinstance(data, str) else data).hexdigest()
