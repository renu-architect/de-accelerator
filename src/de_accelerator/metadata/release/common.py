"""Shared release constants and row conversion helpers."""

from __future__ import annotations

import json
import uuid
from typing import Any

from de_accelerator.metadata.packs.ids import NAMESPACE, canonical_json
from de_accelerator.metadata.schema.entities import BY_KIND

# release statuses (cfg_pack_release.status). The active-release pointer is the source of truth; status is derived/healed.
STAGING, STAGED, ACTIVE, SUPERSEDED, ROLLED_BACK, FAILED = "STAGING", "STAGED", "ACTIVE", "SUPERSEDED", "ROLLED_BACK", "FAILED"
ACTIVATABLE = {STAGED, SUPERSEDED, ROLLED_BACK}
SYSTEM_COLUMNS = {"pack_release_id", "row_version", "created_ts"}


def release_id_for(env: str, bundle_sha256: str) -> str:
    return str(uuid.uuid5(NAMESPACE, f"release|{env}|{bundle_sha256}"))


def to_table_row(kind: str, row: dict[str, Any], release_id: str, row_version: int, created_ts: Any) -> dict[str, Any]:
    ent = BY_KIND[kind]
    out = dict(row)
    for f in ent.fields:
        v = out.get(f.name)
        if f.type == "json":
            out[f.name] = canonical_json(v or {})
        elif f.type == "float" and v is not None:
            out[f.name] = float(v)  # YAML ints (e.g. 0) must be doubles for a typed Delta column
        elif f.type == "int" and v is not None:
            out[f.name] = int(v)
    out["pack_release_id"] = release_id
    out["row_version"] = row_version
    out["created_ts"] = created_ts
    return out


def from_table_row(kind: str, row: dict[str, Any]) -> dict[str, Any]:
    """Inverse of to_table_row for business fields (json text -> objects, arrays -> lists)."""
    ent = BY_KIND[kind]
    out = {k: v for k, v in row.items() if k not in SYSTEM_COLUMNS}
    for f in ent.fields:
        v = out.get(f.name)
        if f.type == "json":
            out[f.name] = json.loads(v) if isinstance(v, str) and v else {}
        elif f.type == "list":
            out[f.name] = list(v) if v is not None else []
    return out


def entity_key(kind: str, row: dict[str, Any]) -> str:
    ent = BY_KIND[kind]
    prefix = f"{row.get('project_code')}/" if ent.project_scoped else ""
    return prefix + "/".join(str(row.get(k)) for k in ent.key)
