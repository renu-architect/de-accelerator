"""Row-level configuration change history derived from release diffs (cfg_config_history)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from de_accelerator.metadata.packs.ids import NAMESPACE, canonical_json
from de_accelerator.metadata.release.common import SYSTEM_COLUMNS, entity_key
from de_accelerator.metadata.schema.entities import ENTITIES
from de_accelerator.metadata.store.base import TableStore, now_utc


@dataclass(frozen=True)
class Change:
    kind: str
    table: str
    key: str
    change_type: str  # ADDED | MODIFIED | REMOVED
    before: dict[str, Any] | None
    after: dict[str, Any] | None


class ChangeDiffer:
    def __init__(self, store: TableStore) -> None:
        self.store = store

    def _rows(self, table: str, release_id: str | None) -> dict[str, dict[str, Any]]:
        if release_id is None:
            return {}
        return {r[table_id[table]]: r for r in self.store.select(table, {"pack_release_id": release_id})}

    def diff(self, release_from: str | None, release_to: str) -> list[Change]:
        changes: list[Change] = []
        for e in ENTITIES:
            a, b = self._rows(e.table, release_from), self._rows(e.table, release_to)
            for rid in sorted(set(a) | set(b)):
                ra, rb = a.get(rid), b.get(rid)
                if ra is None and rb is not None:
                    changes.append(Change(e.kind, e.table, entity_key(e.kind, rb), "ADDED", None, _biz(rb)))
                elif rb is None and ra is not None:
                    changes.append(Change(e.kind, e.table, entity_key(e.kind, ra), "REMOVED", _biz(ra), None))
                elif ra is not None and rb is not None and ra["row_hash"] != rb["row_hash"]:
                    changes.append(Change(e.kind, e.table, entity_key(e.kind, rb), "MODIFIED", _biz(ra), _biz(rb)))
        return changes

    def record(self, env: str, release_from: str | None, release_to: str, seq: int, actor: str | None,
               ticket: str | None) -> int:
        """Idempotently write history rows for the transition. Returns number of changes found."""
        changes = self.diff(release_from, release_to)
        ts = now_utc()
        rows = [{
            "history_id": str(uuid.uuid5(NAMESPACE, f"history|{release_to}|{seq}|{c.table}|{c.key}")),
            "env_id": env, "table_name": c.table, "entity_key": c.key, "change_type": c.change_type,
            "release_from": release_from, "release_to": release_to, "activation_seq": seq,
            "before_json": canonical_json(c.before) if c.before is not None else None,
            "after_json": canonical_json(c.after) if c.after is not None else None,
            "actor": actor, "ticket": ticket, "change_ts": ts,
        } for c in changes]  # fmt: skip
        self.store.insert_if_absent("cfg_config_history", rows, ["release_to", "activation_seq", "table_name", "entity_key"])
        return len(changes)


table_id = {e.table: e.id_column for e in ENTITIES}


def _biz(row: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in row.items() if k not in SYSTEM_COLUMNS}
