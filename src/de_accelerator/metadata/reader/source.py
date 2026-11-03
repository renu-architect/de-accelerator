"""ConfigSource: read-only access to one resolved configuration (an active/explicit release, a snapshot, or in-memory data).

All readers/builders consume this protocol so engines never care where config comes from. Rows are python-typed
(json -> dict, arrays -> list) and contain only business columns + ids (no release bookkeeping).
"""

from __future__ import annotations

import copy
from typing import Any, Protocol

from de_accelerator.core.errors import NotFoundError
from de_accelerator.metadata.release.common import from_table_row
from de_accelerator.metadata.release.manager import ReleaseManager
from de_accelerator.metadata.schema.entities import BY_KIND
from de_accelerator.metadata.store.base import TableStore

Row = dict[str, Any]


def business_sort_key(kind: str, row: Row) -> tuple[str, ...]:
    """Canonical row order for every ConfigSource: by (project, business key). Stable across releases/environments."""
    return tuple(str(row.get(k)) for k in ("project_code", *BY_KIND[kind].key))


class ConfigSource(Protocol):
    release_id: str
    env: str

    def rows(self, kind: str) -> list[Row]: ...


class StoreConfigSource:
    """Reads the rows of ONE release from the Delta/in-memory store. Defaults to the env's active release."""

    def __init__(self, store: TableStore, env: str, release_id: str | None = None) -> None:
        self.store, self.env = store, env
        rm = ReleaseManager(store, env)
        rid = release_id or rm.active_release_id()
        if rid is None:
            raise NotFoundError(f"no active release for env {env}")
        self.release_id = rid
        self.release = rm.get_release(rid)
        self._cache: dict[str, list[Row]] = {}

    def rows(self, kind: str) -> list[Row]:
        if kind not in self._cache:
            ent = BY_KIND[kind]
            raw = self.store.select(ent.table, {"pack_release_id": self.release_id})
            self._cache[kind] = sorted((from_table_row(kind, r) for r in raw), key=lambda r: business_sort_key(kind, r))
        return copy.deepcopy(self._cache[kind])


class DictConfigSource:
    """In-memory source (tests, snapshots)."""

    def __init__(self, release_id: str, env: str, entities: dict[str, list[Row]]) -> None:
        self.release_id, self.env = release_id, env
        self._entities = entities

    def rows(self, kind: str) -> list[Row]:
        return copy.deepcopy(self._entities.get(kind, []))


class SnapshotConfigSource:
    """Config source backed by an immutable Snapshot (what engines use at runtime; needs no cfg_* access)."""

    def __init__(self, snapshot: Any) -> None:
        self.snapshot = snapshot
        self.release_id: str = snapshot.release_id
        self.env: str = snapshot.env
        self.snapshot_id: str = snapshot.snapshot_id

    def rows(self, kind: str) -> list[Row]:
        return copy.deepcopy(self.snapshot.content["entities"].get(kind, []))
