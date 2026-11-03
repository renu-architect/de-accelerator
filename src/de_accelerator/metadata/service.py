"""MetadataService: UI-ready, read-only façade (ADR-0016). It never writes Delta; proposing a change means opening a Git PR
(outside M1 scope, optional enhancement). A future UI would be a client of exactly these methods.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.packs.bundle import PackBundle
from de_accelerator.metadata.reader import DeltaMetadataReader, MetadataReader
from de_accelerator.metadata.release.history import Change, ChangeDiffer
from de_accelerator.metadata.release.manager import ReleaseManager
from de_accelerator.metadata.store.base import TableStore
from de_accelerator.metadata.validation import ValidationReport, validate_pack


class MetadataService:
    def __init__(self, store: TableStore, env: str) -> None:
        self.store, self.env = store, env
        self._releases = ReleaseManager(store, env)

    # validation (no side effects)
    def validate(self, pack_dir: str | Path, env: str | None = None, options: dict[str, Any] | None = None
                 ) -> tuple[ValidationReport, PackBundle | None]:  # fmt: skip
        deployed = Migrator(self.store, MIGRATIONS).current_version() or None
        return validate_pack(pack_dir, env or self.env, deployed_schema_version=deployed, options=options)

    # release inspection
    def list_releases(self) -> list[dict[str, Any]]:
        return self._releases.list_releases()

    def release_status(self) -> dict[str, Any]:
        p = self._releases.pointer()
        return {"env": self.env, "active_release_id": p["release_id"] if p else None,
                "activation_seq": p["activation_seq"] if p else 0,
                "previous_release_id": p["previous_release_id"] if p else None}  # fmt: skip

    def diff_releases(self, release_from: str | None, release_to: str) -> list[Change]:
        return ChangeDiffer(self.store).diff(release_from, release_to)

    def history(self, entity_key: str | None = None) -> list[dict[str, Any]]:
        where = {"env_id": self.env} | ({"entity_key": entity_key} if entity_key else {})
        return sorted(self.store.select("cfg_config_history", where), key=lambda h: (h["change_ts"], h["entity_key"]))

    # queries
    def reader(self, release_id: str | None = None) -> MetadataReader:
        return DeltaMetadataReader(self.store, self.env, release_id)
