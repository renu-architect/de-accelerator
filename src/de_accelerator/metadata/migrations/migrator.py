"""Ordered, idempotent, checksummed metadata-schema migrations (doc 09 §3, ADR-0008).

* `up` must be idempotent (CREATE ... IF NOT EXISTS): a crash between `up` and the bookkeeping insert is healed by re-run.
* Checksum = sha256 of the rendered DDL of the migration; drift between code and an applied migration is an error.
* Rollback runs `down` newest-first; going below version 1 destroys all metadata tables and needs `allow_destroy=True`.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from de_accelerator.core.errors import MigrationError
from de_accelerator.core.logging import get_logger
from de_accelerator.core.versioning import METADATA_SCHEMA_VERSION, framework_version
from de_accelerator.metadata.schema.tablespec import TableSpec, render_create, render_drop
from de_accelerator.metadata.store.base import TableStore, now_utc

log = get_logger("migrations")
MIGRATION_TABLE = "cfg_schema_migration"


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    specs: Callable[[], list[TableSpec]]

    def statements(self, catalog: str | None = None, flavor: str = "DATABRICKS") -> list[str]:
        return [render_create(s, catalog, flavor) for s in self.specs()]

    def down_statements(self, catalog: str | None = None, flavor: str = "DATABRICKS") -> list[str]:
        return [render_drop(s, catalog, flavor) for s in reversed(self.specs())]

    def checksum(self) -> str:
        # checksum is flavor-independent: always the canonical (DATABRICKS, no catalog) rendering
        return hashlib.sha256("\n;\n".join(self.statements(None, "DATABRICKS")).encode()).hexdigest()

    def up(self, store: TableStore) -> None:
        for s in self.specs():
            store.create_table(s)

    def down(self, store: TableStore) -> None:
        for s in reversed(self.specs()):
            store.drop_table(s)


class Migrator:
    def __init__(self, store: TableStore, migrations: list[Migration], actor: str = "system") -> None:
        versions = [m.version for m in migrations]
        if versions != sorted(set(versions)) or (versions and versions[0] != 1) or versions != list(range(1, len(versions) + 1)):
            raise MigrationError("migrations must be numbered contiguously from 1", versions=versions)
        self.store = store
        self.migrations = migrations
        self.actor = actor

    # --- state
    def applied(self) -> dict[int, dict[str, Any]]:
        if not self.store.table_exists(MIGRATION_TABLE):
            return {}
        return {int(r["version"]): r for r in self.store.select(MIGRATION_TABLE) if r["status"] == "APPLIED"}

    def current_version(self) -> int:
        a = self.applied()
        return max(a) if a else 0

    def verify_checksums(self) -> None:
        by_ver = {m.version: m for m in self.migrations}
        for v, rec in self.applied().items():
            m = by_ver.get(v)
            if m is None:
                raise MigrationError(f"database has migration {v} unknown to this framework version", version=v)
            if rec["checksum"] != m.checksum():
                raise MigrationError(f"checksum drift for migration {v} ({m.name}): code changed after apply", version=v)

    # --- plan / apply
    def plan(self, target: int | None = None) -> list[Migration]:
        target = target if target is not None else self.migrations[-1].version
        if target > self.migrations[-1].version:
            raise MigrationError(f"target {target} exceeds known migrations")
        self.verify_checksums()
        cur = self.current_version()
        return [m for m in self.migrations if cur < m.version <= target]

    def apply(self, target: int | None = None, dry_run: bool = False, catalog: str | None = None) -> list[str]:
        """Apply pending migrations. Returns the SQL statements executed (or that would be, in dry-run)."""
        todo = self.plan(target)
        stmts: list[str] = []
        for m in todo:
            stmts += m.statements(catalog, self.store.flavor)
            if dry_run:
                continue
            log.info("applying migration %s %s", m.version, m.name)
            m.up(self.store)
            self.store.insert_if_absent(
                MIGRATION_TABLE,
                [{"version": m.version, "name": m.name, "checksum": m.checksum(), "applied_ts": now_utc(),
                  "applied_by": self.actor, "framework_version": framework_version(), "status": "APPLIED"}],
                ["version"],
            )  # fmt: skip
        if todo and not dry_run:
            self._register_framework_version()
        return stmts

    def rollback(self, to_version: int, allow_destroy: bool = False) -> None:
        cur = self.current_version()
        if to_version >= cur:
            return
        if to_version < 1 and not allow_destroy:
            raise MigrationError("rolling back to version 0 destroys all metadata tables; pass allow_destroy=True")
        self.verify_checksums()
        by_ver = {m.version: m for m in self.migrations}
        for v in range(cur, to_version, -1):
            m = by_ver[v]
            log.info("rolling back migration %s %s", m.version, m.name)
            if v == 1:
                m.down(self.store)  # drops the bookkeeping table too
            else:
                m.down(self.store)
                self.store.update_where(MIGRATION_TABLE, {"status": "ROLLED_BACK"}, {"version": v})

    def _register_framework_version(self) -> None:
        self.store.insert_if_absent(
            "cfg_framework_version",
            [{"framework_version": framework_version(), "metadata_schema_version": METADATA_SCHEMA_VERSION,
              "min_compatible_schema": 1, "status": "ACTIVE", "release_date": None, "wheel_ref": None,
              "registered_ts": now_utc()}],
            ["framework_version"],
        )  # fmt: skip
