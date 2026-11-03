"""TableStore protocol: the only persistence API used by migrator, loader, release manager, snapshot store, readers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Protocol

from de_accelerator.metadata.schema.tablespec import TableSpec

Row = dict[str, Any]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)  # noqa: UP017


class TableStore(Protocol):
    flavor: str
    catalog: str | None

    def create_table(self, spec: TableSpec) -> None: ...

    def drop_table(self, spec: TableSpec) -> None: ...

    def table_exists(self, name: str) -> bool: ...

    def insert(self, table: str, rows: Sequence[Row]) -> int: ...

    def insert_if_absent(self, table: str, rows: Sequence[Row], key_cols: Sequence[str]) -> int:
        """Insert only rows whose key is not present. Returns number inserted. Safe under concurrency."""
        ...

    def select(self, table: str, where: Mapping[str, Any] | None = None) -> list[Row]: ...

    def update_where(self, table: str, values: Mapping[str, Any], where: Mapping[str, Any]) -> int:
        """Atomic conditional update; returns affected row count (0 => precondition failed)."""
        ...

    def count(self, table: str, where: Mapping[str, Any] | None = None) -> int: ...
