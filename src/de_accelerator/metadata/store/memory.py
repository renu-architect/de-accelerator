"""In-memory TableStore mirroring Delta semantics that matter to us (append-only enforcement, NOT NULL, atomic
conditional update). Thread-safe. Used by unit tests and as the reference implementation of the contract."""

from __future__ import annotations

import copy
import threading
from collections.abc import Mapping, Sequence
from typing import Any

from de_accelerator.core.errors import StoreError
from de_accelerator.metadata.schema.tablespec import TableSpec
from de_accelerator.metadata.store.base import Row


class InMemoryTableStore:
    def __init__(self, catalog: str | None = None, flavor: str = "LOCAL") -> None:
        self.catalog = catalog
        self.flavor = flavor
        self._specs: dict[str, TableSpec] = {}
        self._data: dict[str, list[Row]] = {}
        self._lock = threading.RLock()

    # --- DDL
    def create_table(self, spec: TableSpec) -> None:
        with self._lock:
            self._specs.setdefault(spec.name, spec)
            self._data.setdefault(spec.name, [])

    def drop_table(self, spec: TableSpec) -> None:
        with self._lock:
            self._specs.pop(spec.name, None)
            self._data.pop(spec.name, None)

    def table_exists(self, name: str) -> bool:
        return name in self._specs

    # --- helpers
    def _spec(self, table: str) -> TableSpec:
        try:
            return self._specs[table]
        except KeyError:
            raise StoreError(f"table does not exist: {table}") from None

    @staticmethod
    def _match(row: Row, where: Mapping[str, Any] | None) -> bool:
        return all(row.get(k) == v for k, v in (where or {}).items())

    def _normalise(self, spec: TableSpec, row: Mapping[str, Any]) -> Row:
        unknown = set(row) - set(spec.column_names)
        if unknown:
            raise StoreError(f"{spec.name}: unknown columns {sorted(unknown)}")
        out: Row = {c.name: copy.deepcopy(row.get(c.name)) for c in spec.columns}
        for c in spec.columns:
            if not c.nullable and out[c.name] is None:
                raise StoreError(f"{spec.name}.{c.name} is NOT NULL")
        return out

    # --- DML
    def insert(self, table: str, rows: Sequence[Row]) -> int:
        with self._lock:
            spec = self._spec(table)
            new = [self._normalise(spec, r) for r in rows]  # validate all before applying (atomic)
            self._data[table].extend(new)
            return len(new)

    def insert_if_absent(self, table: str, rows: Sequence[Row], key_cols: Sequence[str]) -> int:
        with self._lock:
            spec = self._spec(table)
            existing = {tuple(r[k] for k in key_cols) for r in self._data[table]}
            new: list[Row] = []
            for r in rows:
                n = self._normalise(spec, r)
                key = tuple(n[k] for k in key_cols)
                if None in key:
                    raise StoreError(f"{table}: NULL in merge key {key_cols}")
                if key not in existing:
                    existing.add(key)
                    new.append(n)
            self._data[table].extend(new)
            return len(new)

    def select(self, table: str, where: Mapping[str, Any] | None = None) -> list[Row]:
        with self._lock:
            self._spec(table)
            return [copy.deepcopy(r) for r in self._data[table] if self._match(r, where)]

    def update_where(self, table: str, values: Mapping[str, Any], where: Mapping[str, Any]) -> int:
        with self._lock:
            spec = self._spec(table)
            if spec.append_only:
                raise StoreError(f"{table} is append-only: UPDATE not permitted")
            bad = set(values) - set(spec.column_names)
            if bad:
                raise StoreError(f"{table}: unknown columns {sorted(bad)}")
            n = 0
            for r in self._data[table]:
                if self._match(r, where):
                    r.update(copy.deepcopy(dict(values)))
                    n += 1
            return n

    def delete_where(self, table: str, where: Mapping[str, Any]) -> int:
        with self._lock:
            spec = self._spec(table)
            if spec.append_only:
                raise StoreError(f"{table} is append-only: DELETE not permitted")
            keep = [r for r in self._data[table] if not self._match(r, where)]
            n = len(self._data[table]) - len(keep)
            self._data[table] = keep
            return n

    def count(self, table: str, where: Mapping[str, Any] | None = None) -> int:
        return len(self.select(table, where))
