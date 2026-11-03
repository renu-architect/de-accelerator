"""Test-only fault injection for metadata-scope crash tests (previews the M2 harness in doc 08 §7)."""

from __future__ import annotations

from typing import Any


class InjectedCrash(BaseException):
    """Emulates process death: derives from BaseException so no `except Exception` cleanup/FAILED-marking runs."""


class FaultyStore:
    """Wraps a TableStore; raises InjectedCrash *before* the k-th write operation (1-based). k=None => count only."""

    WRITES = ("insert", "insert_if_absent", "update_where", "delete_where")

    def __init__(self, inner: Any, crash_at: int | None = None) -> None:
        self._inner = inner
        self.crash_at = crash_at
        self.writes = 0

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if name not in self.WRITES:
            return attr

        def wrapped(*a: Any, **kw: Any) -> Any:
            self.writes += 1
            if self.crash_at is not None and self.writes == self.crash_at:
                raise InjectedCrash(f"crash before write #{self.writes} ({name} {a[0] if a else ''})")
            return attr(*a, **kw)

        return wrapped
