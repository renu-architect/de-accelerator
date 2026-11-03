"""Minimal plugin registry (interface only in M1; used by later modules)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Generic, TypeVar

T = TypeVar("T")


class Registry(Generic[T]):
    def __init__(self, kind: str) -> None:
        self.kind = kind
        self._items: dict[str, T] = {}

    def register(self, key: str, item: T) -> None:
        if key in self._items:
            raise KeyError(f"{self.kind} '{key}' already registered")
        self._items[key] = item

    def decorator(self, key: str) -> Callable[[T], T]:
        def wrap(item: T) -> T:
            self.register(key, item)
            return item

        return wrap

    def get(self, key: str) -> T:
        try:
            return self._items[key]
        except KeyError:
            raise KeyError(f"unknown {self.kind}: '{key}' (registered: {sorted(self._items)})") from None

    def keys(self) -> list[str]:
        return sorted(self._items)
