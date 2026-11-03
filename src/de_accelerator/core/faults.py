"""Test-only crash injection (doc 04 §8). The injector is passed explicitly to the engine; it cannot be enabled from metadata
or environment variables. `InjectedCrash` derives from BaseException so no `except Exception` cleanup or FAILED-marking runs,
which emulates process death."""

from __future__ import annotations


class InjectedCrash(BaseException):
    pass


class FaultInjector:
    """crash_at: {point_name: remaining_hits_before_crash}. `hit(point)` raises on the configured occurrence."""

    def __init__(self, crash_at: dict[str, int] | None = None, repeat: bool = False) -> None:
        self._crash_at = dict(crash_at or {})
        self._repeat = repeat
        self.hits: dict[str, int] = {}

    def hit(self, point: str) -> None:
        self.hits[point] = self.hits.get(point, 0) + 1
        if point in self._crash_at and self.hits[point] == self._crash_at[point]:
            if not self._repeat:
                del self._crash_at[point]
            raise InjectedCrash(f"injected crash at {point} (hit {self.hits[point]})")


class NoFaults:
    def hit(self, point: str) -> None:
        return None
