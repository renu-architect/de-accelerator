"""Injectable clock (UTC) so watermark/lookback/lease logic is deterministic in tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)  # noqa: UP017


class ManualClock:
    def __init__(self, start: datetime) -> None:
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, **kw: float) -> None:
        self._now += timedelta(**kw)
