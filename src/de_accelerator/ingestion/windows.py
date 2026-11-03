"""Pure window / watermark arithmetic shared by all connectors (doc 03 §2). No I/O."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from de_accelerator.core.errors import SourceConfigError

_DUR = re.compile(r"^P(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$")


def parse_duration(text: str | None) -> timedelta:
    """ISO-8601 duration subset (weeks/days/hours/minutes/seconds). Months/years are rejected (ambiguous length)."""
    if not text:
        return timedelta(0)
    m = _DUR.match(text)
    if not m or text in ("P", "PT"):
        raise SourceConfigError(f"unsupported ISO-8601 duration '{text}' (use e.g. PT2H, P7D)")
    w, d, h, mi, s = (int(x) if x else 0 for x in m.groups())
    return timedelta(weeks=w, days=d, hours=h, minutes=mi, seconds=s)


def ts_str(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")  # noqa: UP017


def parse_ts(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)  # noqa: UP017


@dataclass(frozen=True)
class Window:
    wm_from: str | None  # last committed value (None = first run)
    wm_to: str | None  # safe upper bound, frozen into the manifest
    read_from: str | None  # lower bound actually read = wm_from - lookback (timestamps)
    empty: bool


def incremental_window(wm_type: str, committed: str | None, source_max: str | None, source_now: datetime,
                       lookback: str | None, late_arrival: str | None) -> Window:  # fmt: skip
    """Compute the batch window for INCREMENTAL/API loads.

    timestamp: wm_to = min(source_max, source_now - late_arrival); read_from = committed - lookback.
    integer:   wm_to = source_max; lookback is not applicable (use late_arrival-style lag in the connector if needed).
    Empty when there is nothing newer than the committed cursor.
    """
    if source_max is None:
        return Window(committed, None, committed, True)
    if wm_type in ("timestamp", "datetime"):
        hi = parse_ts(source_max)
        if late_arrival:
            hi = min(hi, source_now - parse_duration(late_arrival))
        wm_to = ts_str(hi)
        if committed is not None and parse_ts(wm_to) <= parse_ts(committed):
            return Window(committed, wm_to, committed, True)
        read_from = ts_str(parse_ts(committed) - parse_duration(lookback)) if committed is not None else None
        return Window(committed, wm_to, read_from, False)
    if wm_type in ("integer", "int", "bigint"):
        if committed is not None and int(source_max) <= int(committed):
            return Window(committed, source_max, committed, True)
        return Window(committed, str(int(source_max)), committed, False)
    raise SourceConfigError(f"unsupported watermark_type '{wm_type}' (supported: timestamp, integer)")
