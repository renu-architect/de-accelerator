"""Regression tests for closure findings C1-C3 that need no Spark: a store that behaves like Delta (naive timestamps),
and the Delta-conflict retry logic."""

from datetime import timedelta, timezone

import pytest

from de_accelerator.audit.models import batch_id_for
from de_accelerator.audit.repos import EventLog, LeaseRepo
from de_accelerator.core.errors import StoreConflictError
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.store.base import now_utc
from de_accelerator.metadata.store.memory import InMemoryTableStore
from de_accelerator.metadata.store.spark import SparkDeltaTableStore, is_delta_conflict


class NaiveTimestampStore(InMemoryTableStore):
    """Returns timestamps WITHOUT tzinfo, as pyspark does when collecting TIMESTAMP columns (the cause of C1/C2)."""

    def select(self, table, where=None):
        rows = super().select(table, where)
        for r in rows:
            for k, v in r.items():
                if hasattr(v, "tzinfo") and v.tzinfo is not None:
                    r[k] = v.astimezone(timezone.utc).replace(tzinfo=None)  # noqa: UP017
        return rows


@pytest.fixture
def naive_store():
    s = NaiveTimestampStore()
    Migrator(s, MIGRATIONS).apply()
    return s


def test_event_append_writes_exactly_one_row_per_call_even_with_naive_timestamps(naive_store):
    """C1: before the fix this wrote 50 junk rows and then raised RuntimeError."""
    ev, b = EventLog(naive_store), batch_id_for("o", "live", 1)
    assert [ev.append(b, "PLANNED", run_id="r1"), ev.append(b, "EXTRACTING", run_id="r1"), ev.append(b, "DONE", run_id="r1")] == [1, 2, 3]
    assert naive_store.count("run_batch_event") == 3 and ev.state(b) == "DONE"


def test_event_rows_carry_a_unique_nonce_but_keep_payload_fields(naive_store):
    ev, b = EventLog(naive_store), batch_id_for("o", "live", 1)
    ev.append(b, "FAILED_FATAL", run_id="r", error_class="DATA", message="boom")
    import json

    p = json.loads(naive_store.select("run_batch_event")[0]["payload"])
    assert p["error_class"] == "DATA" and p["message"] == "boom" and len(p["_nonce"]) == 32


def test_lease_busy_and_takeover_with_naive_timestamps(naive_store):
    """C2: before the fix this raised TypeError (naive vs aware) whenever another owner held or had held the lease."""
    ls, t0 = LeaseRepo(naive_store), now_utc()
    assert ls.acquire("o", "live", "runA", t0, timedelta(minutes=10)) == 1
    assert ls.acquire("o", "live", "runB", t0 + timedelta(minutes=1), timedelta(minutes=10)) is None  # live holder => busy
    assert ls.acquire("o", "live", "runB", t0 + timedelta(minutes=30), timedelta(minutes=10)) == 2  # expired => takeover, epoch+1
    ls.release("o", "live", "runB", t0 + timedelta(minutes=31))
    assert ls.acquire("o", "live", "runA", t0 + timedelta(minutes=32), timedelta(minutes=10)) == 3


# ---- C3: Delta commit-conflict classification and bounded retry -----------------------------------------------------------
class ConcurrentAppendException(Exception):  # noqa: N818 - mirrors the JVM exception name that surfaces in pyspark messages
    pass


@pytest.mark.parametrize("exc,expected", [
    (ConcurrentAppendException("x"), True),
    (RuntimeError("[DELTA_CONCURRENT_APPEND.WITHOUT_HINT] Transaction conflict"), True),
    (RuntimeError("io.delta.exceptions.MetadataChangedException: The metadata of the Delta table has been changed"), True),
    (ValueError("schema mismatch"), False), (PermissionError("denied"), False),
])
def test_conflict_detection(exc, expected):
    assert is_delta_conflict(exc) is expected


def make_store(retries=3):
    sleeps: list[float] = []
    return SparkDeltaTableStore(None, max_conflict_retries=retries, sleep=sleeps.append), sleeps  # type: ignore[arg-type]


def test_retry_succeeds_after_transient_conflicts_with_backoff():
    store, sleeps = make_store()
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConcurrentAppendException("conflict")
        return "ok"

    assert store._retry(flaky, "w") == "ok" and len(calls) == 3 and len(sleeps) == 2
    assert all(0 < s <= 2.0 for s in sleeps) and sleeps[1] > 0


def test_retry_gives_up_with_a_transient_error_class():
    store, sleeps = make_store(retries=2)
    with pytest.raises(StoreConflictError) as e:
        store._retry(lambda: (_ for _ in ()).throw(ConcurrentAppendException("again")), "insert run_batch_event")
    assert e.value.error_class == "TRANSIENT" and "insert run_batch_event" in str(e.value) and len(sleeps) == 2


def test_non_conflict_errors_are_not_retried():
    store, sleeps = make_store()
    calls = []

    def bad():
        calls.append(1)
        raise ValueError("schema mismatch")

    with pytest.raises(ValueError):
        store._retry(bad, "w")
    assert len(calls) == 1 and sleeps == []
