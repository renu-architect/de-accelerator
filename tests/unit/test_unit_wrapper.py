"""M3's temporary copy of M2's retry/failure semantics (M4-CONSOLIDATE-01). These tests pin the behaviours M2 closure established."""

from datetime import datetime, timezone

import pytest

from de_accelerator.audit.models import FAILED_FATAL, FAILED_RETRYABLE, PLANNED, Manifest, batch_id_for
from de_accelerator.audit.repos import EventLog, ManifestRepo
from de_accelerator.core.clock import ManualClock
from de_accelerator.core.errors import BatchBusyError, DataError, TransientSourceError
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.store.memory import InMemoryTableStore
from de_accelerator.processing.common.unit import RetryPolicy, UnitAudit, UnitResult, run_with_retry

STREAM = "SILVER:live"


@pytest.fixture
def env():
    s = InMemoryTableStore()
    Migrator(s, MIGRATIONS).apply()
    clock = ManualClock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    sleeps: list[float] = []
    audit = UnitAudit(s, clock)
    m = Manifest(batch_id_for("o", STREAM, 1), "o", STREAM, 1, "SILVER", "snap", "rel", "1", "1", None, [], {}, "run")
    ManifestRepo(s).create(m)
    EventLog(s).append(m.batch_id, PLANNED, run_id="run")
    return s, clock, sleeps, audit, m


RETRY = RetryPolicy(3, 1.0, 2.0)


def go(env, fn, retry=RETRY):
    s, clock, sleeps, audit, m = env
    return run_with_retry("o", "SILVER", STREAM, "run", retry, clock, sleeps.append, audit, fn)


def test_success_is_recorded_with_its_real_layer(env):
    res = go(env, lambda n: UnitResult("o", "SILVER", "DONE", rows_in=5, rows_applied=5))
    assert res.status == "DONE" and res.attempts == 1
    row = env[0].select("run_object")[0]
    assert row["layer"] == "SILVER" and row["rows_read"] == 5 and row["status"] == "DONE"


def test_transient_errors_retry_with_backoff_then_succeed(env):
    calls = []

    def fn(n):
        calls.append(n)
        if n < 3:
            raise TransientSourceError("flaky")
        return UnitResult("o", "SILVER", "DONE")

    res = go(env, fn)
    assert res.status == "DONE" and calls == [1, 2, 3] and env[2] == [1.0, 2.0]
    assert len(env[0].select("run_attempt")) == 3


def test_exhausted_transient_error_stays_resumable_not_fatal(env):
    res = go(env, lambda n: (_ for _ in ()).throw(TransientSourceError("down")))
    s, _, _, _, m = env
    assert res.status == "FAILED" and res.error_class == "TRANSIENT" and res.attempts == 3
    assert EventLog(s).state(m.batch_id) == FAILED_RETRYABLE


def test_fatal_error_is_not_retried(env):
    calls = []

    def fn(n):
        calls.append(n)
        raise DataError("bad data")

    res = go(env, fn)
    s, _, _, _, m = env
    assert res.status == "FAILED" and calls == [1] and EventLog(s).state(m.batch_id) == FAILED_FATAL


def test_unexpected_exception_is_contained_as_logic_error(env):
    res = go(env, lambda n: (_ for _ in ()).throw(ValueError("boom")))
    assert res.status == "FAILED" and res.error_class == "LOGIC" and "boom" in (res.message or "")


def test_busy_lease_returns_busy(env):
    res = go(env, lambda n: (_ for _ in ()).throw(BatchBusyError("held")))
    assert res.status == "BUSY"


def test_audit_write_failure_never_changes_the_outcome(env, monkeypatch):
    s, clock, sleeps, audit, m = env
    monkeypatch.setattr(s, "insert", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("audit store down")))
    res = go(env, lambda n: UnitResult("o", "SILVER", "DONE"))
    assert res.status == "DONE"  # run_object insert failed but was swallowed


def test_failure_releases_the_lease(env):
    s, clock, sleeps, audit, m = env
    assert audit.leases.acquire("o", STREAM, "run", clock.now(), __import__("datetime").timedelta(minutes=30)) == 1
    go(env, lambda n: (_ for _ in ()).throw(DataError("x")))
    assert audit.leases.acquire("o", STREAM, "other-run", clock.now(), __import__("datetime").timedelta(minutes=30)) is not None
