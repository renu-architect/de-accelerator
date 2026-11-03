import threading
from datetime import timedelta

import pytest

from de_accelerator.audit.models import DONE, FAILED_RETRYABLE, PLANNED, Manifest, Receipt, batch_id_for
from de_accelerator.audit.repos import EventLog, LeaseRepo, ManifestRepo, ReceiptRepo, WatermarkRepo, open_batch
from de_accelerator.core.errors import WatermarkConflictError
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.store.base import now_utc
from de_accelerator.metadata.store.memory import InMemoryTableStore


@pytest.fixture
def store():
    s = InMemoryTableStore()
    Migrator(s, MIGRATIONS).apply()
    return s


def man(seq=1, stream="live", obj="orders", run="r1", wm_to="10"):
    return Manifest(batch_id_for(obj, stream, seq), obj, stream, seq, "LIVE", "snap", "rel", "0", wm_to, None,
                    [{"layer": "BRONZE", "table": "bronze.t"}], {"query_sha256": "x"}, run)


def test_batch_id_is_deterministic():
    assert batch_id_for("o", "live", 1) == batch_id_for("o", "live", 1) != batch_id_for("o", "live", 2)


def test_manifest_insert_once_and_next_seq(store):
    repo = ManifestRepo(store)
    assert repo.next_seq("orders", "live") == 1
    m, created = repo.create(man())
    assert created and m.manifest_hash == man().manifest_hash and repo.next_seq("orders", "live") == 2
    again, created2 = repo.create(man())  # same runner replans -> same manifest, no duplicate row
    assert created2 and store.count("run_batch_manifest") == 1
    other, mine = repo.create(man(run="r2"))  # a competing planner: the first manifest wins, caller learns it is not the creator
    assert not mine and other.planned_by_run_id == "r1" and store.count("run_batch_manifest") == 1
    assert repo.get(m.batch_id).plan_json == {"query_sha256": "x"}  # type: ignore[union-attr]
    assert repo.next_seq("orders", "backfill-1") == 1  # streams are independent


def test_manifest_hash_covers_window_and_plan():
    assert man(wm_to="10").manifest_hash != man(wm_to="11").manifest_hash
    assert man().manifest_hash == man(run="other-run").manifest_hash  # planner identity is not part of the content hash


def test_event_log_sequence_and_state_fold(store):
    ev = EventLog(store)
    b = batch_id_for("orders", "live", 1)
    assert ev.state(b) == "UNKNOWN"
    for t in ("PLANNED", "EXTRACTING", "BRONZE_WRITTEN"):
        ev.append(b, t, run_id="r1")
    assert [e["event_seq"] for e in ev.events(b)] == [1, 2, 3] and ev.state(b) == "BRONZE_WRITTEN"
    ev.append(b, FAILED_RETRYABLE, run_id="r1", error="boom")
    assert ev.state(b) == FAILED_RETRYABLE  # failure after progress is visible
    ev.append(b, "EXTRACTING", run_id="r2")  # recovery resumes
    assert ev.state(b) == "BRONZE_WRITTEN"
    ev.append(b, DONE, run_id="r2")
    assert ev.state(b) == DONE and ev.events(b)[-1]["event_seq"] == 6


def test_event_seq_unique_under_concurrency(store):
    ev, b = EventLog(store), batch_id_for("o", "live", 1)
    ts = [threading.Thread(target=lambda i=i: ev.append(b, PLANNED, run_id=f"r{i}")) for i in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sorted(e["event_seq"] for e in ev.events(b)) == list(range(1, 9))


def test_open_batch_resolution(store):
    mr, ev = ManifestRepo(store), EventLog(store)
    assert open_batch(mr, ev, "orders", "live") is None
    m1, _ = mr.create(man(1))
    ev.append(m1.batch_id, PLANNED)
    assert open_batch(mr, ev, "orders", "live").batch_id == m1.batch_id  # type: ignore[union-attr]
    ev.append(m1.batch_id, DONE)
    assert open_batch(mr, ev, "orders", "live") is None
    m2, _ = mr.create(man(2))  # manifest without any event (crash right after insert) is still open
    assert open_batch(mr, ev, "orders", "live").batch_id == m2.batch_id  # type: ignore[union-attr]


def test_receipts_insert_once(store):
    rr = ReceiptRepo(store)
    b = batch_id_for("o", "live", 1)
    assert rr.get(b) is None
    first = rr.insert(Receipt(b, "BRONZE", "bronze.t", 3, "tag", 100, rows_read=100, txn_app_id="a", txn_version=1))
    second = rr.insert(Receipt(b, "BRONZE", "bronze.t", 9, "other", 999))
    assert first["delta_version"] == second["delta_version"] == 3 and store.count("run_target_receipt") == 1


def test_watermark_commit_protocol(store):
    wm = WatermarkRepo(store)
    assert wm.get("o", "live") is None
    assert wm.commit("o", "live", "10", 1, "b1") is True
    assert wm.commit("o", "live", "10", 1, "b1") is False  # idempotent replay of the same commit
    assert wm.commit("o", "live", "20", 2, "b2") is True
    with pytest.raises(WatermarkConflictError):
        wm.commit("o", "live", "15", 2, "other-batch")  # same seq, different batch
    with pytest.raises(WatermarkConflictError):
        wm.commit("o", "live", "5", 1, "b-old")  # never moves backwards
    cur = wm.get("o", "live")
    assert (cur.committed_value, cur.committed_seq, cur.committed_batch_id) == ("20", 2, "b2")  # type: ignore[union-attr]
    assert wm.get("o", "backfill-1") is None  # independent per stream


def test_watermark_concurrent_commits_single_winner(store):
    wm = WatermarkRepo(store)
    wm.commit("o", "live", "1", 1, "b1")
    results, barrier = [], threading.Barrier(4)

    def go(i):
        barrier.wait()
        try:
            results.append(wm.commit("o", "live", str(i), 2, f"b{i}"))
        except WatermarkConflictError:
            results.append("conflict")

    ts = [threading.Thread(target=go, args=(i,)) for i in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert results.count(True) == 1 and results.count("conflict") == 3
    assert wm.get("o", "live").committed_seq == 2  # type: ignore[union-attr]


def test_rewind_is_explicit_and_audited_by_caller(store):
    wm = WatermarkRepo(store)
    wm.commit("o", "live", "20", 2, "b2")
    wm.rewind("o", "live", "10", 1, "b1")
    assert wm.get("o", "live").committed_value == "10"  # type: ignore[union-attr]


def test_lease_busy_takeover_and_renewal(store):
    ls, t0 = LeaseRepo(store), now_utc()
    assert ls.acquire("o", "live", "runA", t0, timedelta(minutes=10)) == 1
    assert ls.acquire("o", "live", "runB", t0 + timedelta(minutes=1), timedelta(minutes=10)) is None  # live holder
    assert ls.acquire("o", "live", "runA", t0 + timedelta(minutes=2), timedelta(minutes=10)) == 1  # renewal keeps epoch
    assert ls.acquire("o", "live", "runB", t0 + timedelta(minutes=30), timedelta(minutes=10)) == 2  # expired -> takeover, epoch+1
    assert ls.acquire("o", "live", "runA", t0 + timedelta(minutes=31), timedelta(minutes=10)) is None
    ls.release("o", "live", "runB", t0 + timedelta(minutes=32))
    assert ls.acquire("o", "live", "runA", t0 + timedelta(minutes=33), timedelta(minutes=10)) == 3
