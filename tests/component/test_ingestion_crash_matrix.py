"""Crash-recovery matrix (doc 04 §8, Bronze scope): kill the engine at every protocol point, recover with a FRESH engine, assert invariants."""

import uuid
from collections import Counter
from datetime import timedelta

import pytest

from de_accelerator.audit.models import DONE
from de_accelerator.core.faults import FaultInjector, InjectedCrash
from tests.ingest_util import DDL, build_world, rows, ts

pytestmark = pytest.mark.component

POINTS = ["CP01_before_manifest", "CP02_after_manifest", "CP03_after_lease", "CP04_after_extract", "CP05_after_write",
          "CP06_before_receipt", "CP07_after_receipt", "CP08_after_committed_event", "CP09_after_watermark",
          "CP10_after_watermark_event"]  # fmt: skip
# source reads needed in total (crash-free = 1). Only a crash AFTER extract but BEFORE the Delta commit forces a re-read (I5).
EXPECTED_READS = {p: 1 for p in POINTS} | {"CP04_after_extract": 2}
LOADS = ["FULL", "INCREMENTAL", "SNAPSHOT"]


@pytest.fixture
def mk(spark, tmp_path):
    def make(load_type):
        w = build_world(tmp_path, spark, [{"code": "orders", "table": "orders", "load_type": load_type}], {"orders": (DDL, rows(9, 10, 11))})
        return w

    yield make
    spark.sql("DROP TABLE IF EXISTS bronze.orders")


def recover(w):
    w.clock.advance(minutes=31)  # the crashed runner's lease expires (a real crash cannot release it)
    w.new_engine()  # a brand-new engine/process: no in-memory state survives
    return w.ingest("orders")


def assert_invariants(w, load_type, expect_rows):
    df = w.bronze("orders")
    got = Counter((r["id"], r["_batch_seq"]) for r in df.collect())
    assert got == Counter({(i, 1): 1 for i in (1, 2, 3)}) and df.count() == expect_rows  # I2: no loss, no duplicates
    m = w.engine.manifests.list("orders", "live")
    assert [x.batch_seq for x in m] == [1]  # recovery resumed the SAME batch; it never planned a second one
    evs = w.engine.events.events(m[0].batch_id)
    assert [e["event_seq"] for e in evs] == sorted({e["event_seq"] for e in evs})  # I3 monotonic, unique
    assert [e["event_type"] for e in evs].count(DONE) == 1 and w.engine.events.state(m[0].batch_id) == DONE
    assert w.store.count("run_target_receipt") == 1
    wm = w.engine.watermarks.get("orders", "live")
    assert wm.committed_seq == 1 and wm.committed_batch_id == m[0].batch_id and wm.committed_value == m[0].wm_to  # I1


@pytest.mark.parametrize("load_type", LOADS)
@pytest.mark.parametrize("point", POINTS)
def test_crash_then_recover(mk, load_type, point):
    w = mk(load_type)
    w.new_engine(FaultInjector({point: 1}))
    with pytest.raises(InjectedCrash):
        w.ingest("orders")  # process "dies" here
    res = recover(w)
    assert res.status == "DONE" and res.batch_seq == 1 and res.rows_written == 3
    assert w.source.reads == EXPECTED_READS[point], f"source reads after recovery at {point}"  # I5
    assert_invariants(w, load_type, 3)

    # I4: running again changes nothing for incremental loads (full/snapshot legitimately start a new complete copy)
    if load_type == "INCREMENTAL":
        w.clock.advance(minutes=1)
        assert w.ingest("orders").status == "NO_DATA"
        assert w.engine.manifests.list("orders", "live")[-1].batch_seq == 1 and w.bronze("orders").count() == 3
        # a later batch still works and windows stay contiguous (I7)
        w.source.tables["orders"].rows.append({"id": 4, "amount": 40.0, "modified": ts(11, 40)})
        r2 = w.ingest("orders")
        assert r2.status == "DONE" and r2.batch_seq == 2
        ms = w.engine.manifests.list("orders", "live")
        assert ms[1].wm_from == ms[0].wm_to
    else:
        w.clock.advance(minutes=1)
        r2 = w.ingest("orders")
        assert r2.status == "DONE" and r2.batch_seq == 2 and w.bronze("orders").count() == 6


@pytest.mark.parametrize("load_type", LOADS)
def test_double_fault_crash_during_recovery(mk, load_type):
    w = mk(load_type)
    w.new_engine(FaultInjector({"CP02_after_manifest": 1}))
    with pytest.raises(InjectedCrash):
        w.ingest("orders")
    w.clock.advance(minutes=31)
    w.new_engine(FaultInjector({"CP06_before_receipt": 1}))  # recovery itself dies after the Delta commit
    with pytest.raises(InjectedCrash):
        w.ingest("orders")
    res = recover(w)
    assert res.status == "DONE" and w.source.reads == 1 and res.batch_seq == 1
    assert_invariants(w, load_type, 3)


def test_repeated_crashes_at_the_same_point_converge(mk):
    w = mk("INCREMENTAL")
    for _ in range(3):
        w.new_engine(FaultInjector({"CP08_after_committed_event": 1}))
        with pytest.raises(InjectedCrash):
            w.ingest("orders")
        w.clock.advance(minutes=31)
    res = recover(w)
    assert res.status == "DONE" and w.source.reads == 1
    assert_invariants(w, "INCREMENTAL", 3)


def test_watermark_never_ahead_of_receipt_when_crash_precedes_commit(mk):
    """I1 at the critical point: crash after the Bronze commit but before the receipt/cursor => cursor must still be unset."""
    w = mk("INCREMENTAL")
    w.new_engine(FaultInjector({"CP06_before_receipt": 1}))
    with pytest.raises(InjectedCrash):
        w.ingest("orders")
    assert w.bronze("orders").count() == 3  # the data IS committed in Delta ...
    assert w.engine.watermarks.get("orders", "live") is None  # ... but the cursor has not moved
    assert w.engine.receipts.get(w.engine.manifests.list("orders", "live")[0].batch_id) is None
    res = recover(w)  # recovery finishes the commit protocol WITHOUT re-reading the source
    assert res.status == "DONE" and w.source.reads == 1 and w.bronze("orders").count() == 3


def test_crash_between_commit_and_watermark_does_not_reread_source(mk):
    w = mk("INCREMENTAL")
    w.new_engine(FaultInjector({"CP08_after_committed_event": 1}))  # receipt written, cursor not yet advanced
    with pytest.raises(InjectedCrash):
        w.ingest("orders")
    assert w.engine.watermarks.get("orders", "live") is None
    reads_before = w.source.reads
    recover(w)
    assert w.source.reads == reads_before  # the outstanding work was only the watermark commit
    assert w.engine.watermarks.get("orders", "live").committed_seq == 1


def test_two_engines_racing_after_crash_do_not_duplicate(mk):
    w = mk("INCREMENTAL")
    w.new_engine(FaultInjector({"CP05_after_write": 1}))
    with pytest.raises(InjectedCrash):
        w.ingest("orders")
    w.clock.advance(minutes=31)
    a = w.new_engine()
    b = w.new_engine()
    ref, reader = w.snapshot()
    r1 = a.ingest_object(reader, ref, "ingest", "orders", str(uuid.uuid4()))
    r2 = b.ingest_object(reader, ref, "ingest", "orders", str(uuid.uuid4()))
    assert r1.status == "DONE" and r2.status in ("NO_DATA", "DONE")  # the loser finds nothing new / the finished batch
    assert w.bronze("orders").count() == 3
    assert timedelta(0) == timedelta(0)
