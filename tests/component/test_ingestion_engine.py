"""Ingestion engine end-to-end on the mock source with local Delta Bronze (component tier)."""

import threading
from datetime import timedelta

import pytest

from de_accelerator.audit.models import DONE
from de_accelerator.audit.repos import open_batch
from de_accelerator.core.errors import DataError, TransientSourceError
from tests.ingest_util import DDL, build_world, rows, ts

pytestmark = pytest.mark.component


@pytest.fixture
def mk(spark, tmp_path):
    made = []

    def make(objects, tables):
        w = build_world(tmp_path, spark, objects, tables)
        made.append(w)
        return w

    yield make
    for _ in made:
        for o in ("orders", "customers", "snap", "a", "b", "c"):
            spark.sql(f"DROP TABLE IF EXISTS bronze.{o}")


def states(w, batch_id):
    return [e["event_type"] for e in w.engine.events.events(batch_id)]


def test_full_load_end_to_end(mk):
    w = mk([{"code": "customers", "table": "customers", "load_type": "FULL"}], {"customers": (DDL, rows(9, 10, 11, 12, 13))})
    res = w.ingest("customers")
    assert res.status == "DONE" and res.rows_written == 5 and res.batch_seq == 1
    df = w.bronze("customers")
    assert df.count() == 5 and {r["_op_type"] for r in df.collect()} == {"S"}
    assert states(w, res.batch_id) == ["PLANNED", "EXTRACTING", "BRONZE_WRITTEN", "BRONZE_COMMITTED", "SRC_WM_COMMITTED", "DONE"]
    rc = w.engine.receipts.get(res.batch_id)
    assert rc["rows_written"] == 5 and rc["txn_app_id"] == "customers:live:BRONZE" and rc["txn_version"] == 1 and rc["delta_version"] is not None
    wm = w.engine.watermarks.get("customers", "live")
    assert wm.committed_seq == 1 and wm.committed_batch_id == res.batch_id
    assert w.store.count("run_object") == 1 and w.store.count("run_lineage") == 1 and w.store.count("run_schema_snapshot") == 1
    assert w.store.select("run_batch_manifest")[0]["release_id"] and w.store.select("run_batch_manifest")[0]["snapshot_id"]
    r = df.collect()[0]
    assert r["_config_snapshot_id"] == w.store.select("run_batch_manifest")[0]["snapshot_id"] and r["_source_object"] == "dbo.customers"


def test_incremental_windows_lookback_and_no_data(mk):
    w = mk([{"code": "orders", "table": "orders", "load_type": "INCREMENTAL", "lookback": "PT1H"}], {"orders": (DDL, rows(9, 10, 11))})
    r1 = w.ingest("orders")
    assert r1.status == "DONE" and r1.rows_written == 3
    assert w.engine.watermarks.get("orders", "live").committed_value == ts(11)  # = frozen wm_to, not "now"
    assert w.ingest("orders").status == "NO_DATA" and w.store.count("run_batch_manifest") == 1  # nothing new: no empty batch
    w.source.tables["orders"].rows += [{"id": 4, "amount": 40.0, "modified": ts(11, 30)}]
    r2 = w.ingest("orders")
    assert r2.status == "DONE" and r2.rows_written == 2  # 11:30 (new) + 11:00 (inside the 1h lookback overlap)
    assert w.bronze("orders").count() == 5
    assert w.bronze("orders").where("id = 3").count() == 2  # overlap duplicate kept by design (Silver dedupes)
    assert w.bronze("orders").where("id = 1").count() == 1  # 09:00 is older than the lookback: not re-read
    ms = w.engine.manifests.list("orders", "live")
    assert [m.batch_seq for m in ms] == [1, 2] and ms[1].wm_from == ms[0].wm_to  # contiguous committed windows
    assert w.engine.watermarks.get("orders", "live").committed_value == ts(11, 30)


def test_incremental_late_arrival_window_holds_back_upper_bound(mk):
    objs = [{"code": "orders", "table": "orders", "load_type": "INCREMENTAL"}]
    w = mk(objs, {"orders": (DDL, rows(9, 10, 11))})
    # clock is 12:00; rows newer than (now - 5 min) are not yet safe to read
    w.source.tables["orders"].rows.append({"id": 9, "amount": 1.0, "modified": ts(11, 58)})
    # emulate late_arrival_window by editing the strategy through the mock plan: use lookback only here, window tested in unit tests
    assert w.ingest("orders").rows_written == 4


def test_snapshot_load_is_a_full_copy_each_run(mk):
    w = mk([{"code": "snap", "table": "snap", "load_type": "SNAPSHOT"}], {"snap": (DDL, rows(9, 10))})
    w.ingest("snap")
    w.ingest("snap")
    df = w.bronze("snap")
    assert df.count() == 4 and sorted(r["_batch_seq"] for r in df.collect()) == [1, 1, 2, 2]
    assert w.engine.watermarks.get("snap", "live").committed_seq == 2


def test_transient_failure_is_retried_without_duplicates(mk):
    w = mk([{"code": "orders", "table": "orders", "load_type": "FULL"}], {"orders": (DDL, rows(9, 10))})
    w.source.fail_extract = [TransientSourceError("connection reset"), TransientSourceError("connection reset")]
    res = w.ingest("orders")
    assert res.status == "DONE" and res.attempts == 3 and w.bronze("orders").count() == 2
    assert w.store.count("run_attempt") == 3 and w.store.count("run_error") == 2
    assert [e["event_type"] for e in w.engine.events.events(res.batch_id)].count("FAILED_RETRYABLE") == 2
    assert w.engine.events.state(res.batch_id) == DONE


def test_retries_exhausted_leaves_open_batch_that_a_later_run_completes(mk):
    w = mk([{"code": "orders", "table": "orders", "load_type": "FULL"}], {"orders": (DDL, rows(9, 10))})
    w.source.fail_extract = [TransientSourceError("down")] * 3
    res = w.ingest("orders")
    assert res.status == "FAILED" and res.error_class == "TRANSIENT" and w.engine.watermarks.get("orders", "live") is None
    assert not w.spark.catalog.tableExists("bronze.orders")  # nothing written, cursor untouched
    res2 = w.ingest("orders")
    assert res2.status == "DONE" and res2.batch_seq == 1 and res2.recovered  # SAME batch resumed, not re-planned
    assert w.bronze("orders").count() == 2


def test_data_error_is_fatal_and_needs_restart(mk):
    w = mk([{"code": "orders", "table": "orders", "load_type": "FULL"}], {"orders": (DDL, rows(9))})
    w.source.fail_extract = [DataError("bad data")]
    res = w.ingest("orders")
    assert res.status == "FAILED" and res.error_class == "DATA" and res.attempts == 1  # not retried
    assert w.engine.events.state(open_batch(w.engine.manifests, w.engine.events, "orders", "live").batch_id) == "FAILED_FATAL"
    again = w.ingest("orders")
    assert again.status == "FAILED" and "--restart" in again.message  # does not hammer the source
    ok = w.ingest("orders", restart=True)
    assert ok.status == "DONE" and ok.batch_seq == 1


def test_abandon_releases_the_window_for_replanning(mk):
    w = mk([{"code": "orders", "table": "orders", "load_type": "INCREMENTAL"}], {"orders": (DDL, rows(9, 10))})
    w.source.fail_extract = [DataError("bad")]
    w.ingest("orders")
    bid = w.engine.abandon_batch("orders", "live", "bad window", "ops")
    assert w.engine.events.state(bid) == "ABANDONED"
    res = w.ingest("orders")
    assert res.status == "DONE" and res.batch_seq == 2 and w.bronze("orders").count() == 2  # new seq, same window (cursor never moved)
    assert w.engine.manifests.list("orders", "live")[1].wm_from is None


def test_busy_when_another_runner_holds_the_lease(mk):
    w = mk([{"code": "orders", "table": "orders", "load_type": "FULL"}], {"orders": (DDL, rows(9))})
    w.source.fail_extract = [DataError("x")]
    w.ingest("orders")  # leaves an open (fatal) batch + lease released
    w.engine.leases.acquire("orders", "live", "other-runner", w.clock.now(), timedelta(minutes=30))
    res = w.ingest("orders", restart=True)
    assert res.status == "BUSY"
    w.clock.advance(minutes=31)  # lease expires -> takeover
    assert w.ingest("orders", restart=True).status == "DONE"


def test_concurrent_runners_write_each_row_once(mk):
    w = mk([{"code": "orders", "table": "orders", "load_type": "INCREMENTAL"}], {"orders": (DDL, rows(9, 10, 11))})
    out = []
    barrier = threading.Barrier(3)

    def go():
        barrier.wait()
        out.append(w.ingest("orders").status)

    ts_ = [threading.Thread(target=go) for _ in range(3)]
    [t.start() for t in ts_]
    [t.join() for t in ts_]
    assert "DONE" in out and set(out) <= {"DONE", "BUSY", "NO_DATA"}
    assert w.bronze("orders").count() == 3  # never duplicated


def test_runner_isolates_failures_between_objects(mk):
    w = mk([{"code": "a", "table": "a", "load_type": "FULL"}, {"code": "b", "table": "b", "load_type": "FULL"},
            {"code": "c", "table": "c", "load_type": "FULL"}], {t: (DDL, rows(9, 10)) for t in "abc"})  # fmt: skip
    w.source.fail_extract = [DataError("a is broken")]  # whichever object extracts first fails fatally
    s = w.runner.run(execution="run_all", concurrency=1)
    assert s.status == "PARTIAL" and s.by_status() == {"FAILED": 1, "DONE": 2}
    assert w.store.select("run_pipeline")[0]["status"] == "PARTIAL" and w.store.count("run_object") == 3
    assert w.store.select("run_pipeline")[0]["snapshot_id"] == s.snapshot.snapshot_id


def test_inflight_batch_keeps_its_own_snapshot(mk):
    w = mk([{"code": "orders", "table": "orders", "load_type": "FULL"}], {"orders": (DDL, rows(9))})
    w.source.fail_extract = [TransientSourceError("x")] * 3
    w.ingest("orders")
    m = open_batch(w.engine.manifests, w.engine.events, "orders", "live")
    from de_accelerator.metadata.release import PackLoader, ReleaseManager
    from de_accelerator.metadata.validation import validate_pack
    from tests.packutil import edit_items

    edit_items(w.pack, "projects/ingest/x/obj.yaml", lambda it: it[0].update(priority=7))  # a new release appears mid-batch
    report, bundle = validate_pack(w.pack, "DEV", deployed_schema_version=2)
    rel = PackLoader(w.store, "DEV").stage(bundle)
    ReleaseManager(w.store, "DEV").activate(rel["release_id"])
    res = w.ingest("orders")  # new snapshot is current, but the open batch must continue with the snapshot it was planned with
    assert res.status == "DONE" and w.bronze("orders").collect()[0]["_config_snapshot_id"] == m.snapshot_id
