"""A5: the engine, recovery, parallel runner and CLI against the DELTA-backed audit store (what production uses).

Closure review found that every earlier engine test used the in-memory audit store, which hid defects C1-C4. This module runs the
same behaviours with `SparkDeltaTableStore` for ALL metadata/audit tables and real local Delta for Bronze."""

import os
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import pytest

from de_accelerator.audit.models import DONE
from de_accelerator.cli.main import run
from de_accelerator.core.errors import DataError
from de_accelerator.core.faults import FaultInjector, InjectedCrash
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.schema.tables import all_tables
from de_accelerator.metadata.store.spark import SparkDeltaTableStore
from tests.ingest_util import DDL, build_world, rows, ts

pytestmark = pytest.mark.component

FULL = ["t_full", "t_lease", "cli_a", "cli_b"]
INCR = ["t_incr", "c1", "c2", "c3", "p1", "p2", "p3", "p4"]
CODES = FULL + INCR


@pytest.fixture(scope="module")
def delta_audit(spark):
    s = SparkDeltaTableStore(spark, flavor="LOCAL")
    for t in reversed(all_tables()):
        s.drop_table(t)
    Migrator(s, MIGRATIONS).apply()
    yield s
    for t in reversed(all_tables()):
        s.drop_table(t)


@pytest.fixture(scope="module")
def dw(spark, delta_audit, tmp_path_factory):
    objs = [{"code": c, "table": c, "load_type": "FULL"} for c in FULL] + [{"code": c, "table": c, "load_type": "INCREMENTAL"} for c in INCR]
    w = build_world(tmp_path_factory.mktemp("dw"), spark, objs, {c: (DDL, rows(9, 10, 11)) for c in CODES}, store=delta_audit)
    yield w
    for c in CODES:
        spark.sql(f"DROP TABLE IF EXISTS bronze.{c}")


def event_types(w, batch_id):
    return [e["event_type"] for e in w.engine.events.events(batch_id)]


# ---- A1: event log on Delta ------------------------------------------------------------------------------------------------
def test_full_load_with_delta_audit_writes_exactly_the_protocol_events(dw):
    res = dw.ingest("t_full")
    assert res.status == "DONE" and res.rows_written == 3 and dw.bronze("t_full").count() == 3
    assert event_types(dw, res.batch_id) == ["PLANNED", "EXTRACTING", "BRONZE_WRITTEN", "BRONZE_COMMITTED", "SRC_WM_COMMITTED", "DONE"]
    assert [e["event_seq"] for e in dw.engine.events.events(res.batch_id)] == [1, 2, 3, 4, 5, 6]
    assert dw.store.count("run_batch_event", {"batch_id": res.batch_id}) == 6  # was 50 junk rows + RuntimeError before the fix
    rc = dw.engine.receipts.get(res.batch_id)
    assert rc["rows_written"] == 3 and rc["txn_app_id"] == "t_full:live:BRONZE" and rc["delta_version"] is not None
    assert dw.engine.watermarks.get("t_full", "live").committed_seq == 1


def test_incremental_cycle_with_delta_audit(dw):
    assert dw.ingest("t_incr").rows_written == 3
    assert dw.ingest("t_incr").status == "NO_DATA" and dw.store.count("run_batch_manifest", {"object_code": "t_incr"}) == 1
    dw.source.tables["t_incr"].rows.append({"id": 4, "amount": 40.0, "modified": ts(11, 30)})
    r2 = dw.ingest("t_incr")
    assert r2.status == "DONE" and r2.rows_written == 2 and dw.bronze("t_incr").count() == 5
    ms = dw.engine.manifests.list("t_incr", "live")
    assert [m.batch_seq for m in ms] == [1, 2] and ms[1].wm_from == ms[0].wm_to
    assert dw.engine.watermarks.get("t_incr", "live").committed_value == ts(11, 30)


# ---- A2: lease on Delta ----------------------------------------------------------------------------------------------------
def test_lease_busy_and_takeover_with_delta_audit(dw):
    dw.source.fail_extract = [DataError("bad source")]
    assert dw.ingest("t_lease").status == "FAILED"
    dw.engine.leases.acquire("t_lease", "live", "other-runner", dw.clock.now(), timedelta(minutes=30))
    assert dw.ingest("t_lease", restart=True).status == "BUSY"  # TypeError (naive vs aware) before the fix
    dw.clock.advance(minutes=31)
    res = dw.ingest("t_lease", restart=True)
    assert res.status == "DONE" and res.batch_seq == 1 and dw.bronze("t_lease").count() == 3


def test_delta_timestamps_are_utc_aware_whatever_the_process_timezone(delta_audit):
    """Root cause of C1/C2: pyspark returns naive LOCAL datetimes. The store now rebuilds aware UTC from epoch microseconds."""
    stamp = datetime(2026, 7, 1, 23, 30, 15, 123456, tzinfo=timezone.utc)
    old = os.environ.get("TZ")
    try:
        for tz in ("Asia/Kolkata", "America/Los_Angeles", "UTC"):
            os.environ["TZ"] = tz
            time.tzset()
            delta_audit.insert("run_batch_lease", [{"object_code": f"tz-{tz}", "stream_key": "live", "owner_run_id": "r", "epoch": 1,
                                                    "lease_expiry": stamp}])  # fmt: skip
            got = delta_audit.select("run_batch_lease", {"object_code": f"tz-{tz}"})[0]["lease_expiry"]
            assert got.tzinfo is not None and got == stamp, f"{tz}: {got!r}"
    finally:
        if old is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old
        time.tzset()


# ---- acceptance criterion 3: crash recovery on Delta-backed audit -------------------------------------------------------------
@pytest.mark.parametrize("point,code,expected_reads", [
    ("CP04_after_extract", "c1", 2),  # crashed after reading, before any Bronze commit: the frozen window is re-extracted
    ("CP06_before_receipt", "c2", 1),  # Delta commit done, receipt missing: finished WITHOUT re-reading the source
    ("CP08_after_committed_event", "c3", 1),  # only the watermark commit was outstanding
])
def test_crash_then_recover_with_delta_audit(dw, point, code, expected_reads):
    reads0 = dw.source.reads
    dw.new_engine(FaultInjector({point: 1}))
    with pytest.raises(InjectedCrash):
        dw.ingest(code)
    dw.clock.advance(minutes=31)  # the crashed runner's lease expires
    dw.new_engine()  # a brand-new engine: nothing in memory survives
    res = dw.ingest(code)
    assert res.status == "DONE" and res.batch_seq == 1 and res.rows_written == 3
    assert dw.source.reads - reads0 == expected_reads
    df = dw.bronze(code)
    assert df.count() == 3 and {r["_batch_seq"] for r in df.collect()} == {1}  # no loss, no duplicates
    ms = dw.engine.manifests.list(code, "live")
    assert [m.batch_seq for m in ms] == [1]  # the SAME batch was resumed
    evs = event_types(dw, ms[0].batch_id)
    assert evs.count(DONE) == 1 and dw.engine.events.state(ms[0].batch_id) == DONE
    assert dw.store.count("run_target_receipt", {"batch_id": ms[0].batch_id}) == 1
    wm = dw.engine.watermarks.get(code, "live")
    assert wm.committed_seq == 1 and wm.committed_batch_id == ms[0].batch_id and wm.committed_value == ms[0].wm_to
    assert dw.ingest(code).status == "NO_DATA"  # idempotent: running again changes nothing


# ---- acceptance criterion 4: four-way parallel ingestion on Delta-backed audit --------------------------------------------------
def test_four_way_parallel_ingestion_with_delta_audit(dw):
    retries: list[float] = []
    dw.store._sleep = retries.append  # count Delta commit-conflict retries (and skip the waiting)
    objs = ("p1", "p2", "p3", "p4")
    for round_no in (1, 2):
        if round_no == 2:
            for c in objs:
                dw.source.tables[c].rows.append({"id": 100, "amount": 1.0, "modified": ts(11, 45)})
            dw.clock.advance(minutes=5)
        s = dw.runner.run(objects=objs, concurrency=4)
        assert s.status == "SUCCEEDED", [(r.object_code, r.status, r.error_class, r.message) for r in s.results]
        assert {r.object_code: r.status for r in s.results} == dict.fromkeys(objs, "DONE")
    for c in objs:
        ms = dw.engine.manifests.list(c, "live")
        assert [m.batch_seq for m in ms] == [1, 2] and ms[1].wm_from == ms[0].wm_to  # contiguous windows, no skipped/duplicate batch
        assert dw.bronze(c).count() == 3 + 2  # 3 initial + (new row + 1 lookback overlap row)
        for m in ms:
            assert event_types(dw, m.batch_id).count(DONE) == 1 and dw.store.count("run_target_receipt", {"batch_id": m.batch_id}) == 1
        assert dw.engine.watermarks.get(c, "live").committed_seq == 2
    assert dw.store.count("run_pipeline") >= 2 and dw.store.count("run_error", {"object_code": "p1"}) == 0
    print(f"\n[parallel on Delta] commit-conflict retries absorbed by the store: {len(retries)}")


# ---- acceptance criterion 5: CLI ingestion + batch-status on Delta-backed audit --------------------------------------------------
def test_cli_ingest_and_batch_status_with_delta_audit(dw, tmp_path):
    (tmp_path / "base.yaml").write_text("client_code: acme\nenvironment: DEV\nflavor: LOCAL\nwarehouse_dir: /unused\n")
    lines: list[str] = []

    def call(*argv, expect=0):
        lines.clear()
        rc = run(["--bootstrap", str(tmp_path / "base.yaml"), *argv], lambda cfg: dw.store, out=lines.append, engine_factory=lambda cfg, st: dw.engine)
        assert rc == expect, "\n".join(lines)
        return "\n".join(lines)

    out = call("ingest", "--objects", "cli_a,cli_b", "--concurrency", "2")
    assert "SUCCEEDED" in out and out.count("DONE") == 2 and dw.bronze("cli_a").count() == 3 and dw.bronze("cli_b").count() == 3
    status = call("batch-status", "--object", "cli_a")
    assert "cli_a/live" in status and "DONE" in status and "rows=3" in status


# ---- whole-store integrity at the end -------------------------------------------------------------------------------------------
def test_no_duplicate_event_sequence_numbers_anywhere(dw):
    evs = dw.store.select("run_batch_event")
    dupes = [k for k, n in Counter((e["batch_id"], e["event_seq"]) for e in evs).items() if n > 1]
    assert dupes == []
    by_batch: dict[str, list[int]] = {}
    for e in evs:
        by_batch.setdefault(e["batch_id"], []).append(e["event_seq"])
    assert all(sorted(v) == list(range(1, len(v) + 1)) for v in by_batch.values())  # gap-free numbering per batch
