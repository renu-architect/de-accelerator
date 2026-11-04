"""READ-ONLY PROBE: each audit repository against the REAL Delta store (what production uses). Tables created individually."""
import sys, tempfile, traceback
from datetime import timedelta
sys.path.insert(0, "/home/user/de-accelerator")
from de_accelerator.audit.models import Manifest, Receipt, batch_id_for
from de_accelerator.audit.repos import EventLog, LeaseRepo, ManifestRepo, ReceiptRepo, RunLog, WatermarkRepo
from de_accelerator.metadata.schema.tables import table_map
from de_accelerator.metadata.store.base import now_utc
from de_accelerator.metadata.store.spark import SparkDeltaTableStore, build_local_spark

spark = build_local_spark(tempfile.mkdtemp())
store = SparkDeltaTableStore(spark, flavor="LOCAL")
tm = table_map()
for t in ("run_batch_manifest", "run_batch_event", "run_target_receipt", "wm_watermark", "run_batch_lease", "run_pipeline", "run_object",
          "run_attempt", "run_error", "run_lineage", "run_schema_snapshot"):
    store.create_table(tm[t])
b = batch_id_for("o", "live", 1)


def attempt(name, fn):
    try:
        r = fn()
        print(f"PASS  {name}: {r!r}"[:200])
    except BaseException as e:  # noqa: BLE001
        print(f"FAIL  {name}: {type(e).__name__}: {str(e).splitlines()[0][:170]}")


attempt("ManifestRepo.create+get", lambda: ManifestRepo(store).create(Manifest(b, "o", "live", 1, "LIVE", "snap", "rel", None, "x", None, [{"layer": "BRONZE"}], {"q": 1}, "r1"))[1])
attempt("ManifestRepo.next_seq", lambda: ManifestRepo(store).next_seq("o", "live"))
ev = EventLog(store)
attempt("EventLog.append (1st event)", lambda: ev.append(b, "PLANNED", run_id="r1"))
attempt("EventLog rows written by the failed attempt(s)", lambda: len(store.select("run_batch_event", {"batch_id": b})))
attempt("EventLog.state", lambda: ev.state(b))
row = store.select("run_batch_event", {"batch_id": b})[0] if store.count("run_batch_event") else None
print("      event_ts type read back from Delta:", type(row["event_ts"]).__name__, "tzinfo =", getattr(row["event_ts"], "tzinfo", "n/a")) if row else None
attempt("aware == naive datetime comparison", lambda: now_utc() == now_utc().replace(tzinfo=None))
attempt("ReceiptRepo.insert+get", lambda: ReceiptRepo(store).insert(Receipt(b, "BRONZE", "bronze.t", 1, "tag", 5, rows_read=5, txn_app_id="a", txn_version=1))["rows_written"])
wm = WatermarkRepo(store)
attempt("WatermarkRepo.commit #1 (insert)", lambda: wm.commit("o", "live", "10", 1, b))
attempt("WatermarkRepo.commit #1 replay", lambda: wm.commit("o", "live", "10", 1, b))
attempt("WatermarkRepo.commit #2 (conditional update)", lambda: wm.commit("o", "live", "20", 2, "b2"))
ls = LeaseRepo(store)
attempt("LeaseRepo.acquire #1", lambda: ls.acquire("o", "live", "runA", now_utc(), timedelta(minutes=30)))
attempt("LeaseRepo.acquire renew (same owner)", lambda: ls.acquire("o", "live", "runA", now_utc(), timedelta(minutes=30)))
attempt("LeaseRepo.acquire other owner (should be None=busy)", lambda: ls.acquire("o", "live", "runB", now_utc(), timedelta(minutes=30)))
attempt("LeaseRepo.acquire other owner after expiry", lambda: ls.acquire("o", "live", "runB", now_utc() + timedelta(hours=1), timedelta(minutes=30)))
attempt("LeaseRepo.release", lambda: ls.release("o", "live", "runB", now_utc()))
rl = RunLog(store)
attempt("RunLog.start_pipeline/end", lambda: (rl.start_pipeline("r1", "e", "DEV", "t", "0.1", "rel", "snap"), rl.end_pipeline("r1", "OK"))[1])
attempt("RunLog.object_result+attempt+error+lineage+schema", lambda: (rl.attempt(rl.object_result("r1", "o", "DONE", 1, 1, 1, 0, 1.0, None), 1, None, now_utc(), now_utc(), None),
                                                                      rl.error("r1", "o", "DATA", "c", "m", {}), rl.lineage(b, "s", "t"), rl.schema_snapshot("o", b, "h", "[]", "NONE")) and "ok")
spark.stop()
