"""READ-ONLY PROBE (not part of the repo): does the engine work when the AUDIT tables live on Delta (as in production)?
All repo tests use the in-memory store for audit tables; this runs the same engine on SparkDeltaTableStore."""
import sys, tempfile, traceback
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "/home/user/de-accelerator")
import de_accelerator.connectors.builtin  # noqa: F401
from de_accelerator.connectors.mock.connector import MOCK_SOURCES, MockSource
from de_accelerator.core.clock import ManualClock
from de_accelerator.core.secrets import StaticSecretProvider
from de_accelerator.ingestion.engine import EngineConfig, IngestionEngine
from de_accelerator.ingestion.runner import IngestionRunner
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.release import PackLoader, ReleaseManager
from de_accelerator.metadata.store.spark import SparkDeltaTableStore, build_local_spark
from de_accelerator.metadata.validation import validate_pack
from tests.ingest_util import DDL, rows, write_pack

wh = tempfile.mkdtemp()
spark = build_local_spark(wh)
store = SparkDeltaTableStore(spark, flavor="LOCAL")
Migrator(store, MIGRATIONS).apply()
codes = ["a", "b", "c", "d"]
pack = write_pack(Path(wh) / "p", [{"code": c, "table": c, "load_type": "FULL"} for c in codes])
rep, bundle = validate_pack(pack, "DEV", deployed_schema_version=2)
assert rep.ok, rep.to_text()
ReleaseManager(store, "DEV").activate(PackLoader(store, "DEV").stage(bundle)["release_id"])
src = MockSource()
for c in codes:
    src.add(c, DDL, rows(9, 10, 11))
MOCK_SOURCES["erp"] = src
clock = ManualClock(datetime(2026, 11, 3, 12, 0, tzinfo=timezone.utc))
eng = IngestionEngine(store, spark, StaticSecretProvider({}), EngineConfig(env="DEV", flavor="LOCAL", sleep=lambda s: None), clock)
runner = IngestionRunner(eng, store, "DEV")


def show(title, s):
    print(f"\n### {title}: run status={s.status} by_status={s.by_status()}")
    for r in s.results:
        print(f"   {r.object_code}: {r.status} rows={r.rows_written} attempts={r.attempts} err=[{r.error_class}] {(r.message or '')[:230]}")


try:
    show("PROBE 1: Delta audit store, concurrency=1, 4 objects", runner.run("run_all", concurrency=1))
except BaseException:
    traceback.print_exc()
clock.advance(minutes=1)
try:
    show("PROBE 2: Delta audit store, concurrency=4, 4 objects (second run => second batch each)", runner.run("run_all", concurrency=4))
except BaseException:
    traceback.print_exc()
print("\nbronze rows:", {c: spark.table(f"bronze.{c}").count() for c in codes if spark.catalog.tableExists(f"bronze.{c}")})
print("audit rows: manifests=%s events=%s receipts=%s run_object=%s run_error=%s" % tuple(
    spark.table(t).count() for t in ("audit.run_batch_manifest", "audit.run_batch_event", "audit.run_target_receipt", "audit.run_object", "audit.run_error")))
spark.stop()
