"""READ-ONLY PROBE: exercise the four new CLI commands (never tested) with the in-memory audit store + mock source."""
import sys, tempfile, traceback
from pathlib import Path

sys.path.insert(0, "/home/user/de-accelerator")
import de_accelerator.connectors.builtin  # noqa: F401
from de_accelerator.cli.main import run
from de_accelerator.metadata.store.spark import build_local_spark
from tests.ingest_util import DDL, build_world, rows, ts

wh = tempfile.mkdtemp()
spark = build_local_spark(wh)
w = build_world(Path(wh), spark, [{"code": "orders", "table": "orders", "load_type": "INCREMENTAL"}], {"orders": (DDL, rows(9, 10, 11))})
(Path(wh) / "base.yaml").write_text("client_code: acme\nenvironment: DEV\nflavor: LOCAL\nwarehouse_dir: /unused\n")
base = ["--bootstrap", str(Path(wh) / "base.yaml"), "--actor", "tester"]
lines = []


def call(*argv):
    lines.clear()
    try:
        rc = run([*base, *argv], lambda cfg: w.store, out=lines.append, engine_factory=lambda cfg, st: w.engine)
    except BaseException:
        traceback.print_exc()
        rc = "EXC"
    print(f"\n$ de-accel {' '.join(argv)}\n  exit={rc}")
    for ln in lines:
        print("  " + ln.replace("\n", "\n  "))


call("ingest", "--execution", "run_all")
call("batch-status", "--object", "orders")
w.source.tables["orders"].rows.append({"id": 9, "amount": 1.0, "modified": ts(11, 30)})
call("reprocess", "--object", "orders", "--project", "ingest", "--from", ts(8), "--to", ts(10), "--reason", "probe")
call("reprocess", "--object", "orders", "--project", "ingest", "--from", ts(8), "--to", ts(10), "--reason", "second")
call("batch-status", "--object", "orders", "--stream", "reprocess-1")
print("\nbronze rows:", spark.table("bronze.orders").count(), "| live cursor:", w.engine.watermarks.get("orders", "live"))
print("run_pipeline rows:", w.store.count("run_pipeline"), "| run_object rows:", w.store.count("run_object"),
      "| distinct run_ids in run_object:", len({r['run_id'] for r in w.store.select('run_object')}),
      "| run_object ids missing from run_pipeline:", sorted({r['run_id'] for r in w.store.select('run_object')} - {r['run_id'] for r in w.store.select('run_pipeline')}))
print("distinct _execution_id in bronze:", sorted(r[0] for r in spark.table("bronze.orders").select("_execution_id").distinct().collect()))
call("ingest", "--execution", "nope_not_there")
call("abandon-batch", "--object", "orders", "--reason", "nothing open")
spark.stop()
