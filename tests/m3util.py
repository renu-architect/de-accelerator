"""Test world for the M3 transformation engine: a real v3 metadata pack loaded through M1, Bronze data written through the real M2
Bronze writer + M2 audit repositories (manifest, events, Bronze receipt), and Silver/Gold engines on a (Delta or in-memory) audit store."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import functions as F

from de_accelerator.audit.models import DONE, PLANNED, Manifest, Receipt, batch_id_for
from de_accelerator.audit.repos import EventLog, ManifestRepo, ReceiptRepo
from de_accelerator.core.clock import ManualClock
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.reader import SnapshotMetadataReader, StoreConfigSource
from de_accelerator.metadata.release import PackLoader, ReleaseManager
from de_accelerator.metadata.snapshot import SnapshotBuilder, SnapshotScope, SnapshotStore
from de_accelerator.metadata.store.memory import InMemoryTableStore
from de_accelerator.metadata.validation import validate_pack
from de_accelerator.processing.bronze.audit_columns import AuditValues
from de_accelerator.processing.bronze.writer import BronzeWriter
from de_accelerator.processing.silver.engine import SilverEngine, TransformConfig

PROJECT = "m3"
T0 = datetime(2026, 11, 3, 8, 0, 0, tzinfo=timezone.utc)


def silver_obj(code: str = "orders", *, schema: str = "dbo", table: str | None = None, load: dict[str, Any] | None = None,
               merge: str = "SCD1", delete_mode: str = "soft", columns: list[dict[str, Any]] | None = None,
               on_cast_error: str | None = None, dedupe_order: str | None = None, effective: str | None = None, passthrough: bool = False,
               scd_columns: list[str] | None = None, dq: list[dict[str, Any]] | None = None, business_keys: list[str] | None = None) -> dict[str, Any]:  # fmt: skip
    cols = columns if columns is not None else [
        {"name": "Id", "ordinal": 1, "source_type": "int", "target_type": "bigint", "is_bk": True, "is_nullable": False, "target_name": "id"},
        {"name": "Name", "ordinal": 2, "target_type": "string", "target_name": "name",
         "cleanse_rules": {"ops": [{"op": "trim"}, {"op": "upper"}, {"op": "nullif", "values": [""]}]}},
        {"name": "Amount", "ordinal": 3, "target_type": "decimal(18,2)", "target_name": "amount",
         "cleanse_rules": {"ops": [{"op": "trim"}, {"op": "replace", "from": ",", "to": ""}]}},
        {"name": "Modified", "ordinal": 4, "target_type": "timestamp", "target_name": "modified_ts",
         "cleanse_rules": {"ops": [{"op": "trim"}, {"op": "parse_timestamp", "format": "yyyy-MM-dd HH:mm:ss"}]}},
    ]  # fmt: skip
    return {"code": code, "schema": schema, "table": table or code, "load": load, "merge": merge, "delete_mode": delete_mode, "columns": cols, "on_cast_error": on_cast_error,
            "dedupe_order": dedupe_order, "effective": effective, "passthrough": passthrough, "scd_columns": scd_columns,
            "dq": dq or [], "business_keys": business_keys or ["Id"]}  # fmt: skip


def write_pack(
    root: Path, objects: list[dict[str, Any]], gold: list[dict[str, Any]] | None = None, source: dict[str, Any] | None = None
) -> Path:
    """Build a schema-v3 pack (env DEV) with BRONZE + SILVER mappings for `objects` (and optional GOLD objects)."""
    p = root / "pack"
    (p / f"projects/{PROJECT}/x").mkdir(parents=True)
    (p / "overrides").mkdir()
    (p / "overrides/DEV.yaml").write_text("overrides: []\n")
    (p / "transform_files").mkdir()
    (p / "pack.yaml").write_text(yaml.safe_dump({"pack_format": 1, "pack_name": "m3-test", "pack_version": "1.0.0",
                                                 "requires_core": ">=0.1.0.dev0,<1.0", "metadata_schema_version": 3,
                                                 "client": {"code": "acme", "name": "Acme"}, "environments": ["DEV"]}))  # fmt: skip
    (p / f"projects/{PROJECT}/project.yaml").write_text("name: M3 test\n")

    def put(name: str, kind: str, items: list[dict[str, Any]]) -> None:
        (p / f"projects/{PROJECT}/x/{name}.yaml").write_text(yaml.safe_dump({"kind": kind, "items": items}, sort_keys=False))

    put("env", "environment", [{"code": "DEV", "catalog_name": "acme_dev", "secret_scope": "kv-acme-dev"}])
    put(
        "retry",
        "retry_policy",
        [{"code": "std", "max_attempts": 3, "backoff_sec": 0, "backoff_mult": 1.0, "retryable_error_classes": ["TRANSIENT"]}],
    )
    put(
        "src",
        "source",
        [source or {"code": "erp", "source_type": "mock", "connector_type": "mock", "auth_mode": "NONE", "connection": {"dataset": "erp"}}],
    )
    objs, cols, maps, dqs, loads = [], [], [], [], []
    for o in objects:
        c = o["code"]
        objs.append({"code": c, "source": "erp", "schema_name": o["schema"], "object_name": o["table"], "business_keys": o["business_keys"],
                     "enabled_layers": ["BRONZE", "SILVER"], "delete_mode": o["delete_mode"], "target_bronze": f"bronze.{c}",
                     "target_silver": f"silver.{c}"})  # fmt: skip
        for col in o["columns"]:
            cols.append({"object": c, **col})
        loads.append(
            {
                "object": c,
                **(o["load"] or {"load_type": "INCREMENTAL", "watermark_column": "Modified", "watermark_type": "timestamp"}),
            }
        )
        maps.append({"object": c, "layer": "BRONZE", "target_table": f"bronze.{c}", "merge_type": "APPEND"})
        sm: dict[str, Any] = {"object": c, "layer": "SILVER", "target_table": f"silver.{c}", "merge_type": o["merge"]}
        for k_in, k_out in (("on_cast_error", "on_cast_error"), ("dedupe_order", "dedupe_order"), ("effective", "effective_column"),
                            ("scd_columns", "scd_columns")):  # fmt: skip
            if o[k_in]:
                sm[k_out] = o[k_in]
        if o["passthrough"]:
            sm["column_mode"] = "PASSTHROUGH"
        maps.append(sm)
        dqs += [{"object": c, **r} for r in o["dq"]]
    trs: list[dict[str, Any]] = []
    deps: list[dict[str, Any]] = []
    for g in gold or []:
        c = g["code"]
        objs.append({"code": c, "source": "erp", "object_name": c, "object_kind": "derived", "business_keys": g.get("keys", []),
                     "enabled_layers": ["GOLD"], "target_gold": f"gold.{c}"})  # fmt: skip
        maps.append({"object": c, "layer": "GOLD", "target_table": f"gold.{c}", "merge_type": g["merge"]})
        for i, step in enumerate(g["steps"], 1):
            rel = f"transform_files/gold/{c}_{i}.{'sql' if step['type'] == 'SQL' else 'py'}"
            (p / rel).parent.mkdir(parents=True, exist_ok=True)
            (p / rel).write_text(step["text"])
            trs.append({"object": c, "layer": "GOLD", "step_no": i, "type": step["type"], "body_ref": rel,
                        "input_objects": step.get("inputs", []), "params": step.get("params", {})})  # fmt: skip
            for inp in step.get("inputs", []):
                row = {"object": c, "depends_on": inp, "dependency_type": g.get("dep_type", "HARD")}
                if row not in deps:
                    deps.append(row)
    put("obj", "object", objs)
    if trs:
        put("tr", "transformation", trs)
    if deps:
        put("dep", "dependency", deps)
    if cols:
        put("col", "column", cols)
    put("load", "load_strategy", loads)
    put("map", "layer_mapping", maps)
    if dqs:
        put("dq", "dq_rule", dqs)
    return p


@dataclass
class World:
    store: Any
    spark: Any
    engine: SilverEngine
    clock: ManualClock
    reader: Any
    snap: Any
    bronze: BronzeWriter
    manifests: ManifestRepo
    events: EventLog
    receipts: ReceiptRepo
    root: Path
    pack: Path | None = None
    gold: Any = None
    runner: Any = None
    seq: dict[str, int] = field(default_factory=dict)

    # ---- Bronze seeding through the real M2 writer and repositories ----------------------------------------------------
    def seed(self, code: str, rows: list[dict[str, Any]], *, op: str = "C", mode: str = "incremental", receipt: bool = True,
             ingest_ts: datetime | None = None, ddl: str | None = None) -> str:  # fmt: skip
        """Append one Bronze batch (stream `live`) and (unless receipt=False) its verified Bronze receipt; returns the batch id."""
        seq = self.manifests.next_seq(code, "live")
        bid = batch_id_for(code, "live", seq)
        target = f"bronze.{code}"
        m = Manifest(bid, code, "live", seq, "LIVE", "bronze-snap", "bronze-rel", None, None, None,
                     [{"layer": "BRONZE", "table": target, "write_mode": "APPEND"}], {"mode": mode, "table": code}, "seed")  # fmt: skip
        self.manifests.create(m)
        self.events.append(bid, PLANNED, run_id="seed")
        df = self.spark.createDataFrame(rows, ddl or "Id int, Name string, Amount string, Modified string")
        av = AuditValues(
            bid,
            seq,
            "live",
            "LIVE",
            "seed",
            "bronze-snap",
            "bronze-rel",
            ingest_ts or self.clock.now(),
            "erp",
            code,
            "h",
            "0.1.0",
            "S" if mode == "full" else "C",
        )
        if op == "D":
            df = df.withColumn("_op_type", F.lit("D"))
        w = self.bronze.write(df, target, av, "ADD_COLUMNS", f"{code}:live:BRONZE", m.manifest_hash)
        if receipt:
            ver = self.bronze.verify(target, bid, "live", seq)
            self.receipts.insert(
                Receipt(
                    bid,
                    "BRONZE",
                    target,
                    ver.delta_version if ver else None,
                    None,
                    w.rows_observed,
                    0,
                    w.rows_observed,
                    None,
                    seq,
                )
            )
            self.events.append(bid, DONE, run_id="seed")
        return bid

    def run(self, code: str = "orders", retry: Any = None):
        return self.engine.run_object(self.reader, self.snap, PROJECT, code, str(uuid.uuid4()), retry)

    def run_gold(self, code: str, retry: Any = None):
        return self.gold.run_object(self.reader, self.snap, PROJECT, code, str(uuid.uuid4()), retry)

    def table(self, name: str, order: str | None = None) -> list[dict[str, Any]]:
        df = self.spark.table(name)
        if order:
            df = df.orderBy(order)
        return [r.asDict() for r in df.collect()]

    def silver(self, code: str = "orders", order: str | None = None) -> list[dict[str, Any]]:
        df = self.spark.table(f"silver.{code}")
        if order:
            df = df.orderBy(order)
        return [r.asDict() for r in df.collect()]

    def reload(self) -> None:
        """Re-snapshot the active release (after pack edits are loaded)."""
        self.reader, self.snap = snapshot(self.store)


def snapshot(store: Any, env: str = "DEV"):
    s = SnapshotBuilder(StoreConfigSource(store, env)).build(SnapshotScope((), (), None))
    ref = SnapshotStore(store).persist(s, "test")
    return SnapshotMetadataReader(s), ref


def build_world(tmp_path: Path, spark: Any, objects: list[dict[str, Any]], store: Any = None, faults: Any = None,
                gold: list[dict[str, Any]] | None = None, engine_cls: Any = SilverEngine, source: dict[str, Any] | None = None) -> World:  # fmt: skip
    pack = write_pack(tmp_path, objects, gold, source)
    report, bundle = validate_pack(pack, "DEV")
    assert report.ok and bundle is not None, report.to_text()
    store = store if store is not None else InMemoryTableStore()
    Migrator(store, MIGRATIONS).apply()
    rel = PackLoader(store, "DEV").stage(bundle, approved_by="test")
    ReleaseManager(store, "DEV").activate(rel["release_id"])
    reader, ref = snapshot(store)
    clock = ManualClock(T0)
    cfg = TransformConfig(env="DEV", catalog=None, flavor="LOCAL", sleep=lambda s: None, transform_root=str(pack))
    engine = engine_cls(store, spark, cfg, clock, faults)
    try:  # Gold engine and runner arrive with P4; Silver-only worlds work without them
        from de_accelerator.processing.gold.engine import GoldEngine
        from de_accelerator.processing.runner import TransformRunner

        gold_engine = GoldEngine(store, spark, cfg, clock, faults)
        runner = TransformRunner(store, spark, cfg, engine, gold_engine, "DEV")
    except ImportError:  # pragma: no cover
        gold_engine = runner = None
    return World(store, spark, engine, clock, reader, ref, BronzeWriter(spark, None, "LOCAL"), ManifestRepo(store), EventLog(store),
                 ReceiptRepo(store), tmp_path, pack, gold_engine, runner)  # fmt: skip


def drop_tables(spark: Any, *names: str) -> None:
    for n in names:
        spark.sql(f"DROP TABLE IF EXISTS {n}")
