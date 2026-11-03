"""Helpers for ingestion tests: build a metadata pack (mock source), load+activate it via M1, create engine/runner."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from de_accelerator.connectors.mock.connector import MOCK_SOURCES, MockSource
from de_accelerator.core.clock import ManualClock
from de_accelerator.core.faults import FaultInjector
from de_accelerator.core.secrets import StaticSecretProvider
from de_accelerator.ingestion.engine import EngineConfig, IngestionEngine, RetrySettings
from de_accelerator.ingestion.runner import IngestionRunner
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.reader import SnapshotMetadataReader, StoreConfigSource
from de_accelerator.metadata.release import PackLoader, ReleaseManager
from de_accelerator.metadata.snapshot import SnapshotBuilder, SnapshotScope, SnapshotStore
from de_accelerator.metadata.store.memory import InMemoryTableStore
from de_accelerator.metadata.validation import validate_pack

DDL = "id bigint, amount double, modified string"


def ts(h: int, m: int = 0) -> str:
    return f"2026-11-03T{h:02d}:{m:02d}:00.000000Z"


def rows(*hours: int) -> list[dict[str, Any]]:
    return [{"id": i + 1, "amount": float(i + 1) * 10, "modified": ts(h)} for i, h in enumerate(hours)]


def write_pack(root: Path, objects: list[dict[str, Any]], source: dict[str, Any] | None = None) -> Path:
    """objects: {code, table, load_type, [watermark_column], [lookback], [schema_evolution_mode]} -> pack dir (env DEV)."""
    p = root / "pack"
    (p / "projects/ingest/x").mkdir(parents=True)
    (p / "overrides").mkdir()
    (p / "overrides/DEV.yaml").write_text("overrides: []\n")
    (p / "pack.yaml").write_text(yaml.safe_dump({"pack_format": 1, "pack_name": "ingest-test", "pack_version": "1.0.0",
                                                 "requires_core": ">=0.1.0.dev0,<1.0", "metadata_schema_version": 2,
                                                 "client": {"code": "acme", "name": "Acme"}, "environments": ["DEV"]}))  # fmt: skip
    (p / "projects/ingest/project.yaml").write_text("name: ingest\n")

    def put(name: str, kind: str, items: list[dict[str, Any]]) -> None:
        (p / f"projects/ingest/x/{name}.yaml").write_text(yaml.safe_dump({"kind": kind, "items": items}, sort_keys=False))

    put("env", "environment", [{"code": "DEV", "catalog_name": "acme_dev", "secret_scope": "kv-acme-dev"}])
    put("retry", "retry_policy", [{"code": "std", "max_attempts": 3, "backoff_sec": 0, "backoff_mult": 1.0, "retryable_error_classes": ["TRANSIENT"]}])
    put("src", "source", [source or {"code": "erp", "source_type": "mock", "connector_type": "mock", "auth_mode": "NONE",
                                      "connection": {"dataset": "erp"}}])  # fmt: skip
    put("obj", "object", [{"code": o["code"], "source": "erp", "schema_name": o.get("schema", "dbo"), "object_name": o["table"], "business_keys": ["id"],
                           "enabled_layers": ["BRONZE"], "target_bronze": f"bronze.{o['code']}"} for o in objects])  # fmt: skip
    ls = []
    for o in objects:
        d = {"object": o["code"], "load_type": o["load_type"], "schema_evolution_mode": o.get("schema_evolution_mode", "ADD_COLUMNS"),
             "is_complete_extract": o["load_type"] in ("FULL", "SNAPSHOT")}  # fmt: skip
        if o["load_type"] == "INCREMENTAL":
            d.update(watermark_column=o.get("watermark_column", "modified"), watermark_type=o.get("watermark_type", "timestamp"),
                     lookback_window=o.get("lookback", "PT1H"))  # fmt: skip
            if o.get("late_arrival"):
                d["late_arrival_window"] = o["late_arrival"]
        if o.get("partition_column"):
            d.update(partition_column=o["partition_column"], num_partitions=o.get("num_partitions", 4))
        ls.append(d)
    put("ls", "load_strategy", ls)
    put("lm", "layer_mapping", [{"object": o["code"], "layer": "BRONZE", "target_table": f"bronze.{o['code']}", "merge_type": "APPEND"} for o in objects])
    put("exec", "execution", [{"code": "run_all", "scope": {"objects": [o["code"] for o in objects]}, "concurrency": 4, "retry_policy": "std"}])
    return p


@dataclass
class World:
    store: InMemoryTableStore
    spark: Any
    clock: ManualClock
    source: MockSource
    pack: Path
    engine: IngestionEngine
    runner: IngestionRunner
    sleeps: list[float] = field(default_factory=list)
    secrets: dict[str, str] = field(default_factory=dict)

    def new_engine(self, faults: FaultInjector | None = None) -> IngestionEngine:
        cfg = EngineConfig(env="DEV", catalog=None, flavor="LOCAL", sleep=self.sleeps.append)
        eng = IngestionEngine(self.store, self.spark, StaticSecretProvider(self.secrets), cfg, self.clock, faults)
        self.engine = eng
        self.runner = IngestionRunner(eng, self.store, "DEV")
        return eng

    def snapshot(self):
        snap = SnapshotBuilder(StoreConfigSource(self.store, "DEV")).build(SnapshotScope())
        ref = SnapshotStore(self.store).persist(snap, "test")
        return ref, SnapshotMetadataReader(snap)

    def ingest(self, code: str, run_id: str | None = None, **kw: Any):
        ref, reader = self.snapshot()
        return self.engine.ingest_object(reader, ref, "ingest", code, run_id or str(uuid.uuid4()), retry=kw.pop("retry", RetrySettings(3, 0, 1)), **kw)

    def bronze(self, code: str):
        return self.spark.table(f"bronze.{code}")


def build_world(tmp_path: Path, spark: Any, objects: list[dict[str, Any]], source_tables: dict[str, tuple[str, list[dict[str, Any]]]],
                source: dict[str, Any] | None = None, secrets: dict[str, str] | None = None) -> World:
    store = InMemoryTableStore()
    Migrator(store, MIGRATIONS).apply()
    pack = write_pack(tmp_path, objects, source)
    report, bundle = validate_pack(pack, "DEV", deployed_schema_version=2)
    assert report.ok, report.to_text()
    rel = PackLoader(store, "DEV").stage(bundle)  # type: ignore[arg-type]
    ReleaseManager(store, "DEV").activate(rel["release_id"])
    src = MockSource()
    for name, (ddl, data) in source_tables.items():
        src.add(name, ddl, data)
    if source is None:
        MOCK_SOURCES["erp"] = src
    clock = ManualClock(datetime(2026, 11, 3, 12, 0, tzinfo=timezone.utc))  # noqa: UP017
    w = World(store, spark, clock, src, pack, None, None)  # type: ignore[arg-type]
    w.secrets = secrets or {}
    w.new_engine()
    return w
