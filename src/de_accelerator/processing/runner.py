"""Dependency-aware transform runner (D9): topological levels, parallel within a level, HARD-dependent skip.

TEMPORARY DUPLICATION (M4-CONSOLIDATE-01): the thread-pool / run-summary pattern mirrors M2's `ingestion/runner.py` (≈25 lines);
M3 does not import or modify it.
"""

from __future__ import annotations

import threading
import uuid
from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import SparkSession

from de_accelerator.core.errors import NotFoundError
from de_accelerator.core.logging import redact
from de_accelerator.core.versioning import framework_version
from de_accelerator.metadata.reader import SnapshotMetadataReader, StoreConfigSource
from de_accelerator.metadata.snapshot import SnapshotBuilder, SnapshotScope, SnapshotStore
from de_accelerator.metadata.store.base import TableStore
from de_accelerator.processing.common.errors import TransformConfigError
from de_accelerator.processing.common.unit import RetryPolicy, UnitResult
from de_accelerator.processing.gold.engine import GoldEngine
from de_accelerator.processing.silver.engine import SilverEngine, TransformConfig

Node = tuple[str, str, str]  # (layer, project, object)


@dataclass
class RunSummary:
    run_id: str
    status: str  # SUCCEEDED | PARTIAL | FAILED | NOTHING_TO_DO
    snapshot: Any
    results: list[UnitResult] = field(default_factory=list)

    def by_status(self) -> dict[str, int]:
        out: dict[str, int] = defaultdict(int)
        for r in self.results:
            out[r.status] += 1
        return dict(out)


def levels(nodes: Sequence[Node], deps: dict[Node, set[tuple[Node, bool]]]) -> list[list[Node]]:
    """Topological levels (Kahn). `deps[n]` = {(upstream, is_hard)} restricted to nodes in the run. Raises on a cycle."""
    remaining = set(nodes)
    done: set[Node] = set()
    out: list[list[Node]] = []
    while remaining:
        ready = sorted(n for n in remaining if all(u in done for u, _ in deps.get(n, ())))
        if not ready:
            raise TransformConfigError(f"dependency cycle among {sorted(remaining)}")
        out.append(ready)
        done |= set(ready)
        remaining -= set(ready)
    return out


class TransformRunner:
    def __init__(self, store: TableStore, spark: SparkSession, cfg: TransformConfig, silver: SilverEngine | None = None,
                 gold: GoldEngine | None = None, env: str | None = None) -> None:  # fmt: skip
        self.store, self.spark, self.cfg = store, spark, cfg
        self.silver = silver or SilverEngine(store, spark, cfg)
        self.gold = gold or GoldEngine(store, spark, cfg)
        self.env = env or cfg.env

    def snapshot_for(
        self, execution: str | None, projects: tuple[str, ...] = (), objects: tuple[str, ...] = ()
    ) -> tuple[Any, Any]:
        snap = SnapshotBuilder(StoreConfigSource(self.store, self.env)).build(SnapshotScope(projects, objects, execution))
        ref = SnapshotStore(self.store).persist(snap, "transform-runner")
        return ref, SnapshotMetadataReader(snap)

    def _nodes(self, reader: Any, layer: str | None) -> tuple[list[Node], dict[Node, set[tuple[Node, bool]]]]:
        nodes: list[Node] = []
        for p in [x.code for x in reader.projects()]:
            for o in reader.objects(p):
                maps = {m.layer for m in reader.layer_mappings(p, o.code)}
                if "SILVER" in maps and layer in (None, "SILVER"):
                    nodes.append(("SILVER", p, o.code))
                if "GOLD" in maps and layer in (None, "GOLD"):
                    nodes.append(("GOLD", p, o.code))
        present = set(nodes)
        deps: dict[Node, set[tuple[Node, bool]]] = defaultdict(set)
        by_obj: dict[tuple[str, str], list[Node]] = defaultdict(list)
        for n in nodes:
            by_obj[(n[1], n[2])].append(n)
        for n in nodes:
            layer_n, p, code = n
            if (
                layer_n == "GOLD"
            ):  # a Gold object depends on the Silver/Gold node of each declared input (HARD unless the dependency table says SOFT)
                soft = {d.depends_on for d in reader.dependencies(p) if d.object == code and d.dependency_type == "SOFT"}
                for t in reader.transformations(p, code, "GOLD"):
                    for inp in t.input_objects:
                        for up in by_obj.get((p, inp), []):
                            if up != n and up in present:
                                deps[n].add((up, inp not in soft))
        for p in [x.code for x in reader.projects()]:
            for d in reader.dependencies(p):
                for n in by_obj.get((p, d.object), []):
                    for up in by_obj.get((p, d.depends_on), []):
                        if up != n and up in present and not (up[0] == "GOLD" and n[0] == "SILVER"):
                            deps[n].add((up, d.dependency_type != "SOFT"))
        return nodes, deps

    def run(self, execution: str | None = None, objects: tuple[str, ...] = (), layer: str | None = None, project: str | None = None,
            run_id: str | None = None, trigger: str = "manual", concurrency: int | None = None) -> RunSummary:  # fmt: skip
        run_id = run_id or str(uuid.uuid4())
        ref, reader = self.snapshot_for(execution, (project,) if project else (), objects)
        retry, workers = RetryPolicy(), concurrency or 4
        if execution:
            for p in [x.code for x in reader.projects()]:
                try:
                    ex = reader.execution(p, execution)
                except NotFoundError:
                    continue
                if ex.retry_policy:
                    rp = reader.retry_policy(p, ex.retry_policy)
                    retry = RetryPolicy(
                        rp.max_attempts, rp.backoff_sec, rp.backoff_mult, tuple(rp.retryable_error_classes) or ("TRANSIENT",)
                    )
                workers = concurrency or ex.concurrency or workers
                break
        nodes, deps = self._nodes(reader, layer)
        if objects:
            nodes = [n for n in nodes if n[2] in objects]
            deps = {n: {(u, h) for u, h in d if u in nodes} for n, d in deps.items() if n in nodes}
        self.silver.audit.runlog.start_pipeline(
            run_id, execution, self.env, trigger, framework_version(), ref.release_id, ref.snapshot_id
        )
        if not nodes:
            self.silver.audit.runlog.end_pipeline(run_id, "NOTHING_TO_DO")
            return RunSummary(run_id, "NOTHING_TO_DO", ref, [])
        results: dict[Node, UnitResult] = {}
        lock = threading.Lock()

        def work(n: Node) -> UnitResult:
            layer_n, p, code = n
            failed_up = [
                u
                for u, hard in deps.get(n, ())
                if hard and results.get(u) is not None and results[u].status in ("FAILED", "BUSY", "SKIPPED_UPSTREAM_FAILED")
            ]
            if failed_up:
                res = UnitResult(
                    code, layer_n, "SKIPPED_UPSTREAM_FAILED", message=f"upstream failed: {sorted(u[2] for u in failed_up)}"
                )
                (self.silver if layer_n == "SILVER" else self.gold).audit.finish(run_id, res, [])
                return res
            eng = self.silver if layer_n == "SILVER" else self.gold
            try:
                return eng.run_object(reader, ref, p, code, run_id, retry)
            except Exception as exc:  # noqa: BLE001 - safety net: one object's unexpected error must never abort the others
                return UnitResult(
                    code,
                    layer_n,
                    "FAILED",
                    error_class="LOGIC",
                    error_code="ACC-7000",
                    message=redact(f"{type(exc).__name__}: {exc}"),
                )

        for lvl in levels(nodes, deps):
            with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
                for n, res in zip(lvl, pool.map(work, lvl), strict=True):
                    with lock:
                        results[n] = res
        ordered = [results[n] for lvl in levels(nodes, deps) for n in lvl]
        bad = [r for r in ordered if r.status in ("FAILED", "SKIPPED_UPSTREAM_FAILED")]
        status = "SUCCEEDED" if not bad else ("FAILED" if len(bad) == len(ordered) else "PARTIAL")
        self.silver.audit.best_effort("end_pipeline", self.silver.audit.runlog.end_pipeline, run_id, status)
        return RunSummary(run_id, status, ref, ordered)
