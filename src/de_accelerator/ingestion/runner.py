"""IngestionRunner: runs `ingest_object` for an execution's objects concurrently (thread pool, per-source connection caps).

M2 slice of orchestration (doc M2/05 §4): one process = one Databricks job task. The M4 planner will call `ingest_object`
the same way. Objects are independent: one failure never stops the others."""

from __future__ import annotations

import threading
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from de_accelerator.core.errors import NotFoundError
from de_accelerator.core.versioning import framework_version
from de_accelerator.ingestion.engine import IngestionEngine, ObjectResult, RetrySettings
from de_accelerator.metadata.reader import MetadataReader, SnapshotMetadataReader, StoreConfigSource
from de_accelerator.metadata.snapshot import SnapshotBuilder, SnapshotRef, SnapshotScope, SnapshotStore
from de_accelerator.metadata.store.base import TableStore


@dataclass
class RunSummary:
    run_id: str
    status: str  # SUCCEEDED | PARTIAL | FAILED | NOTHING_TO_DO
    snapshot: SnapshotRef
    results: list[ObjectResult] = field(default_factory=list)

    def by_status(self) -> dict[str, int]:
        out: dict[str, int] = defaultdict(int)
        for r in self.results:
            out[r.status] += 1
        return dict(out)


class IngestionRunner:
    def __init__(self, engine: IngestionEngine, store: TableStore, env: str) -> None:
        self.engine, self.store, self.env = engine, store, env

    def snapshot_for(self, execution: str | None, projects: tuple[str, ...] = (), objects: tuple[str, ...] = ()) -> tuple[SnapshotRef, MetadataReader]:
        snap = SnapshotBuilder(StoreConfigSource(self.store, self.env)).build(SnapshotScope(projects, objects, execution))
        ref = SnapshotStore(self.store).persist(snap, "ingestion-runner")
        return ref, SnapshotMetadataReader(snap)

    def run(self, execution: str | None = None, objects: tuple[str, ...] = (), project: str | None = None, run_id: str | None = None,
            restart: bool = False, trigger: str = "manual", concurrency: int | None = None) -> RunSummary:  # fmt: skip
        run_id = run_id or str(uuid.uuid4())
        ref, reader = self.snapshot_for(execution, (project,) if project else (), objects)
        projects = [p.code for p in reader.projects()]
        ex = None
        if execution:
            for p in projects:
                try:
                    ex = reader.execution(p, execution)
                    project = project or p
                    break
                except NotFoundError:
                    continue
        retry = RetrySettings()
        if ex is not None and ex.retry_policy:
            rp = reader.retry_policy(project or projects[0], ex.retry_policy)
            retry = RetrySettings(rp.max_attempts, rp.backoff_sec, rp.backoff_mult, tuple(rp.retryable_error_classes) or ("TRANSIENT",))
        workers = concurrency or (ex.concurrency if ex is not None and ex.concurrency else 4)

        todo: list[tuple[str, str, str]] = []  # (project, object, source)
        for p in projects:
            for o in reader.objects(p, layer="BRONZE"):
                todo.append((p, o.code, o.source))
        self.engine.runlog.start_pipeline(run_id, execution, self.env, trigger, framework_version(), ref.release_id, ref.snapshot_id)
        if not todo:
            self.engine.runlog.end_pipeline(run_id, "NOTHING_TO_DO")
            return RunSummary(run_id, "NOTHING_TO_DO", ref, [])

        caps: dict[tuple[str, str], threading.Semaphore] = {}
        for p, _, s in todo:
            n = int((reader.source(p, s).options or {}).get("max_connections", workers))
            caps.setdefault((p, s), threading.Semaphore(max(1, n)))

        def work(item: tuple[str, str, str]) -> ObjectResult:
            p, code, s = item
            with caps[(p, s)]:
                return self.engine.ingest_object(reader, ref, p, code, run_id, retry=retry, restart=restart)

        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            results = list(pool.map(work, todo))
        failed = [r for r in results if r.status == "FAILED"]
        status = "SUCCEEDED" if not failed else ("FAILED" if len(failed) == len(results) else "PARTIAL")
        self.engine.runlog.end_pipeline(run_id, status)
        return RunSummary(run_id, status, ref, results)

