"""IngestionEngine: metadata-driven source -> Bronze for ONE object (doc 03/04). Recovery-first, idempotent, retry-aware.

`ingest_object` is the unit of work reused later by the M4 planner. Every step is safe to repeat; durable facts (manifest,
Delta commit, receipt, watermark) decide where a run resumes, never in-memory state."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from de_accelerator.audit.models import (
    ABANDONED,
    BRONZE_COMMITTED,
    BRONZE_WRITTEN,
    DONE,
    EXTRACTING,
    FAILED_FATAL,
    FAILED_RETRYABLE,
    PLANNED,
    SRC_WM_COMMITTED,
    Manifest,
    Receipt,
    batch_id_for,
)
from de_accelerator.audit.repos import EventLog, LeaseRepo, ManifestRepo, ReceiptRepo, RunLog, WatermarkRepo, open_batch
from de_accelerator.connectors.base import (
    ExtractContext,
    ExtractPlan,
    ObjectBinding,
    PlanRequest,
    WatermarkState,
    connector_class,
)
from de_accelerator.core.clock import Clock, SystemClock
from de_accelerator.core.errors import (
    AcceleratorError,
    BatchBusyError,
    IngestionError,
    IntegrityError,
    SourceConfigError,
)
from de_accelerator.core.faults import FaultInjector, NoFaults
from de_accelerator.core.logging import get_logger, quiet_wire_loggers, redact
from de_accelerator.core.secrets import SecretProvider
from de_accelerator.core.versioning import framework_version
from de_accelerator.metadata.reader import MetadataReader, SnapshotMetadataReader
from de_accelerator.metadata.snapshot import SnapshotRef, SnapshotStore
from de_accelerator.metadata.store.base import TableStore
from de_accelerator.processing.bronze.audit_columns import AuditValues
from de_accelerator.processing.bronze.writer import BronzeWriter, Verification

log = get_logger("ingestion")


@dataclass(frozen=True)
class RetrySettings:
    max_attempts: int = 3
    backoff_sec: float = 30.0
    backoff_mult: float = 2.0
    retryable_error_classes: tuple[str, ...] = ("TRANSIENT",)

    def delay(self, attempt: int) -> float:
        return float(self.backoff_sec * (self.backoff_mult ** (attempt - 1)))


@dataclass
class EngineConfig:
    env: str
    catalog: str | None = None
    flavor: str = "LOCAL"
    lease_ttl: timedelta = timedelta(minutes=30)
    landing_root: str | None = None
    sleep: Callable[[float], None] = time.sleep


@dataclass
class ObjectResult:
    object_code: str
    status: str  # DONE | NO_DATA | BUSY | FAILED
    batch_id: str | None = None
    batch_seq: int | None = None
    rows_read: int | None = None
    rows_written: int | None = None
    attempts: int = 1
    error_class: str | None = None
    error_code: str | None = None
    message: str | None = None
    duration_s: float = 0.0
    recovered: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


class IngestionEngine:
    def __init__(self, store: TableStore, spark: Any, secrets: SecretProvider, cfg: EngineConfig, clock: Clock | None = None,
                 faults: FaultInjector | NoFaults | None = None) -> None:  # fmt: skip
        quiet_wire_loggers()  # credentials travel through py4j as Spark options: keep its DEBUG protocol log off
        self.store, self.spark, self.secrets, self.cfg = store, spark, secrets, cfg
        self.clock = clock or SystemClock()
        self.faults = faults or NoFaults()
        self.manifests, self.events, self.receipts = ManifestRepo(store), EventLog(store), ReceiptRepo(store)
        self.watermarks, self.leases, self.runlog = WatermarkRepo(store), LeaseRepo(store), RunLog(store)
        self.writer = BronzeWriter(spark, cfg.catalog, cfg.flavor)
        self._connectors: dict[tuple[str, str], Any] = {}

    # ------------------------------------------------------------------------------------------------------------------------
    def ingest_object(self, reader: MetadataReader, snap: SnapshotRef, project: str, object_code: str, run_id: str, *,
                      stream_key: str = "live", batch_type: str = "LIVE", window_override: tuple[str | None, str | None] | None = None,
                      retry: RetrySettings | None = None, restart: bool = False) -> ObjectResult:  # fmt: skip
        retry = retry or RetrySettings()
        t0 = self.clock.now()
        last: AcceleratorError | None = None
        attempts: list[tuple[int, str | None, Any, Any, str | None]] = []
        for attempt in range(1, retry.max_attempts + 1):
            started = self.clock.now()
            try:
                res = self._ingest_once(reader, snap, project, object_code, run_id, stream_key, batch_type, window_override,
                                        attempt, restart)  # fmt: skip
                res.attempts = attempt
                res.duration_s = (self.clock.now() - t0).total_seconds()
                attempts.append((attempt, None, started, self.clock.now(), None))
                self._finish(run_id, res, attempts)
                return res
            except BatchBusyError as exc:
                res = ObjectResult(object_code, "BUSY", attempts=attempt, message=str(exc))
                self._finish(run_id, res, attempts)
                return res
            except Exception as raw:  # AcceleratorError keeps its class; anything unexpected is a LOGIC error (never silently retried)
                err = raw if isinstance(raw, AcceleratorError) else IngestionError(f"{type(raw).__name__}: {raw}")
                last = err
                msg = redact(str(err))
                resumable = err.error_class in retry.retryable_error_classes  # exhausted transient errors stay resumable
                retry_now = resumable and attempt < retry.max_attempts
                self._record_failure(object_code, stream_key, run_id, attempt, err, msg, resumable)
                attempts.append((attempt, msg, started, self.clock.now(), "retry" if retry_now else None))
                if not retry_now:
                    break
                self.cfg.sleep(retry.delay(attempt))
        assert last is not None
        res = ObjectResult(object_code, "FAILED", attempts=len(attempts), error_class=last.error_class, error_code=last.code,
                           message=redact(str(last)), duration_s=(self.clock.now() - t0).total_seconds())  # fmt: skip
        self._finish(run_id, res, attempts)
        return res

    # ------------------------------------------------------------------------------------------------------------------------
    def _binding(self, reader: MetadataReader, project: str, object_code: str) -> tuple[ObjectBinding, Any, str]:
        obj = reader.object(project, object_code)
        src = reader.source(project, obj.source)
        cols = tuple(reader.columns(project, object_code))
        strat = reader.load_strategy(project, object_code)
        maps = [m for m in reader.layer_mappings(project, object_code) if m.layer == "BRONZE"]
        target = (maps[0].target_table if maps and maps[0].target_table else None) or obj.target_bronze
        if not target:
            raise SourceConfigError(f"object '{object_code}' has no Bronze target (layer_mapping BRONZE.target_table / target_bronze)")
        return ObjectBinding(src, obj, cols, strat), reader.environment(project), str(target)

    def _connector(self, binding: ObjectBinding, env: Any, run_id: str) -> Any:
        key = (binding.source.code, run_id)
        if key not in self._connectors:
            from de_accelerator.connectors.base import ConnectorContext

            ctx = ConnectorContext(binding.source, env, self.secrets, self.spark, run_id, self.clock, self.cfg.landing_root)
            self._connectors[key] = connector_class(binding.source.source_type)(ctx)
        return self._connectors[key]

    def _reader_for(self, m: Manifest, reader: MetadataReader, snap: SnapshotRef) -> MetadataReader:
        """An in-flight batch always continues with the configuration snapshot it was planned with."""
        if m.snapshot_id == snap.snapshot_id:
            return reader
        return SnapshotMetadataReader(SnapshotStore(self.store).load(m.snapshot_id))

    # ------------------------------------------------------------------------------------------------------------------------
    def _ingest_once(self, reader: MetadataReader, snap: SnapshotRef, project: str, object_code: str, run_id: str, stream: str,
                     batch_type: str, window_override: tuple[str | None, str | None] | None, attempt: int, restart: bool) -> ObjectResult:  # fmt: skip
        f = self.faults
        binding, env, target = self._binding(reader, project, object_code)
        caps = connector_class(binding.source.source_type).capabilities  # type: ignore[attr-defined]
        if not caps.supports(binding.strategy.load_type):
            raise SourceConfigError(f"connector '{binding.source.source_type}' does not support load_type {binding.strategy.load_type}")

        wm = self.watermarks.get(object_code, stream)
        committed = WatermarkState(wm.committed_value, wm.committed_seq, wm.epoch) if wm else None
        m = open_batch(self.manifests, self.events, object_code, stream)
        recovered = m is not None
        if m is not None and self.events.state(m.batch_id) == FAILED_FATAL and not restart:
            raise SourceConfigError(f"batch {m.batch_id} (seq {m.batch_seq}) failed fatally earlier; fix the cause and re-run with "
                                    "--restart, or abandon it")  # fmt: skip
        if m is not None and m.snapshot_id != snap.snapshot_id:  # continue with the batch's own configuration
            reader = self._reader_for(m, reader, snap)
            binding, env, target = self._binding(reader, project, object_code)

        connector = self._connector(binding, env, run_id)
        req = PlanRequest(binding, stream, batch_type, committed, window_override)  # type: ignore[arg-type]

        if m is None:
            plan = connector.plan(req)
            if plan.empty:
                return ObjectResult(object_code, "NO_DATA", rows_read=0, rows_written=0)
            f.hit("CP01_before_manifest")
            seq = self.manifests.next_seq(object_code, stream)
            m = Manifest(batch_id_for(object_code, stream, seq), object_code, stream, seq, batch_type, snap.snapshot_id, snap.release_id,
                         plan.wm_from, plan.wm_to, [x.__dict__ for x in plan.file_list] if plan.file_list else None,
                         [{"layer": "BRONZE", "table": target, "write_mode": "APPEND"}], {**plan.plan_json, "lookback": plan.lookback},
                         run_id)  # fmt: skip
            m, _ = self.manifests.create(m)
            if self.events.state(m.batch_id) == "UNKNOWN":
                self.events.append(m.batch_id, PLANNED, run_id=run_id, attempt=attempt, manifest_hash=m.manifest_hash)
            f.hit("CP02_after_manifest")

        epoch = self.leases.acquire(object_code, stream, run_id, self.clock.now(), self.cfg.lease_ttl)
        if epoch is None:
            raise BatchBusyError(f"object '{object_code}' stream '{stream}' is being processed by another runner")
        f.hit("CP03_after_lease")
        plan = ExtractPlan.from_manifest(m.wm_from, m.wm_to, m.file_list, m.plan_json)
        txn_app = f"{object_code}:{stream}:BRONZE"
        rows_read = rows_written = None

        receipt = self.receipts.get(m.batch_id)
        if receipt is None:
            ver = self.writer.verify(target, m.batch_id, stream, m.batch_seq)  # did a previous attempt already commit?
            if ver is None:
                self.events.append(m.batch_id, EXTRACTING, run_id=run_id, attempt=attempt)
                ectx = ExtractContext(m.batch_id, m.batch_seq, stream, self.cfg.landing_root)
                result = connector.extract(plan, req, ectx)
                f.hit("CP04_after_extract")
                av = AuditValues(m.batch_id, m.batch_seq, stream, m.batch_type, run_id, m.snapshot_id, m.release_id, self.clock.now(),
                                 binding.source.code, f"{binding.obj.schema_name + '.' if binding.obj.schema_name else ''}{binding.obj.object_name}",
                                 result.source_schema.schema_hash, framework_version(),
                                 "S" if binding.strategy.load_type in ("FULL", "SNAPSHOT") else "C")  # fmt: skip
                self.leases.acquire(object_code, stream, run_id, self.clock.now(), self.cfg.lease_ttl)  # renew before the long write
                wr = self.writer.write(result.df, target, av, binding.strategy.schema_evolution_mode or "ADD_COLUMNS", txn_app, m.manifest_hash)
                self.runlog.schema_snapshot(object_code, m.batch_id, result.source_schema.schema_hash, wr.schema_json,
                                            wr.decision.change_type)  # fmt: skip
                self.events.append(m.batch_id, BRONZE_WRITTEN, run_id=run_id, attempt=attempt, txn_app_id=txn_app, txn_version=m.batch_seq,
                                   rows=wr.rows_observed)  # fmt: skip
                f.hit("CP05_after_write")
                ver = self.writer.verify(target, m.batch_id, stream, m.batch_seq)
                if ver is None and wr.rows_observed == 0:
                    ver = Verification(None, 0, 0)
                if ver is None:
                    raise IntegrityError(f"Bronze write for batch {m.batch_id} left no verifiable trace in {target}", batch_id=m.batch_id)
                rows_read = wr.rows_observed
            rows_written = ver.rows_in_commit if ver.rows_in_commit is not None else ver.rows_counted
            f.hit("CP06_before_receipt")
            receipt = self.receipts.insert(Receipt(m.batch_id, "BRONZE", target, ver.delta_version, ver.commit_tag, rows_written, 0,
                                                   rows_read if rows_read is not None else rows_written, txn_app, m.batch_seq))  # fmt: skip
            f.hit("CP07_after_receipt")
        else:
            rows_written = int(receipt["rows_written"] or 0)
            rows_read = int(receipt["rows_read"] or rows_written)
        if not self._has_event(m.batch_id, BRONZE_COMMITTED):
            self.events.append(m.batch_id, BRONZE_COMMITTED, run_id=run_id, attempt=attempt, delta_version=receipt["delta_version"],
                               rows_written=rows_written)  # fmt: skip
        f.hit("CP08_after_committed_event")

        # the cursor may advance only now: a verified receipt exists (invariant I1)
        if self.receipts.get(m.batch_id) is None:  # pragma: no cover - defensive
            raise IntegrityError("refusing to advance the watermark without a Bronze receipt", batch_id=m.batch_id)
        self.watermarks.commit(object_code, stream, m.wm_to, m.batch_seq, m.batch_id)
        f.hit("CP09_after_watermark")
        if not self._has_event(m.batch_id, SRC_WM_COMMITTED):
            self.events.append(m.batch_id, SRC_WM_COMMITTED, run_id=run_id, attempt=attempt, committed_value=m.wm_to, committed_seq=m.batch_seq)
        f.hit("CP10_after_watermark_event")
        self.runlog.lineage(m.batch_id, f"{binding.source.code}:{binding.obj.object_name}", target)
        if not self._has_event(m.batch_id, DONE):
            self.events.append(m.batch_id, DONE, run_id=run_id, attempt=attempt, rows=rows_written)
        self.leases.release(object_code, stream, run_id, self.clock.now())
        return ObjectResult(object_code, "DONE", m.batch_id, m.batch_seq, rows_read, rows_written, recovered=recovered)

    # ------------------------------------------------------------------------------------------------------------------------
    def _has_event(self, batch_id: str, event_type: str) -> bool:
        return any(e["event_type"] == event_type for e in self.events.events(batch_id))

    def _record_failure(self, object_code: str, stream: str, run_id: str, attempt: int, exc: AcceleratorError, msg: str, retryable: bool) -> None:
        m = open_batch(self.manifests, self.events, object_code, stream)
        if m is not None:
            self.events.append(m.batch_id, FAILED_RETRYABLE if retryable else FAILED_FATAL, run_id=run_id, attempt=attempt,
                               error_class=exc.error_class, code=exc.code, message=msg[:500])  # fmt: skip
        self.runlog.error(run_id, object_code, exc.error_class, exc.code, msg, {"attempt": attempt, "stream": stream})
        if not isinstance(exc, BatchBusyError):  # a handled failure frees the lease; a crash leaves it to expire (TTL)
            self.leases.release(object_code, stream, run_id, self.clock.now())
        log.warning("object %s attempt %s failed (%s): %s", object_code, attempt, exc.error_class, msg)

    def _finish(self, run_id: str, res: ObjectResult, attempts: list[tuple[int, str | None, Any, Any, str | None]]) -> str:
        oid = self.runlog.object_result(run_id, res.object_code, res.status, res.attempts, res.rows_read, res.rows_written, 0,
                                        res.duration_s, res.error_class)  # fmt: skip
        for n, err, s, e, reason in attempts:
            self.runlog.attempt(oid, n, err, s, e, reason)
        return oid

    # ---- operator actions -----------------------------------------------------------------------------------------------
    def abandon_batch(self, object_code: str, stream: str, reason: str, actor: str) -> str:
        m = open_batch(self.manifests, self.events, object_code, stream)
        if m is None:
            raise SourceConfigError("no open batch to abandon")
        if self.receipts.get(m.batch_id) is not None:
            raise SourceConfigError("batch already has a Bronze receipt (data is committed): re-run to finish it instead of abandoning")
        self.events.append(m.batch_id, ABANDONED, run_id=actor, reason=reason)
        return m.batch_id
