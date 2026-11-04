"""Unit execution wrapper shared by the Silver and Gold engines.

TEMPORARY DUPLICATION (approved, tracked M4-CONSOLIDATE-01): this mirrors the behaviour of M2's `IngestionEngine.ingest_object`
(retry loop), `_record_failure`, `_best_effort` and `_finish`, using only M2's *public* repositories. M2 is intentionally not
edited in M3; in M4 both are extracted into one neutral package. Behaviour pinned by tests/unit/test_unit_wrapper.py:
transient errors retry then stay resumable, fatal errors are not retried, a busy lease returns BUSY, an audit-write failure never
changes the outcome, and nothing aborts the whole run.

Differences from M2 on purpose: run_object/lineage rows carry the real layer (M2's `RunLog` hard-codes BRONZE).
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from de_accelerator.audit.models import ABANDONED, FAILED_FATAL, FAILED_RETRYABLE
from de_accelerator.audit.repos import EventLog, LeaseRepo, ManifestRepo, ReceiptRepo, RunLog, open_batch
from de_accelerator.core.clock import Clock
from de_accelerator.core.errors import AcceleratorError, BatchBusyError, ConfigError
from de_accelerator.core.logging import redact
from de_accelerator.metadata.store.base import TableStore
from de_accelerator.processing.common.errors import TransformError

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_sec: float = 30.0
    backoff_mult: float = 2.0
    retryable_error_classes: tuple[str, ...] = ("TRANSIENT",)

    def delay(self, attempt: int) -> float:
        return float(self.backoff_sec * (self.backoff_mult ** (attempt - 1)))


@dataclass
class UnitResult:
    object_code: str
    layer: str
    status: str  # DONE | NO_DATA | BUSY | FAILED | SKIPPED_UPSTREAM_FAILED
    batch_id: str | None = None
    batch_seq: int | None = None
    rows_in: int | None = None
    rows_applied: int | None = None
    rows_rejected: int | None = None
    attempts: int = 1
    error_class: str | None = None
    error_code: str | None = None
    message: str | None = None
    duration_s: float = 0.0
    recovered: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


Attempt = tuple[int, str | None, datetime, datetime, str | None]  # (n, error, start, end, retry_reason)


class UnitAudit:
    """Best-effort audit writes for a layer's units (an audit failure is logged, never propagated)."""

    def __init__(self, store: TableStore, clock: Clock) -> None:
        self.store, self.clock = store, clock
        self.manifests, self.events, self.leases, self.runlog = (
            ManifestRepo(store),
            EventLog(store),
            LeaseRepo(store),
            RunLog(store),
        )
        self.receipts = ReceiptRepo(store)

    def replan(self, object_code: str, stream: str, layer: str, reason: str, actor: str) -> str:
        """Operator action (no data is ever skipped): abandon the open unit so the next run re-plans the same input under the current
        snapshot. Refused when the unit already has a receipt (its data is committed: re-run to finish it instead).
        `actor` is a free-text label, NOT an authenticated identity (same as M2 abandon-batch)."""
        m = open_batch(self.manifests, self.events, object_code, stream)
        if m is None:
            raise ConfigError("no open unit to re-plan")
        if self.receipts.get(m.batch_id, layer) is not None:
            raise ConfigError("the unit already has a receipt (data is committed): re-run to finish it instead of re-planning")
        self.events.append(m.batch_id, ABANDONED, run_id=actor, reason=reason)
        return m.batch_id

    def best_effort(self, what: str, fn: Callable[..., Any], *args: Any) -> Any:
        try:
            return fn(*args)
        except Exception as audit_exc:  # noqa: BLE001 - audit problems must not change the outcome of the object
            log.error("audit write failed (%s): %s", what, redact(f"{type(audit_exc).__name__}: {audit_exc}"))
            return None

    def record_failure(
        self, object_code: str, stream: str, run_id: str, attempt: int, exc: AcceleratorError, msg: str, retryable: bool
    ) -> None:
        log.warning("object %s attempt %s failed (%s): %s", object_code, attempt, exc.error_class, msg)

        def mark() -> None:
            m = open_batch(self.manifests, self.events, object_code, stream)
            if m is not None:
                self.events.append(m.batch_id, FAILED_RETRYABLE if retryable else FAILED_FATAL, run_id=run_id, attempt=attempt,
                                   error_class=exc.error_class, code=exc.code, message=msg[:500])  # fmt: skip

        self.best_effort("failure event", mark)
        self.best_effort(
            "run_error",
            self.runlog.error,
            run_id,
            object_code,
            exc.error_class,
            exc.code,
            msg,
            {"attempt": attempt, "stream": stream},
        )
        if not isinstance(exc, BatchBusyError):  # a handled failure frees the lease; a crash leaves it to expire (TTL)
            self.best_effort("lease release", self.leases.release, object_code, stream, run_id, self.clock.now())

    def finish(self, run_id: str, res: UnitResult, attempts: list[Attempt]) -> str | None:
        """Record the per-object result (with its real layer) and the attempts."""

        def write() -> str:
            oid = str(uuid.uuid4())
            self.store.insert("run_object", [{"run_object_id": oid, "run_id": run_id, "object_code": res.object_code, "layer": res.layer,
                                              "status": res.status, "attempt": res.attempts, "rows_read": res.rows_in,
                                              "rows_written": res.rows_applied, "rows_rejected": res.rows_rejected,
                                              "duration_s": res.duration_s, "error_class": res.error_class}])  # fmt: skip
            return oid

        oid = self.best_effort("run_object", write)
        if oid is not None:
            for n, err, s, e, reason in attempts:
                self.best_effort("run_attempt", self.runlog.attempt, oid, n, err, s, e, reason)
        return oid  # type: ignore[no-any-return]

    def lineage(self, batch_id: str, source_object: str, target_object: str, layer: str) -> None:
        self.best_effort("lineage", self.store.insert_if_absent, "run_lineage",
                         [{"edge_id": str(uuid.uuid5(uuid.NAMESPACE_OID, f"{batch_id}|{target_object}")), "batch_id": batch_id,
                           "source_object": source_object, "target_object": target_object, "source_column": None,
                           "target_column": None, "layer": layer}], ["edge_id"])  # fmt: skip


def run_with_retry(object_code: str, layer: str, stream: str, run_id: str, retry: RetryPolicy, clock: Clock,
                   sleep: Callable[[float], None], audit: UnitAudit, run_once: Callable[[int], UnitResult]) -> UnitResult:  # fmt: skip
    """Run `run_once(attempt)` with the M2 retry semantics. `run_once` must itself begin with recovery of an open unit."""
    t0 = clock.now()
    last: AcceleratorError | None = None
    attempts: list[Attempt] = []
    for attempt in range(1, retry.max_attempts + 1):
        started = clock.now()
        try:
            res = run_once(attempt)
            res.attempts = attempt
            res.duration_s = (clock.now() - t0).total_seconds()
            attempts.append((attempt, None, started, clock.now(), None))
            audit.finish(run_id, res, attempts)
            return res
        except BatchBusyError as exc:
            res = UnitResult(object_code, layer, "BUSY", attempts=attempt, message=str(exc))
            audit.finish(run_id, res, attempts)
            return res
        except (
            Exception
        ) as raw:  # AcceleratorError keeps its class; anything unexpected is a LOGIC error (never silently retried)
            err = raw if isinstance(raw, AcceleratorError) else TransformError(f"{type(raw).__name__}: {raw}")
            last = err
            msg = redact(str(err))
            resumable = err.error_class in retry.retryable_error_classes  # exhausted transient errors stay resumable
            retry_now = resumable and attempt < retry.max_attempts
            audit.record_failure(object_code, stream, run_id, attempt, err, msg, resumable)
            attempts.append((attempt, msg, started, clock.now(), "retry" if retry_now else None))
            if not retry_now:
                break
            sleep(retry.delay(attempt))
    assert last is not None
    res = UnitResult(object_code, layer, "FAILED", attempts=len(attempts), error_class=last.error_class, error_code=last.code,
                     message=redact(str(last)), duration_s=(clock.now() - t0).total_seconds())  # fmt: skip
    audit.finish(run_id, res, attempts)
    return res


LEASE_TTL = timedelta(minutes=30)
