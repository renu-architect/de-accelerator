"""Silver engine: one recoverable unit per object (doc 00 section 8).

plan -> manifest -> lease -> build -> merge -> content verification -> receipt -> TARGET_COMMITTED -> cursor advance -> CURSOR_ADVANCED -> DONE
Recovery decides from durable facts only: manifest without receipt -> rebuild + re-merge (a data no-op if already applied) + verify;
receipt without cursor move -> advance only; cursor moved without DONE -> finish bookkeeping.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from de_accelerator.audit.layer_cursor import LayerCursorRepo
from de_accelerator.audit.models import DONE, PLANNED, Manifest, Receipt, batch_id_for
from de_accelerator.audit.repos import open_batch
from de_accelerator.core.clock import Clock, SystemClock
from de_accelerator.core.errors import BatchBusyError
from de_accelerator.core.faults import FaultInjector
from de_accelerator.metadata.packs.ids import canonical_json, sha256_hex
from de_accelerator.metadata.reader import SnapshotMetadataReader
from de_accelerator.metadata.snapshot import SnapshotStore
from de_accelerator.metadata.store.base import TableStore
from de_accelerator.processing.common.columns import SilverPlan, build_silver_plan
from de_accelerator.processing.common.errors import TransformConfigError
from de_accelerator.processing.common.names import layer_table
from de_accelerator.processing.common.rejects import RejectWriter
from de_accelerator.processing.common.unit import LEASE_TTL, RetryPolicy, UnitAudit, UnitResult, run_with_retry
from de_accelerator.processing.silver import strategies as st
from de_accelerator.processing.silver.builder import BuildContext, Staged, build_staged, check_reconciliation
from de_accelerator.processing.silver.planner import BRONZE_STREAM, UnitPlan, plan_unit

log = logging.getLogger(__name__)
STREAM = "SILVER:live"
LAYER = "SILVER"
TARGET_COMMITTED = "TARGET_COMMITTED"
CURSOR_ADVANCED = "CURSOR_ADVANCED"


class NoFaults:
    def hit(self, point: str) -> None:
        return None


@dataclass
class TransformConfig:
    env: str
    catalog: str | None = None
    flavor: str = "LOCAL"
    lease_ttl: timedelta = LEASE_TTL
    sleep: Callable[[float], None] = time.sleep
    transform_root: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


class SilverEngine:
    def __init__(self, store: TableStore, spark: SparkSession, cfg: TransformConfig, clock: Clock | None = None,
                 faults: FaultInjector | NoFaults | None = None) -> None:  # fmt: skip
        self.store, self.spark, self.cfg = store, spark, cfg
        self.clock = clock or SystemClock()
        self.faults = faults or NoFaults()
        self.audit = UnitAudit(store, self.clock)
        self.cursors = LayerCursorRepo(store)
        self.rejects = RejectWriter(spark, cfg.catalog, cfg.flavor)

    # ---- public -------------------------------------------------------------------------------------------------------
    def run_object(
        self, reader: Any, snap: Any, project: str, object_code: str, run_id: str, retry: RetryPolicy | None = None
    ) -> UnitResult:
        return run_with_retry(object_code, LAYER, STREAM, run_id, retry or RetryPolicy(), self.clock, self.cfg.sleep, self.audit,
                              lambda attempt: self._run_once(reader, snap, project, object_code, run_id, attempt))  # fmt: skip

    def replan(self, object_code: str, reason: str, actor: str) -> str:
        return self.audit.replan(object_code, STREAM, LAYER, reason, actor)

    # ---- one attempt --------------------------------------------------------------------------------------------------
    def _reader_for(self, m: Manifest, reader: Any, snap: Any) -> Any:
        """An in-flight unit always continues with the configuration snapshot it was planned with."""
        if m.snapshot_id == snap.snapshot_id:
            return reader
        return SnapshotMetadataReader(SnapshotStore(self.store).load(m.snapshot_id))

    def _run_once(self, reader: Any, snap: Any, project: str, object_code: str, run_id: str, attempt: int) -> UnitResult:
        f = self.faults
        a = self.audit
        m = open_batch(a.manifests, a.events, object_code, STREAM)
        recovered = m is not None
        if m is None:
            plan = build_silver_plan(reader, project, object_code)
            unit = plan_unit(a.manifests, a.events, a.receipts, object_code, self.cursors.applied_seq(object_code, LAYER))
            if unit is None:
                return UnitResult(object_code, LAYER, "NO_DATA", rows_in=0, rows_applied=0, rows_rejected=0)
            f.hit("U_CP1_before_manifest")
            seq = a.manifests.next_seq(object_code, STREAM)
            m = Manifest(batch_id_for(object_code, STREAM, seq), object_code, STREAM, seq, "SILVER", snap.snapshot_id, snap.release_id,
                         str(unit.seq_from), str(unit.seq_to), None, [{"layer": LAYER, "table": plan.silver_target, "write_mode": plan.merge_type}],
                         {"mode": "silver_unit", "bronze_batches": list(unit.batch_ids), "bronze_target": unit.bronze_target,
                          "bronze_rows": unit.bronze_rows_expected, "snapshot_like": unit.snapshot_like,
                          "logic_hash": sha256_hex(canonical_json(plan.logic_fingerprint()))}, run_id)  # fmt: skip
            m, _ = a.manifests.create(m)
            if a.events.state(m.batch_id) == "UNKNOWN":
                a.events.append(m.batch_id, PLANNED, run_id=run_id, attempt=attempt, manifest_hash=m.manifest_hash)
            f.hit("U_CP2_after_manifest")
        rdr = self._reader_for(m, reader, snap)
        plan = build_silver_plan(rdr, project, object_code)
        epoch = a.leases.acquire(object_code, STREAM, run_id, self.clock.now(), self.cfg.lease_ttl)
        if epoch is None:
            raise BatchBusyError(f"object '{object_code}' stream '{STREAM}' is being processed by another runner")
        fq = layer_table(plan.silver_target, LAYER, self.cfg.catalog, self.cfg.flavor)
        seq_to = int(m.wm_to or 0)

        receipt = a.receipts.get(m.batch_id, LAYER)
        if receipt is None:
            counters = self._apply(m, plan, fq, f)
            f.hit("U_CP4_after_merge")
            receipt = a.receipts.insert(Receipt(m.batch_id, LAYER, plan.silver_target, counters.pop("delta_version"), None, counters["rows_applied"],
                                                counters["rows_rejected"], counters["rows_in"], None, m.batch_seq,
                                                counters.pop("checksum")))  # fmt: skip
            a.events.append(m.batch_id, TARGET_COMMITTED, run_id=run_id, attempt=attempt, counters=counters)
        else:
            counters = self._counters_from_events(m.batch_id)
        f.hit("U_CP5_after_receipt")
        if self.cursors.applied_seq(object_code, LAYER) < seq_to:
            self.cursors.advance(object_code, LAYER, seq_to, m.batch_id)
        if not any(e["event_type"] == CURSOR_ADVANCED for e in a.events.events(m.batch_id)):
            a.events.append(m.batch_id, CURSOR_ADVANCED, run_id=run_id, attempt=attempt, applied_seq=seq_to)
        f.hit("U_CP6_after_cursor")
        a.events.append(m.batch_id, DONE, run_id=run_id, attempt=attempt)
        a.lineage(m.batch_id, m.plan_json.get("bronze_target", ""), plan.silver_target, LAYER)
        a.leases.release(object_code, STREAM, run_id, self.clock.now())
        return UnitResult(object_code, LAYER, "DONE", m.batch_id, m.batch_seq, counters.get("rows_in"), counters.get("rows_applied"),
                          counters.get("rows_rejected"), recovered=recovered, extra={"counters": counters})  # fmt: skip

    def _counters_from_events(self, batch_id: str) -> dict[str, Any]:
        for e in reversed(self.audit.events.events(batch_id)):
            if e["event_type"] == TARGET_COMMITTED:
                return dict(json.loads(e["payload"]).get("counters", {}))
        return {}

    # ---- build + merge + verify ---------------------------------------------------------------------------------------
    def _apply(self, m: Manifest, plan: SilverPlan, fq: str, f: Any) -> dict[str, Any]:
        now = self.clock.now()
        unit = UnitPlan(int(m.wm_from or 0), int(m.wm_to or 0), tuple(m.plan_json["bronze_batches"]), int(m.plan_json["bronze_rows"]),
                        str(m.plan_json["bronze_target"]), bool(m.plan_json.get("snapshot_like")))  # fmt: skip
        bronze_fq = layer_table(unit.bronze_target, "BRONZE", self.cfg.catalog, self.cfg.flavor)
        bronze = self.spark.table(bronze_fq).where(
            (F.col("_stream_key") == BRONZE_STREAM) & F.col("_batch_id").isin(*unit.batch_ids)
        )
        ctx = BuildContext(plan.object_code, m.batch_id, now, dedupe=plan.merge_type != "SCD2")
        staged: Staged = build_staged(plan, bronze, ctx)
        try:
            check_reconciliation(staged.counters, unit.bronze_rows_expected, plan.object_code)
            wctx = st.WriteContext(fq, m.batch_id, m.batch_seq, now, m.snapshot_id, f"{plan.object_code}:{STREAM}:SILVER")
            self.rejects.write(plan.object_code, staged.rejects)
            f.hit("U_CP3_after_build")
            all_rejects = staged.rejects
            if plan.merge_type == "APPEND":
                final = st.finalize(staged.good, wctx)
                applied = st.apply_append(self.spark, wctx, final)
                n, chk = st.verify_append(self.spark, wctx, final)
            elif plan.merge_type == "SCD1":
                final = st.finalize(staged.good, wctx)
                applied = st.apply_scd1(self.spark, wctx, final, plan)
                n, chk = st.verify_scd1(self.spark, wctx, final, plan)
            elif plan.merge_type == "SCD2":
                from de_accelerator.processing.silver import scd2  # P3 module, imported where used

                res = scd2.apply_scd2(
                    self.spark, wctx, plan, staged.good, staged.target_columns, plan.object_code, m.batch_id, now
                )
                try:
                    self.rejects.write(plan.object_code, res.late_rejects)
                    all_rejects = staged.rejects.unionByName(res.late_rejects)
                    applied = res.applied
                    n, chk = scd2.verify_scd2(self.spark, wctx, plan, res)
                    late = res.counters["late_rejected"]
                    staged.counters["rows_rejected"] += late
                    staged.counters["rows_applied"] -= late
                    staged.counters.update(res.counters)
                finally:
                    res.release()
            else:
                raise TransformConfigError(f"Silver merge_type {plan.merge_type} is not implemented")
            missing = self.rejects.missing(plan.object_code, all_rejects)
            if missing:
                from de_accelerator.processing.common.errors import VerificationError

                raise VerificationError(f"{missing} staged reject rows are not in the reject table", batch_id=m.batch_id)
            out: dict[str, Any] = dict(staged.counters)
            out["delta_version"] = applied.delta_version
            out["checksum"] = f"{n}:{chk}"
            return out
        finally:
            staged.release()
