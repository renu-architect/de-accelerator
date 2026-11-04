"""Gold engine: reviewed SQL/PySpark files over pinned Silver versions; FULL (OVERWRITE) and KEY_MERGE (MERGE) (doc 00 section 5/8).

Planning resolves each input table's head Delta version and a logic hash; if every head equals the cursor's `input_versions` and the
logic hash equals the last committed unit's, the run is NO_DATA. Otherwise the versions are pinned in the manifest BEFORE any read and
the inputs are read `VERSION AS OF` them; recovery reuses the pinned versions and never re-decides.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import uuid
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
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
from de_accelerator.processing.common.errors import TransformConfigError, TransformDataError, VerificationError
from de_accelerator.processing.common.hashing import content_hash
from de_accelerator.processing.common.names import layer_table, q
from de_accelerator.processing.common.unit import RetryPolicy, UnitAudit, UnitResult, run_with_retry
from de_accelerator.processing.gold.loader import load_pyspark, read_artifact
from de_accelerator.processing.gold.sqlguard import check_query_text, parameters_used, resolve_placeholders
from de_accelerator.processing.silver import strategies as st
from de_accelerator.processing.silver.engine import CURSOR_ADVANCED, TARGET_COMMITTED, NoFaults, TransformConfig

log = logging.getLogger(__name__)
STREAM = "GOLD:main"
LAYER = "GOLD"
GOLD_TECH = ("_content_hash", "_gold_batch_id", "_gold_ts", "_config_snapshot_id")


def typed_params(params: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, spec in (params or {}).items():
        t, v = spec["type"], spec["value"]
        if t == "string":
            out[name] = str(v)
        elif t == "int":
            out[name] = int(v)
        elif t == "decimal":
            out[name] = Decimal(str(v))
        elif t == "date":
            out[name] = dt.date.fromisoformat(str(v))
        elif t == "bool":
            out[name] = v if isinstance(v, bool) else str(v).lower() in ("true", "1", "yes")
        else:  # pragma: no cover - validator rejects other types
            raise TransformConfigError(f"parameter '{name}': unsupported type {t}")
    return out


class GoldEngine:
    def __init__(self, store: TableStore, spark: SparkSession, cfg: TransformConfig, clock: Clock | None = None,
                 faults: FaultInjector | NoFaults | None = None) -> None:  # fmt: skip
        self.store, self.spark, self.cfg = store, spark, cfg
        self.clock = clock or SystemClock()
        self.faults = faults or NoFaults()
        self.audit = UnitAudit(store, self.clock)
        self.cursors = LayerCursorRepo(store)

    def run_object(
        self, reader: Any, snap: Any, project: str, object_code: str, run_id: str, retry: RetryPolicy | None = None
    ) -> UnitResult:
        return run_with_retry(object_code, LAYER, STREAM, run_id, retry or RetryPolicy(), self.clock, self.cfg.sleep, self.audit,
                              lambda attempt: self._run_once(reader, snap, project, object_code, run_id, attempt))  # fmt: skip

    def replan(self, object_code: str, reason: str, actor: str) -> str:
        return self.audit.replan(object_code, STREAM, LAYER, reason, actor)

    # ---- configuration of one Gold object ----------------------------------------------------------------------------------
    def _config(self, reader: Any, project: str, object_code: str) -> SimpleNamespace:
        obj = reader.object(project, object_code)
        gm = [m for m in reader.layer_mappings(project, object_code) if m.layer == "GOLD"]
        if not gm:
            raise TransformConfigError(f"object '{object_code}' has no GOLD layer mapping")
        target = gm[0].target_table or obj.target_gold
        steps = sorted(reader.transformations(project, object_code, "GOLD"), key=lambda t: t.step_no)
        if not target or not steps:
            raise TransformConfigError(f"object '{object_code}' needs a Gold target and transformation steps")
        inputs: dict[str, str] = {}
        for s in steps:
            for inp in s.input_objects:
                maps = {m.layer: m for m in reader.layer_mappings(project, inp)}
                lay = "SILVER" if "SILVER" in maps else ("GOLD" if "GOLD" in maps else None)
                if lay is None or not maps[lay].target_table:
                    raise TransformConfigError(f"input '{inp}' of '{object_code}' has no Silver/Gold target")
                inputs[inp] = layer_table(maps[lay].target_table, lay, self.cfg.catalog, self.cfg.flavor)
        return SimpleNamespace(object=obj, target=target, merge_type=gm[0].merge_type, steps=steps, inputs=inputs,
                               keys=tuple(obj.business_keys), fq=layer_table(target, LAYER, self.cfg.catalog, self.cfg.flavor))  # fmt: skip

    @staticmethod
    def _logic_hash(c: SimpleNamespace) -> str:
        return sha256_hex(canonical_json({"target": c.target, "merge": c.merge_type, "keys": list(c.keys), "inputs": sorted(c.inputs),
                                          "steps": [[s.step_no, s.type, s.body_ref, s.checksum, s.params, list(s.input_objects)] for s in c.steps]}))  # fmt: skip

    def _reader_for(self, m: Manifest, reader: Any, snap: Any) -> Any:
        return (
            reader if m.snapshot_id == snap.snapshot_id else SnapshotMetadataReader(SnapshotStore(self.store).load(m.snapshot_id))
        )

    # ---- one attempt -------------------------------------------------------------------------------------------------------------
    def _run_once(self, reader: Any, snap: Any, project: str, object_code: str, run_id: str, attempt: int) -> UnitResult:
        f, a = self.faults, self.audit
        m = open_batch(a.manifests, a.events, object_code, STREAM)
        recovered = m is not None
        if m is None:
            c = self._config(reader, project, object_code)
            heads: dict[str, int] = {}
            for inp, fq in c.inputs.items():
                v = st.head_version(self.spark, fq) if self.spark.catalog.tableExists(fq) else None
                if v is None:
                    raise TransformConfigError(f"input '{inp}' ({fq}) does not exist yet: run its Silver unit first")
                heads[inp] = v
            logic = self._logic_hash(c)
            cur = self.cursors.get(object_code, LAYER)
            if cur is not None and cur.input_versions == heads:
                last = a.manifests.get(cur.applied_batch_id) if cur.applied_batch_id else None
                if last is not None and last.plan_json.get("logic_hash") == logic:
                    return UnitResult(object_code, LAYER, "NO_DATA", rows_in=0, rows_applied=0, rows_rejected=0)
            f.hit("U_CP1_before_manifest")
            seq = a.manifests.next_seq(object_code, STREAM)
            m = Manifest(batch_id_for(object_code, STREAM, seq), object_code, STREAM, seq, "GOLD", snap.snapshot_id, snap.release_id,
                         str(cur.applied_seq if cur else 0), str(seq), None, [{"layer": LAYER, "table": c.target, "write_mode": c.merge_type}],
                         {"mode": "gold_unit", "inputs": heads, "logic_hash": logic, "merge_type": c.merge_type}, run_id)  # fmt: skip
            m, _ = a.manifests.create(m)
            if a.events.state(m.batch_id) == "UNKNOWN":
                a.events.append(m.batch_id, PLANNED, run_id=run_id, attempt=attempt, manifest_hash=m.manifest_hash)
            f.hit("U_CP2_after_manifest")
        rdr = self._reader_for(m, reader, snap)
        c = self._config(rdr, project, object_code)
        epoch = a.leases.acquire(object_code, STREAM, run_id, self.clock.now(), self.cfg.lease_ttl)
        if epoch is None:
            raise BatchBusyError(f"object '{object_code}' stream '{STREAM}' is being processed by another runner")
        pinned = {k: int(v) for k, v in m.plan_json["inputs"].items()}
        receipt = a.receipts.get(m.batch_id, LAYER)
        if receipt is None:
            counters = self._execute(m, c, pinned, f)
            f.hit("U_CP4_after_merge")
            receipt = a.receipts.insert(Receipt(m.batch_id, LAYER, c.target, counters.pop("delta_version"), None, counters["rows_applied"], 0,
                                                counters["rows_in"], None, m.batch_seq, counters.pop("checksum")))  # fmt: skip
            a.events.append(m.batch_id, TARGET_COMMITTED, run_id=run_id, attempt=attempt, counters=counters, inputs=pinned)
        else:
            counters = {}
            for e in reversed(a.events.events(m.batch_id)):
                if e["event_type"] == TARGET_COMMITTED:
                    counters = dict(json.loads(e["payload"]).get("counters", {}))
                    break
        f.hit("U_CP5_after_receipt")
        if self.cursors.applied_seq(object_code, LAYER) < m.batch_seq:
            self.cursors.advance(object_code, LAYER, m.batch_seq, m.batch_id, pinned)
        if not any(e["event_type"] == CURSOR_ADVANCED for e in a.events.events(m.batch_id)):
            a.events.append(m.batch_id, CURSOR_ADVANCED, run_id=run_id, attempt=attempt, applied_seq=m.batch_seq)
        f.hit("U_CP6_after_cursor")
        a.events.append(m.batch_id, DONE, run_id=run_id, attempt=attempt)
        for inp in c.inputs:
            a.lineage(m.batch_id, inp, object_code, LAYER)
        a.leases.release(object_code, STREAM, run_id, self.clock.now())
        return UnitResult(object_code, LAYER, "DONE", m.batch_id, m.batch_seq, counters.get("rows_in"), counters.get("rows_applied"), 0,
                          recovered=recovered, extra={"counters": counters})  # fmt: skip

    # ---- build, write, verify ---------------------------------------------------------------------------------------------------
    def _result(self, m: Manifest, c: SimpleNamespace, pinned: dict[str, int]) -> DataFrame:
        tag = uuid.uuid4().hex[:8]
        in_views: dict[str, str] = {}
        dfs: dict[str, DataFrame] = {}
        for inp, fq in c.inputs.items():
            df = self.spark.sql(f"SELECT * FROM {fq} VERSION AS OF {pinned[inp]}")
            view = f"__in_{tag}_{len(in_views)}"
            df.createOrReplaceTempView(view)
            in_views[inp] = view
            dfs[inp] = df
        step_views: dict[int, str] = {}
        steps_df: dict[int, DataFrame] = {}
        result: DataFrame | None = None
        for s in c.steps:
            params = typed_params(dict(s.params or {}))
            declared = {i: in_views[i] for i in s.input_objects}
            if s.type == "SQL":
                text = read_artifact(self.cfg.transform_root, s.body_ref, s.checksum).decode("utf-8")
                sql = resolve_placeholders(text, declared, step_views)
                check_query_text(sql)
                unknown = parameters_used(sql) - set(params)
                if unknown:
                    raise TransformConfigError(f"SQL uses undeclared parameter(s) {sorted(unknown)}", step=s.step_no)
                result = self.spark.sql(sql, args=params) if params else self.spark.sql(sql)
            elif s.type == "PYSPARK":
                fn = load_pyspark(self.cfg.transform_root, s.body_ref, s.checksum)
                ctx = SimpleNamespace(
                    spark=self.spark, object_code=m.object_code, batch_id=m.batch_id, params=params, steps=dict(steps_df)
                )
                result = fn({i: dfs[i] for i in s.input_objects}, params, ctx)
                if not isinstance(result, DataFrame):
                    raise TransformConfigError(f"transformation '{s.body_ref}' must return a DataFrame")
            else:  # pragma: no cover - validator rejects other types
                raise TransformConfigError(f"transformation type {s.type} is not supported")
            view = f"__step_{tag}_{s.step_no}"
            result.createOrReplaceTempView(view)
            step_views[s.step_no] = view
            steps_df[s.step_no] = result
        assert result is not None
        for k in c.keys:
            if k not in result.columns:
                raise TransformConfigError(f"transformation result of '{m.object_code}' lacks business key column '{k}'")
        bad = [x for x in result.columns if x.startswith("_")]
        if bad:
            raise TransformConfigError(f"result columns {bad} use the reserved '_' prefix")
        return result

    def _execute(self, m: Manifest, c: SimpleNamespace, pinned: dict[str, int], f: Any) -> dict[str, Any]:
        now = self.clock.now()
        res = self._result(m, c, pinned)
        cols = list(res.columns)
        out = (res.withColumn("_content_hash", content_hash(cols)).withColumn("_gold_batch_id", F.lit(m.batch_id))
               .withColumn("_gold_ts", F.lit(now).cast("timestamp")).withColumn("_config_snapshot_id", F.lit(m.snapshot_id))
               .persist(StorageLevel.MEMORY_AND_DISK))  # fmt: skip
        try:
            try:
                rows = out.count()
            except Exception as exc:  # noqa: BLE001
                msg = str(exc)
                if "FileNotFound" in msg or "does not exist" in msg or "VACUUM" in msg.upper():
                    raise TransformConfigError(
                        "a pinned input version is no longer readable (vacuumed?): keep Silver retention longer than "
                        "the Gold lag, then run transform-replan to re-pin",
                        object=m.object_code,
                    ) from exc
                raise
            f.hit("U_CP3_after_build")
            if c.merge_type == "OVERWRITE":
                applied = self._full(c, out, m)
                n, chk = self._verify_full(c, out)
            elif c.merge_type == "MERGE":
                self._check_unique(c, out)
                applied = self._key_merge(c, out)
                n, chk = self._verify_key_merge(c, out)
            else:
                raise TransformConfigError(f"Gold merge_type {c.merge_type} is not supported")
            return {"rows_in": rows, "rows_applied": rows, "delta_version": applied.delta_version, "checksum": f"{n}:{chk}"}
        finally:
            out.unpersist()

    # FULL ----------------------------------------------------------------------------------------------------------------------
    def _full(self, c: SimpleNamespace, out: DataFrame, m: Manifest) -> st.Applied:
        """Atomic full replacement in ONE Delta commit: a MERGE that matches nothing, inserts every result row and deletes every
        existing row. (DataFrame/SQL overwrite and CREATE OR REPLACE are not used: they fail on the local Delta 3.2.1/Spark 3.5.9
        pair with 'does not support truncate in batch mode'; MERGE is portable, atomic and already proven in M1-M3.)"""
        st.ensure_table(self.spark, c.fq, out)
        cols = list(out.columns)
        view = f"__gold_{uuid.uuid4().hex}"
        out.createOrReplaceTempView(view)
        try:
            self.spark.sql(
                f"MERGE INTO {c.fq} t USING {view} s ON 1 = 0 "
                f"WHEN NOT MATCHED THEN INSERT ({', '.join(q(x) for x in cols)}) VALUES ({', '.join(f's.{q(x)}' for x in cols)}) "
                "WHEN NOT MATCHED BY SOURCE THEN DELETE"
            )
        finally:
            self.spark.catalog.dropTempView(view)
        return st.Applied(st.head_version(self.spark, c.fq))

    def _verify_full(self, c: SimpleNamespace, out: DataFrame) -> tuple[int, str]:
        exp = st.checksum_of(out)
        got = st.checksum_of(self.spark.table(c.fq))
        if exp != got:
            raise VerificationError(f"Gold FULL verification failed: result checksum {exp} differs from the target {got}")
        return exp

    # KEY_MERGE -----------------------------------------------------------------------------------------------------------------
    def _check_unique(self, c: SimpleNamespace, out: DataFrame) -> None:
        d = out.groupBy(*[F.col(q(k)) for k in c.keys]).count().where("count > 1").count()
        if d:
            raise TransformDataError(
                f"the transformation result has {d} duplicated business keys: KEY_MERGE needs one row per key", rows=d
            )

    def _key_merge(self, c: SimpleNamespace, out: DataFrame) -> st.Applied:
        st.ensure_table(self.spark, c.fq, out)
        cols = list(out.columns)
        view = f"__gold_{uuid.uuid4().hex}"
        out.createOrReplaceTempView(view)
        on = " AND ".join(f"t.{q(k)} <=> s.{q(k)}" for k in c.keys)
        try:
            self.spark.sql(f"MERGE INTO {c.fq} t USING {view} s ON {on} "
                           f"WHEN MATCHED AND t._content_hash <> s._content_hash THEN UPDATE SET {', '.join(f't.{q(x)} = s.{q(x)}' for x in cols)} "
                           f"WHEN NOT MATCHED THEN INSERT ({', '.join(q(x) for x in cols)}) VALUES ({', '.join(f's.{q(x)}' for x in cols)})")  # fmt: skip
        finally:
            self.spark.catalog.dropTempView(view)
        return st.Applied(st.head_version(self.spark, c.fq))

    def _verify_key_merge(self, c: SimpleNamespace, out: DataFrame) -> tuple[int, str]:
        t = self.spark.table(c.fq)
        cond = [out[k].eqNullSafe(t[k]) for k in c.keys] + [out["_content_hash"] == t["_content_hash"]]
        missing = out.join(t, cond, "left_anti").count()
        keys = [F.col(q(k)) for k in c.keys]
        dup = (
            t.join(out.select(*keys).distinct(), [k for k in c.keys], "left_semi")
            .groupBy(*keys)
            .count()
            .where("count > 1")
            .count()
        )
        if missing or dup:
            raise VerificationError(f"Gold KEY_MERGE verification failed: {missing} result rows not in the target, {dup} duplicated keys",
                                    missing=missing, duplicated=dup)  # fmt: skip
        return st.checksum_of(out)
