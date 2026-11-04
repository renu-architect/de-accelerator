"""Build the staged Silver dataset of one unit from Bronze rows: map -> cleanse -> cast -> key check -> DQ -> dedupe (D1/D2/D4/D10).

Output columns: declared target columns + `_content_hash, _effective_ts, _src_batch_seq, _src_batch_id, _op ('U'|'D')`. Rejected rows are
returned as pointer rows (Bronze batch id + `_row_hash` + reason + column name); they never carry keys, values or payload.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from de_accelerator.processing.common.cleansing import apply_ops
from de_accelerator.processing.common.columns import ColumnSpec, DqRule, SilverPlan
from de_accelerator.processing.common.errors import TransformConfigError, TransformDataError
from de_accelerator.processing.common.hashing import content_hash

_ERR_TYPE = "array<struct<reason:string,col:string>>"
TECH = ("_content_hash", "_effective_ts", "_src_batch_seq", "_src_batch_id", "_op")


@dataclass
class BuildContext:
    object_code: str
    unit_batch_id: str
    reject_ts: datetime
    dedupe: bool = True  # SCD2 handles duplicates itself (logical-change identity), so it passes False


@dataclass
class Staged:
    good: DataFrame  # persisted; call release()
    rejects: DataFrame
    specs: tuple[ColumnSpec, ...]
    counters: dict[str, int] = field(default_factory=dict)
    _flagged: DataFrame | None = None

    @property
    def target_columns(self) -> list[str]:
        return [s.target for s in self.specs]

    def release(self) -> None:
        for df in (self._flagged, self.good):
            if df is not None:
                try:
                    df.unpersist()
                except Exception:  # noqa: BLE001, S110 - best effort cleanup
                    pass


def _passthrough_specs(bronze: DataFrame) -> tuple[ColumnSpec, ...]:
    return tuple(ColumnSpec(c, c, None, ()) for c in bronze.columns if not c.startswith("_"))


def _is_blank(c: Column, is_string: bool) -> Column:
    return c.isNull() | (F.trim(c) == "") if is_string else c.isNull()


def _dq_failing(rule: DqRule, t: dict[str, Column]) -> list[tuple[str, Column]]:
    """(target-column-name-free) failing conditions per rule column: [(source_column, condition)]."""
    out: list[tuple[str, Column]] = []
    for src in rule.columns:
        c = t[src]
        if rule.rule_type == "NOT_NULL":
            out.append((src, c.isNull()))
        elif rule.rule_type == "VALID_VALUES":
            vals = [str(v) for v in rule.params.get("values", [])]
            out.append((src, c.isNotNull() & ~c.cast("string").isin(*vals)))
        elif rule.rule_type == "RANGE":
            cond = F.lit(False)
            if "min" in rule.params:
                cond = cond | (c < F.lit(rule.params["min"]))
            if "max" in rule.params:
                cond = cond | (c > F.lit(rule.params["max"]))
            out.append((src, c.isNotNull() & cond))
        else:  # pragma: no cover - the validator only admits the three supported types
            raise TransformConfigError(f"data-quality rule type {rule.rule_type} is not implemented")
    return out


def build_staged(plan: SilverPlan, bronze: DataFrame, ctx: BuildContext) -> Staged:
    specs = plan.columns or _passthrough_specs(bronze)
    for sp in specs:
        if sp.target.startswith("_"):
            raise TransformConfigError(f"target column '{sp.target}' uses the reserved '_' prefix")
    names = {s.source: i for i, s in enumerate(specs)}
    for bk in plan.business_keys:
        if bk not in names:
            raise TransformConfigError(f"business key '{bk}' of '{plan.object_code}' is not a mapped column")
    df = bronze.withColumn("_is_del", F.col("_op_type") == F.lit("D"))
    for i, s in enumerate(specs):
        pre, fin = apply_ops(F.col(f"`{s.source}`"), s.ops)
        df = df.withColumn(f"__p{i}", pre).withColumn(f"__v{i}", fin)
    for i, s in enumerate(specs):
        df = df.withColumn(f"__t{i}", F.expr(f"try_cast(`__v{i}` AS {s.type})") if s.type else F.col(f"__v{i}"))
    t = {s.source: F.col(f"__t{i}") for i, s in enumerate(specs)}

    errs: list[Column] = []
    fail_ct: dict[str, Column] = {}
    dq_cols: dict[str, str] = {}  # rule code -> sanitised helper column
    for bk in plan.business_keys:
        i = names[bk]
        is_str = (specs[i].type or "string").lower() == "string"
        errs.append(
            F.when(
                _is_blank(t[bk], is_str),
                F.struct(F.lit("NULL_BUSINESS_KEY").alias("reason"), F.lit(specs[i].target).alias("col")),
            )
        )
    cast_cond = F.lit(False)
    for i, s in enumerate(specs):
        if not s.type:
            continue
        bad = (~F.col("_is_del")) & F.col(f"__p{i}").isNotNull() & F.col(f"__t{i}").isNull()
        cast_cond = cast_cond | bad
        if plan.on_cast_error == "REJECT":
            errs.append(F.when(bad, F.struct(F.lit("CAST_ERROR").alias("reason"), F.lit(s.target).alias("col"))))
    df = df.withColumn("__cast_bad", cast_cond)
    for r in plan.dq_rules:
        for src, cond in _dq_failing(r, t):
            fail = (~F.col("_is_del")) & cond
            key = dq_cols.setdefault(r.code, f"{len(dq_cols)}")
            fail_ct[key] = fail if key not in fail_ct else (fail_ct[key] | fail)
            if r.action in ("QUARANTINE", "FAIL_BATCH"):
                errs.append(
                    F.when(fail, F.struct(F.lit(f"DQ_{r.code}").alias("reason"), F.lit(specs[names[src]].target).alias("col")))
                )
    err_arr = F.filter(F.array(*errs), lambda x: x.isNotNull()) if errs else F.array().cast(_ERR_TYPE)
    for k, c in fail_ct.items():
        df = df.withColumn(f"__dq_{k}", c)
    df = df.withColumn("__errs", err_arr).persist(StorageLevel.MEMORY_AND_DISK)

    aggs = [F.count(F.lit(1)).alias("rows_in"), F.sum(F.when(F.size("__errs") > 0, 1).otherwise(0)).alias("rows_rejected"),
            F.sum(F.when(F.col("__cast_bad"), 1).otherwise(0)).alias("cast_bad")]  # fmt: skip
    aggs += [F.sum(F.when(F.col(f"__dq_{k}"), 1).otherwise(0)).alias(f"dq_{k}") for k in fail_ct]
    agg = df.agg(*aggs).collect()[0].asDict()
    counters = {k: int(v or 0) for k, v in agg.items()}
    if plan.on_cast_error == "FAIL" and counters["cast_bad"] > 0:
        df.unpersist()
        raise TransformDataError(f"{counters['cast_bad']} rows have values that fail their cast and on_cast_error is FAIL",
                                 object=plan.object_code, rows=counters["cast_bad"])  # fmt: skip
    for r in plan.dq_rules:
        failed = counters.get(f"dq_{dq_cols[r.code]}", 0) if r.code in dq_cols else 0
        if r.action == "FAIL_BATCH" and counters["rows_in"] > 0 and failed > 0:
            pct = 100.0 * failed / counters["rows_in"]
            if pct > r.threshold_pct:
                df.unpersist()
                raise TransformDataError(f"data-quality rule {r.code} failed for {pct:.2f}% of rows (limit {r.threshold_pct}%)",
                                         object=plan.object_code, rule=r.code)  # fmt: skip

    # ---- rejects (pointers only) --------------------------------------------------------------------------------------
    rej = (df.where(F.size("__errs") > 0).select(F.col("_batch_id").alias("src_batch_id"), F.col("_batch_seq").alias("src_batch_seq"),
                                                  F.col("_row_hash").alias("row_hash"), F.explode("__errs").alias("e"))
           .select("src_batch_id", "src_batch_seq", "row_hash", F.col("e.reason").alias("reason"), F.coalesce(F.col("e.col"), F.lit("")).alias("column_name"))
           .groupBy("src_batch_id", "src_batch_seq", "row_hash", "reason", "column_name").agg(F.count(F.lit(1)).alias("occurrences"))
           .select(F.lit(ctx.object_code).alias("object_code"), "src_batch_id", F.col("src_batch_seq").cast("bigint"), "row_hash", "reason",
                   "column_name", "occurrences", F.lit(ctx.unit_batch_id).alias("unit_batch_id"),
                   F.lit(ctx.reject_ts).cast("timestamp").alias("reject_ts")))  # fmt: skip

    # ---- good rows ----------------------------------------------------------------------------------------------------
    good = df.where(F.size("__errs") == 0)
    if plan.delete_mode == "ignore":
        good = good.where(~F.col("_is_del"))
    eff: Column = F.col("_ingest_ts")
    if plan.effective_column:
        if plan.effective_column not in names:
            raise TransformConfigError(f"effective_column '{plan.effective_column}' is not a mapped column")
        eff = F.coalesce(F.col(f"__t{names[plan.effective_column]}").cast("timestamp"), F.col("_ingest_ts"))
    targets = [F.col(f"__t{i}").alias(s.target) for i, s in enumerate(specs)]
    base = good.select(*targets, F.when(F.col("_is_del"), F.lit("D")).otherwise(F.lit("U")).alias("_op"), eff.alias("_effective_ts"),
                       F.col("_batch_seq").cast("bigint").alias("_src_batch_seq"), F.col("_batch_id").alias("_src_batch_id"),
                       F.col("_op_seq"), F.col("_ingest_ts"), F.col("_row_hash"))  # fmt: skip
    tcols = [s.target for s in specs]
    base = base.withColumn("_carried", F.lit(False))
    if ctx.dedupe:
        key_cols = [plan.target_of(k) for k in plan.business_keys]
        order = [(F.col(plan.target_of(c)).desc() if d else F.col(plan.target_of(c)).asc()) for c, d in plan.dedupe_order]
        order += [F.col("_effective_ts").desc(), F.col("_src_batch_seq").desc(), F.col("_op_seq").desc_nulls_last(),
                  F.col("_ingest_ts").desc(), F.col("_row_hash").desc()]  # fmt: skip
        if plan.delete_mode == "soft":
            # A delete that wins inside the unit keeps the last values of the key seen earlier in the SAME unit, so the result does not
            # depend on where unit boundaries fall (the same events in two units give a soft-deleted row with the last values).
            order_asc = [(F.col(plan.target_of(c)).asc() if d else F.col(plan.target_of(c)).desc()) for c, d in plan.dedupe_order]
            order_asc += [F.col("_effective_ts").asc(), F.col("_src_batch_seq").asc(), F.col("_op_seq").asc_nulls_first(),
                          F.col("_ingest_ts").asc(), F.col("_row_hash").asc()]  # fmt: skip
            wv = Window.partitionBy(*key_cols).orderBy(*order_asc).rowsBetween(Window.unboundedPreceding, Window.currentRow)
            for tc in tcols:
                if tc not in key_cols:
                    base = base.withColumn(tc, F.when(F.col("_op") == "D", F.last(F.when(F.col("_op") == "U", F.col(tc)), True).over(wv))
                                           .otherwise(F.col(tc)))  # fmt: skip
            base = base.withColumn("_carried", (F.col("_op") == "D") & (F.count(F.when(F.col("_op") == "U", 1)).over(wv) > 0))
        w = Window.partitionBy(*key_cols).orderBy(*order)
        base = base.withColumn("__rn", F.row_number().over(w)).where(F.col("__rn") == 1).drop("__rn")
    base = base.withColumn("_content_hash", content_hash(tcols))
    out = base.select(*tcols, *TECH, "_carried", "_op_seq", "_ingest_ts", "_row_hash").persist(StorageLevel.MEMORY_AND_DISK)
    counters["rows_applied"] = out.count()
    counters["rows_deduped"] = counters["rows_in"] - counters["rows_rejected"] - counters["rows_applied"]
    return Staged(out, rej, tuple(specs), counters, df)


def check_reconciliation(counters: dict[str, Any], bronze_rows_expected: int, object_code: str) -> None:
    """rows read from Bronze must equal the sum of the Bronze receipts; applied + deduped + rejected must equal rows read."""
    from de_accelerator.core.errors import IntegrityError

    if counters["rows_in"] != bronze_rows_expected:
        raise IntegrityError(f"Bronze rows read ({counters['rows_in']}) differ from the Bronze receipts ({bronze_rows_expected})",
                             object=object_code)  # fmt: skip
    if counters["rows_applied"] + counters["rows_deduped"] + counters["rows_rejected"] != counters["rows_in"]:
        raise IntegrityError("unit counters do not reconcile (applied + deduped + rejected != rows in)", object=object_code)
