"""SCD Type 2 (doc 00 section 4.2): logical-change identity, run collapse, same-time changes, late arrivals, tombstones, verification.

Logical change identity (LCI) = (business key, _scd_hash, _effective_ts); `_sk` = 128-bit hash of the LCI (batch independent), so a
replay or a re-delivery of the same change in another batch is a duplicate and dropped. Versions of a key are ordered by
(_effective_ts, _src_batch_seq, _scd_hash). Consecutive identical states collapse (no new version); untracked columns of the current
version follow the newest row. A version older than the key's newest `_valid_from` is a late arrival: rejected (pointer row), never lost.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from de_accelerator.processing.common.columns import SilverPlan
from de_accelerator.processing.common.errors import VerificationError
from de_accelerator.processing.common.hashing import content_hash, hex128
from de_accelerator.processing.common.names import q
from de_accelerator.processing.silver import strategies as st

SCD2_TECH = ("_sk", "_scd_hash", "_valid_from", "_valid_to", "_is_current")
_STAGING = ("_op", "_op_seq", "_ingest_ts", "_row_hash")


@dataclass
class Scd2Result:
    applied: st.Applied
    late_rejects: DataFrame
    counters: dict[str, int]
    inserts: DataFrame | None = None
    s4: DataFrame | None = None
    persisted: list[DataFrame] = field(default_factory=list)

    def release(self) -> None:
        for df in self.persisted:
            try:
                df.unpersist()
            except Exception:  # noqa: BLE001, S110
                pass


def _ordc(seed_first: bool = True) -> Column:
    return F.struct(F.when(F.col("_is_seed"), 0).otherwise(1).alias("a"), "_effective_ts", "_src_batch_seq", "_scd_hash")


def prepare(plan: SilverPlan, staged: DataFrame, targets: list[str]) -> DataFrame:
    """Add `_scd_hash` and `_sk` (LCI) to staged rows and drop within-staging duplicates/conflicts (counted by the caller)."""
    keys = [plan.target_of(k) for k in plan.business_keys]
    tracked = [plan.target_of(c) for c in plan.scd_columns]
    s = staged.withColumn("_scd_hash", content_hash([*tracked, F.col("_op")]))
    s = s.withColumn("_sk", hex128([*keys, "_scd_hash", F.col("_effective_ts")]))
    order = [(F.col(plan.target_of(c)).desc() if d else F.col(plan.target_of(c)).asc()) for c, d in plan.dedupe_order]
    order += [F.col("_op_seq").desc_nulls_last(), F.col("_ingest_ts").desc(), F.col("_row_hash").desc()]
    w1 = Window.partitionBy(*keys, "_effective_ts", "_src_batch_seq").orderBy(*order)  # one state per key, time and Bronze batch
    s = s.withColumn("__rn", F.row_number().over(w1)).where("__rn = 1").drop("__rn")
    w2 = Window.partitionBy(*keys, "_sk").orderBy(F.col("_src_batch_seq").asc())  # exact repeats of one logical change
    return s.withColumn("__rn", F.row_number().over(w2)).where("__rn = 1").drop("__rn")


def apply_scd2(spark: SparkSession, ctx: st.WriteContext, plan: SilverPlan, staged: DataFrame, targets: list[str], object_code: str,
               unit_batch_id: str, reject_ts: Any) -> Scd2Result:  # fmt: skip
    keys = [plan.target_of(k) for k in plan.business_keys]
    s2 = prepare(plan, staged, targets).persist()
    persisted = [s2]
    n_staged_in = staged.count()
    n_s2 = s2.count()
    # schema first (creates the table / adds declared columns) from the full target layout
    layout = _layout(s2, targets)
    st.ensure_table(spark, ctx.fq, layout)
    t_all = spark.table(ctx.fq)
    touched = s2.select(*keys).distinct()
    tt = t_all.join(touched, keys, "left_semi")
    dup = s2.join(tt.select("_sk"), "_sk", "left_semi")
    s3 = s2.join(tt.select("_sk"), "_sk", "left_anti")
    tcur = tt.where(F.col("_is_current"))
    late = s3.join(tcur.select(*keys, F.col("_valid_from").alias("__cur_vf")), keys, "inner").where(
        F.col("_effective_ts") < F.col("__cur_vf")
    )
    # Frames below read the target table. Delta invalidates (and lazily recomputes) cached plans that read a table it just wrote, so the
    # pre-merge decision is cut from its lineage with a local checkpoint: verification must compare against THIS decision, not a re-derived one.
    s4 = s3.join(late.select("_sk").distinct(), "_sk", "left_anti").localCheckpoint(eager=True)
    persisted.append(s4)
    counters = {"duplicates_dropped": (n_staged_in - n_s2) + dup.count(), "late_rejected": late.count()}

    late_rej = (late.select(F.col("_src_batch_id").alias("src_batch_id"), F.col("_src_batch_seq").alias("src_batch_seq"),
                            F.col("_row_hash").alias("row_hash"))
                .groupBy("src_batch_id", "src_batch_seq", "row_hash").agg(F.count(F.lit(1)).alias("occurrences"))
                .select(F.lit(object_code).alias("object_code"), "src_batch_id", F.col("src_batch_seq").cast("bigint"), "row_hash",
                        F.lit("SCD2_LATE_ARRIVAL").alias("reason"), F.lit("").alias("column_name"), "occurrences",
                        F.lit(unit_batch_id).alias("unit_batch_id"), F.lit(reject_ts).cast("timestamp").alias("reject_ts")))  # fmt: skip

    late_rej = late_rej.localCheckpoint(eager=True)
    persisted.append(late_rej)

    common = [*targets, "_content_hash", "_effective_ts", "_src_batch_seq", "_src_batch_id", "_op", "_scd_hash", "_sk"]
    seed = tcur.select(*targets, "_content_hash", F.col("_valid_from").alias("_effective_ts"), "_src_batch_seq", "_src_batch_id",
                       F.when(F.col("_is_deleted"), "D").otherwise("U").alias("_op"), "_scd_hash", "_sk").withColumn("_is_seed", F.lit(True))  # fmt: skip
    new = s4.select(*common).withColumn("_is_seed", F.lit(False))
    u = seed.unionByName(new)
    w = Window.partitionBy(*keys).orderBy(
        F.when(F.col("_is_seed"), 0).otherwise(1), "_effective_ts", "_src_batch_seq", "_scd_hash"
    )
    u = u.withColumn("__prev", F.lag("_scd_hash").over(w))
    flag = F.when(F.col("_is_seed"), 0).when(F.col("__prev").isNull() | (F.col("__prev") != F.col("_scd_hash")), 1).otherwise(0)
    u = u.withColumn("__run", F.sum(flag).over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    u = u.persist()  # materialised: breaks the (wide) query plan here and freezes the pre-merge decision
    persisted.append(u)
    u.count()
    oc = _ordc()
    g = u.groupBy(*keys, "__run").agg(
        F.max(F.col("_is_seed").cast("int")).alias("__has_seed"),
        F.sum(F.when(~F.col("_is_seed"), 1).otherwise(0)).alias("__n_staged"),
        F.min_by("_effective_ts", oc).alias("__vf"), F.min_by("_sk", oc).alias("__sk"), F.min_by("_scd_hash", oc).alias("__hash"),
        F.min_by("_src_batch_seq", oc).alias("__seq"), F.min_by("_src_batch_id", oc).alias("__bid"), F.min_by("_content_hash", oc).alias("__first_ch"),
        F.max_by("_content_hash", oc).alias("__ch"), F.max_by("_op", oc).alias("__op"),
        *[F.max_by(c, oc).alias(f"__v_{c}") for c in targets])  # fmt: skip
    g = g.persist()
    persisted.append(g)
    g.count()
    wk = Window.partitionBy(*keys).orderBy("__run")
    g = g.withColumn("__seed_any", F.max("__has_seed").over(Window.partitionBy(*keys)))  # over ALL runs, including the seed run
    newv = g.where((F.col("__run") > 0) | (F.col("__has_seed") == 0))
    newv = newv.withColumn("__rn", F.row_number().over(wk))
    newv = newv.where(~((F.col("__op") == "D") & (F.col("__rn") == 1) & (F.col("__seed_any") == 0))).drop(
        "__rn", "__seed_any"
    )  # no tombstone for unknown keys
    newv = newv.withColumn("__vt", F.lead("__vf").over(wk))

    def build(src: DataFrame, valid_to: Column, current: Column, vf: str = "__vf") -> DataFrame:
        return src.select(*[F.col(f"__v_{c}").alias(c) for c in targets], F.col("__ch").alias("_content_hash"), F.col(vf).alias("_effective_ts"),
                          F.col("__seq").alias("_src_batch_seq"), F.col("__bid").alias("_src_batch_id"),
                          F.lit(ctx.batch_id).alias("_silver_batch_id"), F.lit(ctx.silver_ts).cast("timestamp").alias("_silver_ts"),
                          F.lit(ctx.snapshot_id).alias("_config_snapshot_id"), (F.col("__op") == "D").alias("_is_deleted"),
                          F.when(F.col("__op") == "D", F.col(vf)).alias("_deleted_ts"), F.col("__sk").alias("_sk"),
                          F.col("__hash").alias("_scd_hash"), F.col(vf).alias("_valid_from"), valid_to.alias("_valid_to"),
                          current.alias("_is_current"))  # fmt: skip

    inserts = build(newv, F.col("__vt"), F.col("__vt").isNull()).withColumn("_kind", F.lit("I"))
    first_new = newv.groupBy(*keys).agg(F.min("__vf").alias("__first_new_vf"))
    seed_run = g.where((F.col("__run") == 0) & (F.col("__has_seed") == 1)).join(first_new, keys, "left")
    changed = (F.col("__n_staged") > 0) & (F.col("__ch") != F.col("__first_ch"))
    seed_run = seed_run.where(changed | F.col("__first_new_vf").isNotNull())
    updates = build(seed_run, F.col("__first_new_vf"), F.col("__first_new_vf").isNull()).withColumn("_kind", F.lit("U"))
    final = inserts.unionByName(updates).localCheckpoint(eager=True)  # the decision, frozen before the merge (see above)
    persisted.append(final)
    ins_df = final.where(F.col("_kind") == "I").drop("_kind")
    upd_df = final.where(F.col("_kind") == "U").drop("_kind")
    counters.update(versions_inserted=ins_df.count(), versions_closed=upd_df.where(~F.col("_is_current")).count())
    merge_src = final.drop("_kind")
    cols = list(merge_src.columns)
    view = f"__scd2_{uuid.uuid4().hex}"
    merge_src.createOrReplaceTempView(view)
    set_cols = [c for c in cols if c != "_sk"]
    try:
        spark.sql(
            f"MERGE INTO {ctx.fq} t USING {view} s ON t._sk = s._sk "
            f"WHEN MATCHED THEN UPDATE SET {', '.join(f't.{q(c)} = s.{q(c)}' for c in set_cols)} "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(q(c) for c in cols)}) VALUES ({', '.join(f's.{q(c)}' for c in cols)})"
        )
    finally:
        spark.catalog.dropTempView(view)
    return Scd2Result(st.Applied(st.head_version(spark, ctx.fq)), late_rej, counters, ins_df, s4, persisted)


def _layout(s2: DataFrame, targets: list[str]) -> DataFrame:
    """An (empty) frame with the full SCD2 table layout, used to create/extend the table."""
    cols = [*targets, "_content_hash", "_effective_ts", "_src_batch_seq", "_src_batch_id", "_sk", "_scd_hash"]
    base = s2.select(*cols).limit(0)
    return (base.withColumn("_silver_batch_id", F.lit("").cast("string")).withColumn("_silver_ts", F.lit(None).cast("timestamp"))
            .withColumn("_config_snapshot_id", F.lit("").cast("string")).withColumn("_is_deleted", F.lit(False))
            .withColumn("_deleted_ts", F.lit(None).cast("timestamp")).withColumn("_valid_from", F.lit(None).cast("timestamp"))
            .withColumn("_valid_to", F.lit(None).cast("timestamp")).withColumn("_is_current", F.lit(True)))  # fmt: skip


def verify_scd2(spark: SparkSession, ctx: st.WriteContext, plan: SilverPlan, res: Scd2Result) -> tuple[int, str]:
    """Content-level verification (doc 00 section 8): staged versions present with equal hash/valid_from, chain integrity per touched
    key, newest version carries the newest staged content."""
    keys = [plan.target_of(k) for k in plan.business_keys]
    t = spark.table(ctx.fq)
    ins, s4_staged = res.inserts, res.s4
    assert ins is not None and s4_staged is not None
    missing = (
        ins.join(t.select("_sk", F.col("_scd_hash").alias("__t_hash"), F.col("_valid_from").alias("__t_vf")), "_sk", "left")
        .where(F.col("__t_hash").isNull() | (F.col("__t_hash") != F.col("_scd_hash")) | (F.col("__t_vf") != F.col("_valid_from")))
        .count()
    )
    touched = s4_staged.select(*keys).distinct().unionByName(ins.select(*keys).distinct()).distinct()
    tt = t.join(touched, keys, "left_semi")
    w = Window.partitionBy(*keys).orderBy("_valid_from", "_src_batch_seq", "_scd_hash")
    c = (
        tt.withColumn("__next", F.lead("_valid_from").over(w))
        .withColumn("__rn", F.row_number().over(w))
        .withColumn("__n", F.count(F.lit(1)).over(Window.partitionBy(*keys)))
    )
    last = F.col("__rn") == F.col("__n")
    bad_chain = c.where(
        (~last & ((F.col("_valid_to").isNull()) | (F.col("_valid_to") != F.col("__next")) | F.col("_is_current")))
        | (last & (F.col("_valid_to").isNotNull() | ~F.col("_is_current")))).count()  # fmt: skip
    # newest staged state per key must be the current version's state
    wn = Window.partitionBy(*keys).orderBy(
        F.col("_effective_ts").desc(), F.col("_src_batch_seq").desc(), F.col("_scd_hash").desc()
    )
    newest = s4_staged.withColumn("__r", F.row_number().over(wn)).where("__r = 1")
    cur = t.where(F.col("_is_current")).select(
        *keys, F.col("_content_hash").alias("__t_ch"), F.col("_is_deleted").alias("__t_del")
    )
    mism = newest.join(cur, keys, "left").where(
        ((F.col("_op") == "U") & (F.col("__t_ch").isNull() | (F.col("__t_ch") != F.col("_content_hash")) | F.col("__t_del")))
        | ((F.col("_op") == "D") & F.col("__t_ch").isNotNull() & ~F.col("__t_del"))).count()  # fmt: skip
    if missing or bad_chain or mism:
        raise VerificationError(f"SCD2 verification failed for unit {ctx.batch_id}: {missing} staged versions missing/different, "
                                f"{bad_chain} chain violations, {mism} keys whose current version differs from the newest staged row",
                                batch_id=ctx.batch_id, missing=missing, chain=bad_chain, current=mism)  # fmt: skip
    return st.checksum_of(ins)
