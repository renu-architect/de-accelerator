"""Silver write strategies: table management, APPEND and SCD1 with content-level verification (doc 00 section 8).

Recovery model: merges are data-idempotent (ordering guard + content hash), so a replay after a crash is a data no-op; the unit
is only receipted after `verify_*` proves the target holds the staged content. Row counts are never used as proof of content.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from de_accelerator.processing.common.columns import SilverPlan
from de_accelerator.processing.common.errors import TransformConfigError, VerificationError
from de_accelerator.processing.common.names import q

#: technical columns present in every Silver table (SCD2 adds more in processing/silver/scd2.py)
SILVER_TECH = ("_content_hash", "_effective_ts", "_src_batch_seq", "_src_batch_id", "_silver_batch_id", "_silver_ts",
               "_config_snapshot_id", "_is_deleted", "_deleted_ts")  # fmt: skip
STAGING_ONLY = ("_op", "_carried", "_op_seq", "_ingest_ts", "_row_hash")


@dataclass(frozen=True)
class WriteContext:
    fq: str
    batch_id: str
    batch_seq: int
    silver_ts: datetime
    snapshot_id: str
    txn_app_id: str


@dataclass
class Applied:
    delta_version: int | None
    checksum: str | None = None
    rows_written: int = 0


def finalize(df: DataFrame, ctx: WriteContext) -> DataFrame:
    """Add the Silver technical columns to staged rows (keeps `_op` and the ordering helpers until the strategy is done)."""
    return (df.withColumn("_silver_batch_id", F.lit(ctx.batch_id)).withColumn("_silver_ts", F.lit(ctx.silver_ts).cast("timestamp"))
            .withColumn("_config_snapshot_id", F.lit(ctx.snapshot_id))
            .withColumn("_is_deleted", F.col("_op") == F.lit("D"))
            .withColumn("_deleted_ts", F.when(F.col("_op") == F.lit("D"), F.col("_effective_ts"))))  # fmt: skip


def table_columns(df: DataFrame) -> list[str]:
    return [c for c in df.columns if c not in STAGING_ONLY]


# ---- table management ----------------------------------------------------------------------------------------------------
def ensure_table(spark: SparkSession, fq: str, df: DataFrame) -> None:
    """Create the Silver table from the staged schema; add newly declared columns (nullable ADD COLUMN); any other schema change
    (type change, narrowing) fails the unit as a CONFIG error: M3 supports ADD COLUMN only."""
    cols = table_columns(df)
    if not spark.catalog.tableExists(fq):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq.rsplit('.', 1)[0]}")
        df.select(*cols).limit(0).writeTo(fq).using("delta").create()
        return
    existing = {f.name: f for f in spark.table(fq).schema.fields}
    staged = {f.name: f for f in df.select(*cols).schema.fields}
    for name, f in staged.items():
        if name in existing and existing[name].dataType.simpleString() != f.dataType.simpleString():
            raise TransformConfigError(f"column '{name}' changed type {existing[name].dataType.simpleString()} -> "
                                       f"{f.dataType.simpleString()}: not supported (M3 supports ADD COLUMN only)", column=name)  # fmt: skip
    new = [n for n in staged if n not in existing]
    if new:
        spark.sql(f"ALTER TABLE {fq} ADD COLUMNS ({', '.join(f'{q(n)} {staged[n].dataType.simpleString()}' for n in new)})")


def head_version(spark: SparkSession, fq: str) -> int | None:
    try:
        return int(DeltaTable.forName(spark, fq).history(1).collect()[0]["version"])
    except Exception:  # noqa: BLE001 - informational only
        return None


def _checksum(df: DataFrame) -> DataFrame:
    """Order-independent content checksum: row count + sum of the first 60 bits of every `_content_hash`."""
    return df.agg(F.count(F.lit(1)).alias("n"),
                  F.coalesce(F.sum(F.conv(F.substring("_content_hash", 1, 15), 16, 10).cast("decimal(38,0)")), F.lit(0)).cast("string").alias("s"))  # fmt: skip


def checksum_of(df: DataFrame) -> tuple[int, str]:
    r = _checksum(df).collect()[0]
    return int(r["n"]), str(r["s"])


# ---- APPEND ----------------------------------------------------------------------------------------------------------------
def apply_append(spark: SparkSession, ctx: WriteContext, staged_final: DataFrame) -> Applied:
    ensure_table(spark, ctx.fq, staged_final)
    cols = table_columns(staged_final)
    (staged_final.select(*cols).write.format("delta").mode("append").option("txnAppId", ctx.txn_app_id)
     .option("txnVersion", ctx.batch_seq).saveAsTable(ctx.fq))  # fmt: skip
    return Applied(head_version(spark, ctx.fq))


def verify_append(spark: SparkSession, ctx: WriteContext, staged_final: DataFrame) -> tuple[int, str]:
    exp_n, exp_s = checksum_of(staged_final)
    got_n, got_s = checksum_of(spark.table(ctx.fq).where(F.col("_silver_batch_id") == ctx.batch_id))
    if (exp_n, exp_s) != (got_n, got_s):
        raise VerificationError(f"APPEND verification failed for unit {ctx.batch_id}: staged {exp_n} rows / checksum {exp_s}, "
                                f"target holds {got_n} rows / checksum {got_s}", batch_id=ctx.batch_id)  # fmt: skip
    return exp_n, exp_s


# ---- SCD1 ------------------------------------------------------------------------------------------------------------------
def _newer(s: str = "s", t: str = "t") -> str:
    return f"({s}._effective_ts > {t}._effective_ts OR ({s}._effective_ts = {t}._effective_ts AND {s}._src_batch_seq > {t}._src_batch_seq))"


def apply_scd1(spark: SparkSession, ctx: WriteContext, staged_final: DataFrame, plan: SilverPlan) -> Applied:
    ensure_table(spark, ctx.fq, staged_final)
    cols = table_columns(staged_final)
    keys = [plan.target_of(k) for k in plan.business_keys]
    view = f"__stg_{uuid.uuid4().hex}"
    staged_final.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{q(k)} <=> s.{q(k)}" for k in keys)
    set_all = ", ".join(f"t.{q(c)} = s.{q(c)}" for c in cols)
    tombstone = ", ".join(f"t.{q(c)} = s.{q(c)}" for c in ("_effective_ts", "_src_batch_seq", "_src_batch_id", "_silver_batch_id", "_silver_ts",
                                                           "_config_snapshot_id", "_is_deleted", "_deleted_ts", "_content_hash"))  # fmt: skip
    ins_cols = ", ".join(q(c) for c in cols)
    ins_vals = ", ".join(f"s.{q(c)}" for c in cols)
    clauses = []
    if plan.delete_mode == "hard":
        clauses.append(f"WHEN MATCHED AND s._op = 'D' AND {_newer()} THEN DELETE")
    else:
        clauses.append(f"WHEN MATCHED AND s._op = 'D' AND {_newer()} AND NOT t._is_deleted THEN UPDATE SET {tombstone}")
    clauses.append(
        f"WHEN MATCHED AND s._op = 'U' AND {_newer()} AND (t._content_hash <> s._content_hash OR t._is_deleted) THEN UPDATE SET {set_all}"
    )
    clauses.append(f"WHEN NOT MATCHED AND s._op = 'U' THEN INSERT ({ins_cols}) VALUES ({ins_vals})")
    if plan.delete_mode != "hard":  # a key inserted and deleted inside one unit becomes a soft-deleted row (same as across two units)
        clauses.append(f"WHEN NOT MATCHED AND s._op = 'D' AND s._carried THEN INSERT ({ins_cols}) VALUES ({ins_vals})")
    try:
        spark.sql(f"MERGE INTO {ctx.fq} t USING {view} s ON {on} " + " ".join(clauses))
    finally:
        spark.catalog.dropTempView(view)
    return Applied(head_version(spark, ctx.fq))


def verify_scd1(spark: SparkSession, ctx: WriteContext, staged_final: DataFrame, plan: SilverPlan) -> tuple[int, str]:
    """Every staged key (final state of the unit) must be in the target with the staged content, or have been superseded by a
    strictly newer row; deletes must be visible; no key may appear twice."""
    keys = [plan.target_of(k) for k in plan.business_keys]
    t = spark.table(ctx.fq).alias("t")
    s = staged_final.alias("s")
    cond = [F.col(f"s.{q(k)}").eqNullSafe(F.col(f"t.{q(k)}")) for k in keys]
    j = s.join(t, cond, "left")
    t_newer = (F.col("t._effective_ts") > F.col("s._effective_ts")) | (
        (F.col("t._effective_ts") == F.col("s._effective_ts")) & (F.col("t._src_batch_seq") > F.col("s._src_batch_seq"))
    )
    # identical content is what matters: an unchanged row re-delivered by a later Bronze batch (incremental overlap, full reloads) is
    # correctly NOT rewritten by the merge, so the target keeps the earlier batch sequence / effective time
    same = F.col("t._content_hash") == F.col("s._content_hash")
    present = F.col(f"t.{q(keys[0])}").isNotNull()
    upsert_ok = (F.col("s._op") == "U") & present & ((same & ~F.col("t._is_deleted")) | t_newer)
    if plan.delete_mode == "hard":
        delete_ok = (F.col("s._op") == "D") & (~present | t_newer)
    else:
        # a delete of an already-deleted key is a no-op for the merge (the earlier tombstone stays), so deleted-or-superseded is the proof
        delete_ok = (F.col("s._op") == "D") & ((present & (F.col("t._is_deleted") | t_newer)) | (~present & ~F.col("s._carried")))
    bad = j.where(~(upsert_ok | delete_ok)).count()
    dup = (
        spark.table(ctx.fq)
        .join(s.select(*[F.col(q(k)) for k in keys]).distinct(), keys, "left_semi")
        .groupBy(*[F.col(q(k)) for k in keys])
        .count()
        .where("count > 1")
        .count()
    )
    if bad or dup:
        raise VerificationError(f"SCD1 verification failed for unit {ctx.batch_id}: {bad} staged keys not reflected in the target, "
                                f"{dup} duplicated keys", batch_id=ctx.batch_id, missing=bad, duplicated=dup)  # fmt: skip
    return checksum_of(staged_final)


def schema_of(df: DataFrame) -> StructType:
    return df.schema
