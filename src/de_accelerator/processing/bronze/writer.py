"""Bronze writer: audit columns, schema policy, idempotent Delta append, commit verification (doc 03/04)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame, Observation, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

from de_accelerator.core.errors import IntegrityError, SchemaChangeError, TransientSourceError
from de_accelerator.core.naming import qualified_table
from de_accelerator.metadata.store.spark import is_delta_conflict
from de_accelerator.processing.bronze.audit_columns import AuditValues, add_audit_columns, check_no_reserved, q, raw_columns
from de_accelerator.processing.bronze.schema_policy import SchemaDecision, decide

TABLE_PROPERTIES = {"delta.columnMapping.mode": "name", "delta.minReaderVersion": "2", "delta.minWriterVersion": "5"}


def table_name(target: str, catalog: str | None, flavor: str) -> str:
    """`bronze.<table>` (from layer_mapping/target_bronze) -> `<catalog>.bronze.<table>` (DATABRICKS) or `bronze.<table>` (LOCAL)."""
    parts = target.split(".")
    if len(parts) != 2 or parts[0] != "bronze":
        raise ValueError(f"Bronze target must be 'bronze.<table>', got '{target}'")
    return qualified_table("bronze", parts[1], catalog if flavor == "DATABRICKS" else None)


@dataclass
class WriteResult:
    rows_observed: int
    decision: SchemaDecision
    schema_json: str


@dataclass
class Verification:
    delta_version: int | None
    rows_in_commit: int | None
    rows_counted: int
    commit_tag: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


class BronzeWriter:
    def __init__(self, spark: SparkSession, catalog: str | None, flavor: str) -> None:
        self.spark, self.catalog, self.flavor = spark, catalog, flavor

    def fq(self, target: str) -> str:
        return table_name(target, self.catalog, self.flavor)

    # ---- table ----------------------------------------------------------------------------------------------------
    def exists(self, fq: str) -> bool:
        return bool(self.spark.catalog.tableExists(fq))

    def existing_schema(self, fq: str) -> StructType | None:
        return self.spark.table(fq).schema if self.exists(fq) else None

    def _create_empty(self, fq: str, df: DataFrame) -> None:
        schema_name = fq.rsplit(".", 1)[0]
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        w = df.limit(0).writeTo(fq).using("delta")
        for k, v in TABLE_PROPERTIES.items():
            w = w.tableProperty(k, v)
        w.create()

    # ---- write ----------------------------------------------------------------------------------------------------
    def write(self, df: DataFrame, target: str, a: AuditValues, mode: str, txn_app_id: str, manifest_hash: str) -> WriteResult:
        check_no_reserved(df)
        fq = self.fq(target)
        raw_schema = StructType([f for f in df.schema.fields if f.name in set(raw_columns(df))])
        existing = self.existing_schema(fq)
        decision = decide(raw_schema, existing, mode)

        out = add_audit_columns(df, a)
        if decision.cast_to_table:
            out = out.select(*[F.col(q_name(c)).cast(decision.cast_to_table[c]).alias(c) if c in decision.cast_to_table else F.col(q_name(c))
                               for c in out.columns])  # fmt: skip
        if mode == "RESCUE":
            out = self._apply_rescue(out, decision, existing)
        if existing is None:
            self._create_empty(fq, out)
        else:
            self._widen_table(fq, decision)

        obs = Observation(f"bronze_{a.batch_id}")
        meta = json.dumps({"batch_id": a.batch_id, "seq": a.batch_seq, "stream": a.stream_key, "manifest_hash": manifest_hash},
                          sort_keys=True)  # fmt: skip
        w = (out.observe(obs, F.count(F.lit(1)).alias("rows")).write.format("delta").mode("append")
             .option("txnAppId", txn_app_id).option("txnVersion", a.batch_seq).option("userMetadata", meta))  # fmt: skip
        if decision.needs_merge_schema or (mode == "RESCUE" and existing is not None):
            w = w.option("mergeSchema", "true")
        try:
            w.saveAsTable(fq)
        except Exception as exc:
            if is_delta_conflict(exc):
                raise TransientSourceError(f"Delta commit conflict writing {fq}: {exc}") from exc
            raise
        rows = int(obs.get.get("rows", 0))
        return WriteResult(rows, decision, json.dumps([[f.name, f.dataType.simpleString()] for f in raw_schema.fields]))

    def _apply_rescue(self, out: DataFrame, d: SchemaDecision, existing: StructType | None) -> DataFrame:
        bad = list(d.rescue)
        keep_cols = [c for c in out.columns if c not in bad]
        rescued = (F.to_json(F.struct(*[q(c) for c in bad])) if bad else F.lit(None).cast(StringType())).alias("_rescued_data")
        res = out.select(*[q(c) for c in keep_cols], rescued)
        if existing is not None:  # keep rescued columns present (as null) when they exist in the table
            ex = {f.name: f.dataType for f in existing.fields}
            for c in bad:
                if c in ex:
                    res = res.withColumn(c, F.lit(None).cast(ex[c]))
        return res

    def _widen_table(self, fq: str, d: SchemaDecision) -> None:
        if not d.widen_table:
            return
        try:  # Delta type widening: Databricks runtimes only (verification pending); fall back to a clear error
            self.spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
            for col, t in d.widen_table.items():
                self.spark.sql(f"ALTER TABLE {fq} ALTER COLUMN `{col}` TYPE {t.simpleString()}")
        except Exception as exc:
            raise SchemaChangeError(f"type widening is not supported on this runtime ({type(exc).__name__}); "
                                    "use schema_evolution_mode FAIL/RESCUE or upgrade the runtime", columns=list(d.widen_table)) from exc  # fmt: skip

    # ---- verification ---------------------------------------------------------------------------------------------
    def verify(self, target: str, batch_id: str, stream_key: str, batch_seq: int) -> Verification | None:
        """Find proof that this batch is committed: history entry carrying the batch id + count by (_stream_key,_batch_seq).
        Returns None when no trace of the batch exists. Raises IntegrityError when commit rows and counted rows disagree."""
        fq = self.fq(target)
        if not self.exists(fq):
            return None
        from delta.tables import DeltaTable

        hist = [r for r in DeltaTable.forName(self.spark, fq).history().collect() if r["userMetadata"] and batch_id in r["userMetadata"]]
        counted = int(self.spark.table(fq).where((F.col("_stream_key") == stream_key) & (F.col("_batch_seq") == batch_seq)).count())
        if not hist:
            return Verification(None, None, counted) if counted > 0 else None
        h = max(hist, key=lambda r: r["version"])
        rows = int((h["operationMetrics"] or {}).get("numOutputRows", counted))
        if rows != counted:
            raise IntegrityError(f"Bronze commit v{h['version']} reports {rows} rows but {counted} rows carry batch {batch_id}",
                                 batch_id=batch_id, commit_rows=rows, counted=counted)  # fmt: skip
        return Verification(int(h["version"]), rows, counted, h["userMetadata"])


def q_name(name: str) -> str:
    return f"`{name}`"
