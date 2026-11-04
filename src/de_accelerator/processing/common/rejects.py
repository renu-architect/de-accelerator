"""Reject tables `dq.rej_<object>`: pointers only, idempotent, never business keys, failing values or payload.

Privacy: `row_hash` is Bronze's un-keyed 64-bit `_row_hash` (M2). It is NOT a privacy guarantee; reject tables are restricted
pseudonymous data (see docs/architecture/m3/00_M3_design.md section 4). Identity: (src_batch_id, row_hash, reason, column_name).
"""

from __future__ import annotations

import uuid

from pyspark.sql import DataFrame, SparkSession

from de_accelerator.processing.common.names import reject_table

DDL = ("object_code STRING, src_batch_id STRING, src_batch_seq BIGINT, row_hash BIGINT, reason STRING, column_name STRING, "
       "occurrences BIGINT, unit_batch_id STRING, reject_ts TIMESTAMP")  # fmt: skip
COLUMNS = (
    "object_code",
    "src_batch_id",
    "src_batch_seq",
    "row_hash",
    "reason",
    "column_name",
    "occurrences",
    "unit_batch_id",
    "reject_ts",
)
_ON = "t.src_batch_id <=> s.src_batch_id AND t.row_hash <=> s.row_hash AND t.reason <=> s.reason AND t.column_name <=> s.column_name"


class RejectWriter:
    def __init__(self, spark: SparkSession, catalog: str | None, flavor: str) -> None:
        self.spark, self.catalog, self.flavor = spark, catalog, flavor

    def fq(self, object_code: str) -> str:
        return reject_table(object_code, self.catalog, self.flavor)

    def _ensure(self, fq: str) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq.rsplit('.', 1)[0]}")
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {fq} ({DDL}) USING DELTA")

    def write(self, object_code: str, rejects: DataFrame) -> None:
        """Insert-if-absent by reject identity: replaying a unit never duplicates a reject row."""
        fq = self.fq(object_code)
        self._ensure(fq)
        view = f"__rej_{uuid.uuid4().hex}"
        rejects.select(*COLUMNS).createOrReplaceTempView(view)
        try:
            self.spark.sql(f"MERGE INTO {fq} t USING {view} s ON {_ON} WHEN NOT MATCHED THEN INSERT *")
        finally:
            self.spark.catalog.dropTempView(view)

    def missing(self, object_code: str, rejects: DataFrame) -> int:
        """Content verification: staged reject identities absent from the table (must be 0 after write)."""
        t = self.spark.table(self.fq(object_code))
        s = rejects.select(*COLUMNS)
        cond = [s.src_batch_id.eqNullSafe(t.src_batch_id), s.row_hash.eqNullSafe(t.row_hash), s.reason.eqNullSafe(t.reason),
                s.column_name.eqNullSafe(t.column_name)]  # fmt: skip
        return int(s.join(t, cond, "left_anti").count())
