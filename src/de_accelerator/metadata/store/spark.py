"""Spark/Delta TableStore. Works locally (flavor LOCAL, two-level names) and on Databricks (UC three-level names)."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from de_accelerator.core.errors import StoreError
from de_accelerator.core.naming import qualified_table
from de_accelerator.metadata.schema.tables import table_map
from de_accelerator.metadata.schema.tablespec import TableSpec, render_create, render_drop
from de_accelerator.metadata.store.base import Row

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession


def build_local_spark(warehouse_dir: str, app: str = "de-accelerator-local") -> SparkSession:
    """Local Spark session with Delta (for tests/CLI). Requires the `spark` extra."""
    import os
    import sys

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable  # workers must match the driver interpreter
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse_dir}/_derby")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


_SPARK_TYPES = {"str": "string", "int": "bigint", "float": "double", "bool": "boolean", "list": "array<string>",
                "json": "string", "ts": "timestamp", "bin": "binary"}  # fmt: skip


def _lit(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int | float):
        return repr(v)
    if isinstance(v, dt.datetime):
        return f"TIMESTAMP '{v.astimezone(dt.timezone.utc):%Y-%m-%d %H:%M:%S.%f}'"  # noqa: UP017
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _cond(where: Mapping[str, Any] | None) -> str:
    if not where:
        return "TRUE"
    return " AND ".join(f"`{k}` IS NULL" if v is None else f"`{k}` = {_lit(v)}" for k, v in where.items())


class SparkDeltaTableStore:
    def __init__(self, spark: SparkSession, catalog: str | None = None, flavor: str = "LOCAL") -> None:
        self.spark = spark
        self.catalog = catalog
        self.flavor = flavor
        self._specs = table_map()

    # --- naming / specs
    def _spec(self, table: str) -> TableSpec:
        try:
            return self._specs[table]
        except KeyError:
            raise StoreError(f"unknown table (no spec): {table}") from None

    def _fq(self, table: str) -> str:
        s = self._spec(table)
        return qualified_table(s.schema, s.name, self.catalog if self.flavor == "DATABRICKS" else None)

    # --- DDL
    def create_table(self, spec: TableSpec) -> None:
        self._specs.setdefault(spec.name, spec)
        if self.flavor == "DATABRICKS" and self.catalog:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{spec.schema}")
        else:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {spec.schema}")
        self.spark.sql(render_create(spec, self.catalog, self.flavor))

    def drop_table(self, spec: TableSpec) -> None:
        self.spark.sql(render_drop(spec, self.catalog, self.flavor))

    def table_exists(self, name: str) -> bool:
        return bool(self.spark.catalog.tableExists(self._fq(name)))

    # --- DML
    def _df(self, spec: TableSpec, rows: Sequence[Row]) -> Any:
        ddl = ", ".join(f"`{c.name}` {_SPARK_TYPES[c.type]}" for c in spec.columns)
        data = []
        for r in rows:
            unknown = set(r) - set(spec.column_names)
            if unknown:
                raise StoreError(f"{spec.name}: unknown columns {sorted(unknown)}")
            data.append(tuple(r.get(c.name) for c in spec.columns))
        return self.spark.createDataFrame(data, schema=ddl)

    def insert(self, table: str, rows: Sequence[Row]) -> int:
        if not rows:
            return 0
        spec = self._spec(table)
        self._df(spec, rows).write.format("delta").mode("append").saveAsTable(self._fq(table))
        return len(rows)

    def insert_if_absent(self, table: str, rows: Sequence[Row], key_cols: Sequence[str]) -> int:
        if not rows:
            return 0
        from delta.tables import DeltaTable

        spec = self._spec(table)
        for r in rows:
            if any(r.get(k) is None for k in key_cols):
                raise StoreError(f"{table}: NULL in merge key {list(key_cols)}")
        src = self._df(spec, rows).dropDuplicates(list(key_cols))
        cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in key_cols)
        target = DeltaTable.forName(self.spark, self._fq(table))
        before = target.history(1).collect()[0]["version"]
        target.alias("t").merge(src.alias("s"), cond).whenNotMatchedInsertAll().execute()
        hist = target.history(1).collect()[0]
        if hist["version"] == before:  # a MERGE that changes nothing creates no commit
            return 0
        return int((hist["operationMetrics"] or {}).get("numTargetRowsInserted", 0))

    def select(self, table: str, where: Mapping[str, Any] | None = None) -> list[Row]:
        df = self.spark.table(self._fq(table))
        if where:
            df = df.where(_cond(where))
        out: list[Row] = []
        for r in df.collect():
            d = r.asDict(recursive=True)
            for k, v in d.items():
                if isinstance(v, bytearray):
                    d[k] = bytes(v)
            out.append(d)
        return out

    def update_where(self, table: str, values: Mapping[str, Any], where: Mapping[str, Any]) -> int:
        sets = ", ".join(f"`{k}` = {_lit(v)}" for k, v in values.items())
        res = self.spark.sql(f"UPDATE {self._fq(table)} SET {sets} WHERE {_cond(where)}").collect()
        return int(res[0]["num_affected_rows"]) if res else 0

    def delete_where(self, table: str, where: Mapping[str, Any]) -> int:
        res = self.spark.sql(f"DELETE FROM {self._fq(table)} WHERE {_cond(where)}").collect()
        return int(res[0]["num_affected_rows"]) if res else 0

    def count(self, table: str, where: Mapping[str, Any] | None = None) -> int:
        df = self.spark.table(self._fq(table))
        return int(df.where(_cond(where)).count()) if where else int(df.count())
