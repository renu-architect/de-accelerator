"""Standard Bronze audit columns (doc 03 §3). All names start with `_`; source columns may not."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, TimestampType

from de_accelerator.core.errors import DataError

# columns a connector may supply itself (per-row source metadata)
CONNECTOR_META = ("_op_type", "_op_seq", "_source_file", "_source_file_mod_ts")
AUDIT_COLUMNS = (
    "_batch_id", "_batch_seq", "_stream_key", "_batch_type", "_execution_id", "_config_snapshot_id", "_release_id", "_ingest_ts",
    "_source_system", "_source_object", "_source_file", "_source_file_mod_ts", "_op_type", "_op_seq", "_row_hash", "_schema_hash",
    "_framework_version",
)  # fmt: skip


@dataclass(frozen=True)
class AuditValues:
    batch_id: str
    batch_seq: int
    stream_key: str
    batch_type: str
    execution_id: str
    snapshot_id: str
    release_id: str
    ingest_ts: datetime
    source_system: str
    source_object: str
    schema_hash: str
    framework_version: str
    default_op_type: str  # S snapshot/full, C changed (incremental/files/api)


def q(name: str) -> Column:
    return F.col(f"`{name}`")


def raw_columns(df: DataFrame) -> list[str]:
    return [c for c in df.columns if c not in CONNECTOR_META]


def check_no_reserved(df: DataFrame) -> None:
    bad = [c for c in raw_columns(df) if c.startswith("_")]
    if bad:
        raise DataError(f"source columns collide with reserved audit-column naming ('_' prefix): {bad}", columns=bad)


def add_audit_columns(df: DataFrame, a: AuditValues) -> DataFrame:
    """Return raw columns (original order/names/types, untouched) + audit columns."""
    check_no_reserved(df)
    raw = raw_columns(df)
    present = set(df.columns)
    out = df.select(*[q(c) for c in raw],
                    *[q(c) for c in CONNECTOR_META if c in present])  # fmt: skip
    cols: dict[str, Column] = {
        "_batch_id": F.lit(a.batch_id), "_batch_seq": F.lit(a.batch_seq).cast(LongType()), "_stream_key": F.lit(a.stream_key),
        "_batch_type": F.lit(a.batch_type), "_execution_id": F.lit(a.execution_id), "_config_snapshot_id": F.lit(a.snapshot_id),
        "_release_id": F.lit(a.release_id), "_ingest_ts": F.lit(a.ingest_ts).cast(TimestampType()),
        "_source_system": F.lit(a.source_system), "_source_object": F.lit(a.source_object),
        "_row_hash": F.xxhash64(*[q(c) for c in raw]) if raw else F.lit(0).cast(LongType()),
        "_schema_hash": F.lit(a.schema_hash), "_framework_version": F.lit(a.framework_version),
    }  # fmt: skip
    defaults: dict[str, Column] = {
        "_op_type": F.lit(a.default_op_type), "_op_seq": F.lit(None).cast(StringType()),
        "_source_file": F.lit(None).cast(StringType()), "_source_file_mod_ts": F.lit(None).cast(TimestampType()),
    }  # fmt: skip
    for name, col in defaults.items():
        if name not in present:
            out = out.withColumn(name, col)
    for name, col in cols.items():
        out = out.withColumn(name, col)
    # stable, documented order: raw columns first, then audit columns in AUDIT_COLUMNS order
    return out.select(*[q(c) for c in raw], *[q(c) for c in AUDIT_COLUMNS])
