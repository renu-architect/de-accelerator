"""Bronze writer on real local Delta: raw preservation, audit columns, idempotent replay, verification, schema evolution."""

import uuid
from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F

from de_accelerator.core.errors import DataError, IntegrityError, SchemaChangeError
from de_accelerator.processing.bronze.audit_columns import AUDIT_COLUMNS, AuditValues
from de_accelerator.processing.bronze.writer import BronzeWriter

pytestmark = pytest.mark.component


@pytest.fixture
def bw(spark):
    return BronzeWriter(spark, None, "LOCAL")


@pytest.fixture
def target(spark):
    name = f"t_{uuid.uuid4().hex[:8]}"
    yield f"bronze.{name}"
    spark.sql(f"DROP TABLE IF EXISTS bronze.{name}")


def audit(seq=1, stream="live", batch=None, op="C"):
    return AuditValues(batch or f"batch-{stream}-{seq}", seq, stream, "LIVE", "run-1", "snap1", "rel1",
                       datetime(2026, 11, 3, 12, 0, tzinfo=timezone.utc), "erp", "dbo.Orders", "schemahash", "0.1.0", op)  # fmt: skip


def write(bw, spark, target, rows, ddl="id bigint, amount decimal(10,2), `my col` string", seq=1, mode="ADD_COLUMNS", **kw):
    df = spark.createDataFrame(rows, ddl)
    a = audit(seq, **{k: v for k, v in kw.items() if k in ("stream", "batch", "op")})
    return bw.write(df, target, a, mode, f"orders:{a.stream_key}:BRONZE", "mh"), a


def test_raw_preserved_and_audit_columns(bw, spark, target):
    from decimal import Decimal

    res, a = write(bw, spark, target, [(1, Decimal("10.50"), " keep  spaces "), (2, None, None)])
    assert res.rows_observed == 2 and res.decision.change_type == "CREATED"
    df = spark.table(target)
    assert df.columns[:3] == ["id", "amount", "my col"] and tuple(df.columns[3:]) == AUDIT_COLUMNS  # raw first, audit after
    r = df.where("id = 1").collect()[0]
    assert r["my col"] == " keep  spaces " and str(r["amount"]) == "10.50"  # no trimming / cast: raw preserved
    assert r["_batch_id"] == a.batch_id and r["_batch_seq"] == 1 and r["_stream_key"] == "live" and r["_op_type"] == "C"
    assert r["_execution_id"] == "run-1" and r["_config_snapshot_id"] == "snap1" and r["_source_system"] == "erp"
    assert r["_source_file"] is None and r["_op_seq"] is None and r["_framework_version"] == "0.1.0" and r["_row_hash"] is not None
    assert df.select("_ingest_ts").distinct().count() == 1
    props = spark.sql(f"SHOW TBLPROPERTIES {target}").collect()
    assert ("delta.columnMapping.mode", "name") in [(p["key"], p["value"]) for p in props]


def test_connector_supplied_row_metadata_is_kept(bw, spark, target):
    df = spark.createDataFrame([(1, "I", "0001"), (2, "D", "0002")], "id bigint, _op_type string, _op_seq string")
    bw.write(df, target, audit(op="S"), "ADD_COLUMNS", "orders:live:BRONZE", "mh")
    assert {(r["id"], r["_op_type"], r["_op_seq"]) for r in spark.table(target).collect()} == {(1, "I", "0001"), (2, "D", "0002")}


def test_idempotent_replay_same_txn_version(bw, spark, target):
    write(bw, spark, target, [(1, None, "a"), (2, None, "b")])
    assert spark.table(target).count() == 2
    write(bw, spark, target, [(1, None, "a"), (2, None, "b")])  # crash-retry of the SAME batch (same txnVersion)
    assert spark.table(target).count() == 2  # Delta skipped the replay: no duplicates
    write(bw, spark, target, [(3, None, "c")], seq=2)
    assert spark.table(target).count() == 3
    write(bw, spark, target, [(9, None, "stale")], seq=1)  # stale lower version is also skipped
    assert spark.table(target).count() == 3
    assert {r["_batch_seq"] for r in spark.table(target).collect()} == {1, 2}


def test_streams_have_independent_idempotency(bw, spark, target):
    write(bw, spark, target, [(1, None, "a")], seq=1)
    write(bw, spark, target, [(2, None, "b")], seq=1, stream="backfill-1")  # same seq, different stream/appId => written
    assert spark.table(target).count() == 2


def test_verify_finds_commit_and_counts(bw, spark, target):
    res, a = write(bw, spark, target, [(1, None, "a"), (2, None, "b"), (3, None, "c")])
    v = bw.verify(target, a.batch_id, "live", 1)
    assert v is not None and v.rows_in_commit == 3 == v.rows_counted == res.rows_observed and v.delta_version is not None
    assert a.batch_id in (v.commit_tag or "")
    assert bw.verify(target, "other-batch", "live", 2) is None  # no trace of a batch that never ran
    write(bw, spark, target, [(1, None, "a"), (2, None, "b"), (3, None, "c")])  # replay skipped
    again = bw.verify(target, a.batch_id, "live", 1)
    assert again is not None and again.delta_version == v.delta_version  # same original commit


def test_verify_before_table_exists(bw):
    assert bw.verify("bronze.nope_nope", "b", "live", 1) is None


def test_verify_detects_inconsistent_target(bw, spark, target):
    _, a = write(bw, spark, target, [(1, None, "a")])
    spark.sql(f"INSERT INTO {target} SELECT * FROM {target}")  # corrupt: duplicates rows carrying the same batch id
    with pytest.raises(IntegrityError):
        bw.verify(target, a.batch_id, "live", 1)


def test_reserved_column_names_rejected(bw, spark, target):
    df = spark.createDataFrame([(1, 2)], "id bigint, _batch_id bigint")
    with pytest.raises(DataError, match="reserved"):
        bw.write(df, target, audit(), "ADD_COLUMNS", "x", "mh")
    assert not bw.exists(bw.fq(target))  # nothing created


def test_target_name_rules(bw):
    assert bw.fq("bronze.orders") == "bronze.orders"
    assert BronzeWriter(None, "acme_dev", "DATABRICKS").fq("bronze.orders") == "acme_dev.bronze.orders"
    for bad in ("silver.orders", "orders", "bronze.a.b"):
        with pytest.raises(ValueError):
            bw.fq(bad)


def test_schema_add_column_evolves_table(bw, spark, target):
    write(bw, spark, target, [(1, None, "a")], ddl="id bigint, amount decimal(10,2), `my col` string")
    res, _ = write(bw, spark, target, [(2, None, "b", "x")], ddl="id bigint, amount decimal(10,2), `my col` string, extra string", seq=2)
    assert res.decision.change_type == "ADDED" and res.decision.add_columns == ["extra"]
    rows = {r["id"]: r["extra"] for r in spark.table(target).collect()}
    assert rows == {1: None, 2: "x"}  # old rows null, new column present


def test_schema_fail_mode_blocks_and_writes_nothing(bw, spark, target):
    write(bw, spark, target, [(1, None, "a")])
    with pytest.raises(SchemaChangeError):
        write(bw, spark, target, [(2, None, "b", "x")], ddl="id bigint, amount decimal(10,2), `my col` string, extra string", seq=2, mode="FAIL")
    assert spark.table(target).count() == 1


def test_schema_incompatible_type_fails_or_rescues(bw, spark, target):
    write(bw, spark, target, [(1, None, "a")])
    with pytest.raises(SchemaChangeError):
        write(bw, spark, target, [("not-a-number", None, "b")], ddl="id string, amount decimal(10,2), `my col` string", seq=2)
    res, _ = write(bw, spark, target, [("not-a-number", None, "b")], ddl="id string, amount decimal(10,2), `my col` string", seq=2, mode="RESCUE")
    assert res.decision.rescue == ["id"]
    r = spark.table(target).where("_batch_seq = 2").collect()[0]
    assert r["id"] is None and "not-a-number" in r["_rescued_data"] and r["my col"] == "b"  # nothing lost


def test_narrower_incoming_type_is_upcast(bw, spark, target):
    write(bw, spark, target, [(1, None, "a")])  # id bigint
    write(bw, spark, target, [(2, None, "b")], ddl="id int, amount decimal(10,2), `my col` string", seq=2)
    assert spark.table(target).schema["id"].dataType.simpleString() == "bigint" and spark.table(target).count() == 2


def test_dropped_source_column_leaves_nulls(bw, spark, target):
    write(bw, spark, target, [(1, None, "a")])
    res, _ = write(bw, spark, target, [(2, None)], ddl="id bigint, amount decimal(10,2)", seq=2)
    assert res.decision.change_type == "DROPPED"
    assert spark.table(target).where("id = 2").collect()[0]["my col"] is None


def test_widening_without_runtime_support_fails_clearly(bw, spark, target):
    write(bw, spark, target, [(1, None, "a")], ddl="id int, amount decimal(10,2), `my col` string")
    try:
        write(bw, spark, target, [(2, None, "b")], seq=2, mode="ALLOW_WIDENING")  # id bigint: wider than the table's int
    except SchemaChangeError as exc:
        assert "type widening" in str(exc)  # local Delta lacks the feature: documented fallback (pending Databricks verification)
    else:
        assert spark.table(target).schema["id"].dataType.simpleString() == "bigint"  # runtime supports widening


def test_observation_counts_rows_in_same_pass(bw, spark, target):
    df = spark.range(0, 1000).select(F.col("id"))
    res = bw.write(df, target, audit(), "ADD_COLUMNS", "x:live:BRONZE", "mh")
    assert res.rows_observed == 1000 and spark.table(target).count() == 1000
