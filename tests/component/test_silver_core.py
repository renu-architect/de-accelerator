"""Silver core on real local Delta (audit tables on Delta too): cleansing, casting, rejects, DQ, dedupe, APPEND, SCD1, deletes,
planner ordering, cursors, content verification (including a negative test) and replay healing."""

from datetime import datetime
from decimal import Decimal

import pytest

from de_accelerator.audit.layer_cursor import LayerCursorRepo
from de_accelerator.audit.models import ABANDONED, DONE, Receipt
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.schema.tables import all_tables
from de_accelerator.metadata.store.spark import SparkDeltaTableStore
from de_accelerator.processing.silver import strategies as st
from tests.m3util import build_world, silver_obj

pytestmark = pytest.mark.component

D = "2026-01-01 10:00:00"
CODES = [
    "scd1",
    "hard",
    "ign",
    "app",
    "rej",
    "castfail",
    "dq",
    "dqfail",
    "dqpct",
    "ordr",
    "blk",
    "units",
    "corr",
    "mism",
    "snk",
    "appneg",
]
DQ_BASE = [
    {
        "code": "nn_name",
        "rule_type": "NOT_NULL",
        "columns": ["Name"],
        "severity": "ERROR",
        "threshold_pct": 0.0,
        "action": "QUARANTINE",
    },
    {
        "code": "rng_amt",
        "rule_type": "RANGE",
        "columns": ["Amount"],
        "params": {"min": 0, "max": 100},
        "severity": "ERROR",
        "threshold_pct": 0.0,
        "action": "QUARANTINE",
    },  # fmt: skip
]


@pytest.fixture(scope="module")
def delta_audit(spark):
    s = SparkDeltaTableStore(spark, flavor="LOCAL")
    for t in reversed(all_tables()):
        s.drop_table(t)
    Migrator(s, MIGRATIONS).apply()
    yield s
    for t in reversed(all_tables()):
        s.drop_table(t)


@pytest.fixture(scope="module")
def w(spark, delta_audit, tmp_path_factory):
    objs = [silver_obj(c) for c in CODES]
    by = {o["code"]: o for o in objs}
    by["hard"]["delete_mode"] = "hard"
    by["ign"]["delete_mode"] = "ignore"
    by["app"]["merge"] = "APPEND"
    by["appneg"]["merge"] = "APPEND"
    by["castfail"]["on_cast_error"] = "FAIL"
    by["dq"]["dq"] = DQ_BASE
    by["dqfail"]["dq"] = [{**DQ_BASE[0], "code": "nn_fail", "action": "FAIL_BATCH", "threshold_pct": 0.0}]
    by["dqpct"]["dq"] = [{**DQ_BASE[0], "code": "nn_pct", "action": "FAIL_BATCH", "threshold_pct": 50.0}]
    by["ordr"]["effective"] = "Modified"
    by["ordr"]["dedupe_order"] = "Modified desc"
    world = build_world(tmp_path_factory.mktemp("m3"), spark, objs, store=delta_audit)
    yield world
    for c in CODES:
        for t in (f"bronze.{c}", f"silver.{c}", f"dq.rej_{c}"):
            spark.sql(f"DROP TABLE IF EXISTS {t}")


def r(i, name="a", amt="1", mod=D):
    return {"Id": i, "Name": name, "Amount": amt, "Modified": mod}


def events(w, bid):
    return [e["event_type"] for e in w.events.events(bid)]


def cursor(w, code):
    return LayerCursorRepo(w.store).applied_seq(code, "SILVER")


# ---- happy path ---------------------------------------------------------------------------------------------------------------
def test_cleansing_casting_and_protocol_on_the_happy_path(w):
    w.seed("scd1", [r(1, " alice ", "1,000.50"), r(2, "", "5", "2026-01-02 11:00:00")])
    res = w.run("scd1")
    assert res.status == "DONE" and (res.rows_in, res.rows_applied, res.rows_rejected) == (2, 2, 0)
    rows = {x["id"]: x for x in w.silver("scd1")}
    assert (
        rows[1]["name"] == "ALICE"
        and rows[1]["amount"] == Decimal("1000.50")
        and rows[1]["modified_ts"] == datetime(2026, 1, 1, 10, 0)
    )
    assert rows[2]["name"] is None and rows[2]["amount"] == Decimal("5.00")  # '' -> NULL by nullif
    assert len(rows[1]["_content_hash"]) == 64 and rows[1]["_is_deleted"] is False and rows[1]["_src_batch_seq"] == 1
    rc = w.receipts.get(res.batch_id, "SILVER")
    assert rc is not None and rc["rows_written"] == 2 and rc["rows_read"] == 2 and rc["content_checksum"].startswith("2:")
    assert events(w, res.batch_id) == ["PLANNED", "TARGET_COMMITTED", "CURSOR_ADVANCED", "DONE"]
    assert cursor(w, "scd1") == 1
    assert w.run("scd1").status == "NO_DATA"  # nothing new
    ro = w.store.select("run_object", {"object_code": "scd1"})[0]
    assert ro["layer"] == "SILVER"  # the audit row carries the real layer


def test_scd1_update_insert_and_replay_is_a_data_noop(w):
    w.seed("scd1", [r(1, "alice2", "9"), r(3, "carl")])
    res = w.run("scd1")
    assert res.status == "DONE"
    rows = {x["id"]: x for x in w.silver("scd1")}
    assert rows[1]["name"] == "ALICE2" and rows[3]["name"] == "CARL" and len(rows) == 3
    before = w.silver("scd1", "id")
    # replay of the same merge (what recovery does after a crash) changes no data: re-apply the staged rows of the last unit
    from de_accelerator.processing.common.columns import build_silver_plan
    from de_accelerator.processing.silver.builder import BuildContext, build_staged

    plan = build_silver_plan(w.reader, "m3", "scd1")
    ctx = BuildContext("scd1", res.batch_id, w.clock.now())
    staged = build_staged(plan, w.spark.table("bronze.scd1").where("_batch_seq = 2"), ctx)
    wctx = st.WriteContext("silver.scd1", res.batch_id, 2, w.clock.now(), "s", "x")
    st.apply_scd1(w.spark, wctx, st.finalize(staged.good, wctx), plan)
    staged.release()
    assert w.silver("scd1", "id") == before


def test_effective_time_ordering_guard_and_dedupe_within_a_unit(w):
    w.seed("ordr", [r(1, "newer", "1", "2026-01-05 00:00:00"), r(1, "older", "1", "2026-01-03 00:00:00"), r(2, "x")])
    assert w.run("ordr").status == "DONE"
    assert {x["id"]: x["name"] for x in w.silver("ordr")} == {1: "NEWER", 2: "X"}  # latest by dedupe_order wins inside the unit
    w.seed("ordr", [r(1, "stale", "1", "2026-01-04 00:00:00")])  # older than what Silver holds (01-05)
    res = w.run("ordr")
    assert (
        res.status == "DONE" and {x["id"]: x["name"] for x in w.silver("ordr")}[1] == "NEWER"
    )  # ordering guard: not overwritten
    assert res.extra["counters"]["rows_deduped"] == 0


# ---- rejects + privacy --------------------------------------------------------------------------------------------------------
def test_rejects_are_pointers_only_and_reconcile(w):
    w.seed("rej", [r(1, "ok", "10"), {"Id": None, "Name": "nokey", "Amount": "1", "Modified": D}, r(3, "badamt", "abc"),
                   r(4, "baddate", "1", "not-a-date")])  # fmt: skip
    res = w.run("rej")
    assert res.status == "DONE" and (res.rows_in, res.rows_applied, res.rows_rejected) == (4, 1, 3)
    assert [x["id"] for x in w.silver("rej")] == [1]
    rej = [x.asDict() for x in w.spark.table("dq.rej_rej").collect()]
    assert sorted((x["reason"], x["column_name"]) for x in rej) == [
        ("CAST_ERROR", "amount"),
        ("CAST_ERROR", "modified_ts"),
        ("NULL_BUSINESS_KEY", "id"),
    ]
    assert all(x["occurrences"] == 1 and x["row_hash"] is not None and x["unit_batch_id"] == res.batch_id for x in rej)
    counters = res.extra["counters"]
    assert counters["rows_in"] == counters["rows_applied"] + counters["rows_deduped"] + counters["rows_rejected"]
    # canary: no business value or key anywhere in rejects, events, run errors
    blob = repr(rej) + repr(w.store.select("run_batch_event")) + repr(w.store.select("run_error"))
    for canary in ("nokey", "badamt", "baddate", "not-a-date", "abc"):
        assert canary not in blob


def test_cast_failure_policy_fail_fails_the_unit_and_leaves_state_untouched(w):
    w.seed("castfail", [r(1, "ok", "abc")])
    res = w.run("castfail")
    assert res.status == "FAILED" and res.error_class == "DATA"
    assert cursor(w, "castfail") == 0 and w.spark.table("bronze.castfail").count() == 1  # Bronze untouched, cursor unmoved
    assert not w.spark.catalog.tableExists("silver.castfail") or w.spark.table("silver.castfail").count() == 0
    bid = w.engine.replan("castfail", "bad source value; operator re-plans after a metadata fix", "tester")
    assert ABANDONED in events(w, bid)


# ---- data quality -----------------------------------------------------------------------------------------------------------------
def test_dq_quarantine_rules(w):
    w.seed("dq", [r(1, "ok", "50"), r(2, None, "50"), r(3, "high", "1000")])
    res = w.run("dq")
    assert res.status == "DONE" and [x["id"] for x in w.silver("dq")] == [1]
    reasons = sorted((x["reason"], x["column_name"]) for x in (y.asDict() for y in w.spark.table("dq.rej_dq").collect()))
    assert reasons == [("DQ_nn_name", "name"), ("DQ_rng_amt", "amount")]


def test_dq_fail_batch_above_threshold_fails_and_below_quarantines(w):
    w.seed("dqfail", [r(1, "ok"), r(2, None)])
    res = w.run("dqfail")
    assert res.status == "FAILED" and res.error_class == "DATA" and cursor(w, "dqfail") == 0
    w.seed("dqpct", [r(1, "ok"), r(2, "ok2"), r(3, None)])  # 33% failing <= 50% limit -> quarantined, batch continues
    res = w.run("dqpct")
    assert res.status == "DONE" and sorted(x["id"] for x in w.silver("dqpct")) == [1, 2]


# ---- APPEND + deletes -------------------------------------------------------------------------------------------------------------
def test_append_is_idempotent_and_keeps_history(w):
    w.seed("app", [r(1, "a")])
    assert w.run("app").status == "DONE"
    w.seed("app", [r(1, "b")])
    assert w.run("app").status == "DONE"
    rows = w.silver("app", "_src_batch_seq")
    assert [x["name"] for x in rows] == ["A", "B"] and all(x["_is_deleted"] is False for x in rows)
    assert w.run("app").status == "NO_DATA"


def _delete_row(i):
    return {"Id": i, "Name": None, "Amount": None, "Modified": None}


def test_soft_delete_then_reappearance(w):
    w.seed("scd1", [_delete_row(3)], op="D")
    assert w.run("scd1").status == "DONE"
    row = {x["id"]: x for x in w.silver("scd1")}[3]
    assert (
        row["_is_deleted"] is True and row["_deleted_ts"] is not None and row["name"] == "CARL"
    )  # tombstone keeps the last values
    w.seed("scd1", [r(3, "carl-back")])
    assert w.run("scd1").status == "DONE"
    row = {x["id"]: x for x in w.silver("scd1")}[3]
    assert row["_is_deleted"] is False and row["_deleted_ts"] is None and row["name"] == "CARL-BACK"


def test_hard_delete_removes_the_row(w):
    w.seed("hard", [r(1), r(2)])
    assert w.run("hard").status == "DONE"
    w.seed("hard", [_delete_row(1)], op="D")
    assert w.run("hard").status == "DONE"
    assert [x["id"] for x in w.silver("hard")] == [2]


def test_ignore_mode_drops_delete_rows(w):
    w.seed("ign", [r(1), r(2)])
    assert w.run("ign").status == "DONE"
    w.seed("ign", [_delete_row(1)], op="D")
    res = w.run("ign")
    assert res.status == "DONE" and sorted(x["id"] for x in w.silver("ign")) == [1, 2]
    assert all(x["_is_deleted"] is False for x in w.silver("ign"))


# ---- planner ordering / units --------------------------------------------------------------------------------------------------------
def test_open_lower_batch_blocks_later_ones_until_it_has_a_receipt(w):
    b1 = w.seed("blk", [r(1)], receipt=False)  # Bronze batch 1 is still open (no verified receipt)
    w.seed("blk", [r(2)])
    assert w.run("blk").status == "NO_DATA" and cursor(w, "blk") == 0
    w.receipts.insert(Receipt(b1, "BRONZE", "bronze.blk", None, None, 1, 0, 1, None, 1))
    res = w.run("blk")
    assert res.status == "DONE" and res.rows_in == 2 and sorted(x["id"] for x in w.silver("blk")) == [1, 2]
    assert cursor(w, "blk") == 2  # one unit covered both batches


def test_full_extract_batches_form_their_own_unit(w):
    w.seed("units", [r(1)])
    w.seed("units", [r(2)])
    w.seed("units", [r(3)], mode="full")
    w.seed("units", [r(4)])
    seqs = []
    for _ in range(4):
        res = w.run("units")
        if res.status == "NO_DATA":
            break
        m = w.manifests.get(res.batch_id)
        seqs.append((m.wm_from, m.wm_to))
    assert seqs == [("1", "2"), ("3", "3"), ("4", "4")]


def test_bronze_receipt_mismatch_is_an_integrity_failure(w):
    w.seed("mism", [r(1), r(2)], receipt=False)
    w.receipts.insert(
        Receipt(w.manifests.list("mism", "live")[0].batch_id, "BRONZE", "bronze.mism", None, None, 99, 0, 99, None, 1)
    )
    res = w.run("mism")
    assert res.status == "FAILED" and res.error_code == "ACC-4009" and cursor(w, "mism") == 0


# ---- content verification ---------------------------------------------------------------------------------------------------------
def test_verification_fails_when_the_target_lacks_staged_content_and_recovery_heals(w, monkeypatch):
    w.seed("corr", [r(1, "a"), r(2, "b"), r(3, "c")])
    real = st.apply_scd1

    def corrupt(spark, ctx, final, plan):
        out = real(spark, ctx, final, plan)
        spark.sql(f"DELETE FROM {ctx.fq} WHERE id = 2")  # a "lost" row after the merge
        return out

    monkeypatch.setattr(st, "apply_scd1", corrupt)
    res = w.run("corr")
    assert res.status == "FAILED" and res.error_code == "ACC-7003" and cursor(w, "corr") == 0
    assert w.receipts.get(res.batch_id if res.batch_id else w.manifests.list("corr", "SILVER:live")[0].batch_id, "SILVER") is None
    monkeypatch.setattr(st, "apply_scd1", real)
    res2 = w.run("corr")  # the failed unit is recovered: merge replayed, verification now passes
    assert (
        res2.status == "DONE"
        and res2.recovered
        and sorted(x["id"] for x in w.silver("corr")) == [1, 2, 3]
        and cursor(w, "corr") == 1
    )
    assert DONE in events(w, res2.batch_id)


def test_schema_change_other_than_add_column_is_a_config_error(w, spark):
    w.seed("snk", [r(1)])
    assert w.run("snk").status == "DONE"
    final = spark.createDataFrame([(1,)], "id string")  # type change bigint -> string
    from de_accelerator.processing.common.errors import TransformConfigError

    with pytest.raises(TransformConfigError):
        st.ensure_table(spark, "silver.snk", final.withColumn("_op", final.id))


def test_add_column_is_applied_additively(w, spark):
    from pyspark.sql import functions as F

    base = spark.table("silver.snk").limit(1)
    wider = base.withColumn("extra_col", F.lit("x")).withColumn("_op", F.lit("U")).drop("_op_seq", "_ingest_ts", "_row_hash")
    st.ensure_table(spark, "silver.snk", wider)
    assert "extra_col" in spark.table("silver.snk").columns
    assert spark.table("silver.snk").select("extra_col").first()[0] is None  # old rows: NULL


def test_append_verification_fails_when_a_written_row_is_missing_from_the_target(w, monkeypatch):
    w.seed("appneg", [r(1, "a"), r(2, "b")])
    real = st.apply_append

    def lossy(spark, ctx, final):
        out = real(spark, ctx, final)
        spark.sql(f"DELETE FROM {ctx.fq} WHERE id = 2")  # a row vanishes after the write
        return out

    monkeypatch.setattr(st, "apply_append", lossy)
    res = w.run("appneg")
    assert res.status == "FAILED" and res.error_code == "ACC-7003" and cursor(w, "appneg") == 0
    bid = w.manifests.list("appneg", "SILVER:live")[0].batch_id
    assert w.receipts.get(bid, "SILVER") is None  # no receipt for unverified data
