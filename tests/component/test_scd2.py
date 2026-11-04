"""SCD2 on real local Delta: logical-change identity, collapse, recurrence, same-time changes, late arrivals, tombstones, replay,
verification, plus a pure-Python reference model checked with hypothesis (replay idempotence is covered by the crash matrix)."""

from datetime import datetime, timedelta

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as hs

from de_accelerator.audit.layer_cursor import LayerCursorRepo
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.schema.tables import all_tables
from de_accelerator.metadata.store.spark import SparkDeltaTableStore
from de_accelerator.processing.silver import scd2 as sc
from de_accelerator.processing.silver import strategies as st
from tests.m3util import build_world, silver_obj

pytestmark = pytest.mark.component

DDL = "Id int, Name string, Note string, Modified string"
BASE = datetime(2026, 1, 1, 0, 0, 0)
N_PROP = 6
EXPLICIT = ["basic", "collapse", "recur", "dup", "late", "same", "tomb", "unit", "verify", "replay"]
PROP = [f"pa{i}" for i in range(N_PROP)] + [f"pb{i}" for i in range(N_PROP)]
COLS = [
    {"name": "Id", "ordinal": 1, "target_type": "bigint", "is_bk": True, "is_nullable": False, "target_name": "id"},
    {"name": "Name", "ordinal": 2, "target_type": "string", "target_name": "name"},
    {"name": "Note", "ordinal": 3, "target_type": "string", "target_name": "note"},
    {"name": "Modified", "ordinal": 4, "target_type": "timestamp", "target_name": "modified_ts",
     "cleanse_rules": {"ops": [{"op": "parse_timestamp", "format": "yyyy-MM-dd HH:mm:ss"}]}},
]  # fmt: skip


def mins(n: int) -> str:
    return (BASE + timedelta(minutes=n)).strftime("%Y-%m-%d %H:%M:%S")


def row(i, name, note="n", at=0):
    return {"Id": i, "Name": name, "Note": note, "Modified": mins(at)}


def drow(i, at):
    return {"Id": i, "Name": None, "Note": None, "Modified": mins(at)}


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
    objs = [silver_obj(c, merge="SCD2", columns=COLS, effective="Modified", scd_columns=["Name"], dedupe_order="Modified desc")
            for c in [*EXPLICIT, *PROP]]  # fmt: skip
    world = build_world(tmp_path_factory.mktemp("scd2"), spark, objs, store=delta_audit)
    yield world
    for c in [*EXPLICIT, *PROP]:
        for t in (f"bronze.{c}", f"silver.{c}", f"dq.rej_{c}"):
            spark.sql(f"DROP TABLE IF EXISTS {t}")


def seed(w, code, rows, op="C"):
    return w.seed(code, rows, op=op, ddl=DDL)


def versions(w, code):
    out: dict[int, list[tuple]] = {}
    for r in sorted(w.silver(code), key=lambda r: (r["id"], r["_valid_from"], r["_src_batch_seq"], r["_scd_hash"])):
        f = lambda d: None if d is None else int((d - BASE).total_seconds() // 60)  # noqa: E731
        out.setdefault(r["id"], []).append(
            (f(r["_valid_from"]), f(r["_valid_to"]), r["name"], r["note"], r["_is_deleted"], r["_is_current"])
        )
    return out


# ---- explicit scenarios -----------------------------------------------------------------------------------------------------------
def test_new_version_closes_the_previous_one(w):
    seed(w, "basic", [row(1, "A", "n1", 10)])
    assert w.run("basic").status == "DONE"
    seed(w, "basic", [row(1, "B", "n2", 20)])
    res = w.run("basic")
    assert res.status == "DONE" and res.extra["counters"]["versions_inserted"] == 1
    assert versions(w, "basic")[1] == [(10, 20, "A", "n1", False, False), (20, None, "B", "n2", False, True)]
    assert all(len(r["_sk"]) == 32 for r in w.silver("basic"))


def test_unchanged_tracked_state_collapses_and_updates_untracked_in_place(w):
    seed(w, "collapse", [row(1, "A", "n1", 10)])
    w.run("collapse")
    seed(w, "collapse", [row(1, "A", "n2", 30)])  # same tracked value, later effective time, different untracked column
    res = w.run("collapse")
    assert res.status == "DONE" and res.extra["counters"]["versions_inserted"] == 0
    assert versions(w, "collapse")[1] == [(10, None, "A", "n2", False, True)]  # one version; note refreshed in place


def test_recurrence_a_b_a_creates_three_versions(w):
    for name, at in (("A", 10), ("B", 20), ("A", 30)):
        seed(w, "recur", [row(1, name, "n", at)])
        assert w.run("recur").status == "DONE"
    assert [(v[0], v[1], v[2]) for v in versions(w, "recur")[1]] == [(10, 20, "A"), (20, 30, "B"), (30, None, "A")]


def test_same_logical_change_redelivered_in_another_batch_is_a_duplicate(w):
    seed(w, "dup", [row(1, "A", "n", 10)])
    w.run("dup")
    seed(w, "dup", [row(1, "B", "n", 20)])
    w.run("dup")
    before = versions(w, "dup")
    seed(w, "dup", [row(1, "A", "n", 10)])  # an old state re-delivered with the very same effective time (a duplicate LCI)
    res = w.run("dup")
    assert res.status == "DONE" and res.extra["counters"]["duplicates_dropped"] == 1 and res.rows_rejected == 0
    assert versions(w, "dup") == before


def test_late_arrival_is_rejected_not_lost_or_applied(w):
    seed(w, "late", [row(1, "A", "n", 50)])
    w.run("late")
    before = versions(w, "late")
    seed(w, "late", [row(1, "Z", "n", 20)])  # older than the key's newest valid_from (50) and a new state
    res = w.run("late")
    assert res.status == "DONE" and res.rows_rejected == 1 and res.extra["counters"]["late_rejected"] == 1
    assert versions(w, "late") == before
    rej = [r.asDict() for r in w.spark.table("dq.rej_late").collect()]
    assert [(r["reason"], r["column_name"]) for r in rej] == [("SCD2_LATE_ARRIVAL", "")] and "Z" not in repr(rej)
    assert res.extra["counters"]["rows_in"] == res.rows_applied + res.extra["counters"]["rows_deduped"] + res.rows_rejected


def test_same_effective_time_different_state_keeps_a_zero_length_version(w):
    seed(w, "same", [row(1, "A", "n", 40)])
    w.run("same")
    seed(w, "same", [row(1, "B", "n", 40)])  # same effective time, new state
    assert w.run("same").status == "DONE"
    v = versions(w, "same")[1]
    assert v == [(40, 40, "A", "n", False, False), (40, None, "B", "n", False, True)]
    # point in time: at t=40 exactly one version qualifies (valid_from <= t < valid_to), the zero-length one never does
    assert [x for x in v if x[0] <= 40 and (x[1] is None or 40 < x[1])] == [v[1]]


def test_tombstone_and_reappearance_and_unknown_key_delete(w):
    seed(w, "tomb", [row(1, "A", "n", 10), row(2, "A", "n", 10)])
    w.run("tomb")
    seed(w, "tomb", [drow(1, 20), drow(99, 20)], op="D")  # 99 was never seen: no tombstone for an unknown key
    assert w.run("tomb").status == "DONE"
    v = versions(w, "tomb")
    assert 99 not in v and v[1] == [(10, 20, "A", "n", False, False), (20, None, None, None, True, True)] and v[2][0][5] is True
    seed(w, "tomb", [row(1, "A", "n", 30)])
    w.run("tomb")
    assert [(x[0], x[1], x[4], x[5]) for x in versions(w, "tomb")[1]] == [
        (10, 20, False, False),
        (20, 30, True, False),
        (30, None, False, True),
    ]


def test_several_batches_in_one_unit_keep_every_intermediate_version(w):
    seed(w, "unit", [row(1, "A", "n", 10)])
    seed(w, "unit", [row(1, "B", "n", 20)])
    seed(w, "unit", [row(1, "C", "n", 30)])
    res = w.run("unit")
    assert res.status == "DONE" and res.rows_in == 3
    assert [(v[0], v[1], v[2]) for v in versions(w, "unit")[1]] == [(10, 20, "A"), (20, 30, "B"), (30, None, "C")]
    assert LayerCursorRepo(w.store).applied_seq("unit", "SILVER") == 3


def test_replaying_the_merge_is_a_data_noop(w):
    seed(w, "replay", [row(1, "A", "n", 10), row(2, "B", "n", 10)])
    w.run("replay")
    seed(w, "replay", [row(1, "C", "n", 20)])
    res = w.run("replay")
    before = versions(w, "replay")
    from de_accelerator.processing.common.columns import build_silver_plan
    from de_accelerator.processing.silver.builder import BuildContext, build_staged

    plan = build_silver_plan(w.reader, "m3", "replay")
    staged = build_staged(
        plan,
        w.spark.table("bronze.replay").where("_batch_seq = 2"),
        BuildContext("replay", res.batch_id, w.clock.now(), dedupe=False),
    )
    wctx = st.WriteContext("silver.replay", res.batch_id, 2, w.clock.now(), "s", "x")
    r2 = sc.apply_scd2(w.spark, wctx, plan, staged.good, staged.target_columns, "replay", res.batch_id, w.clock.now())
    assert r2.counters["versions_inserted"] == 0 and r2.counters["duplicates_dropped"] == 1
    sc.verify_scd2(w.spark, wctx, plan, r2)
    r2.release()
    staged.release()
    assert versions(w, "replay") == before


def test_verification_detects_a_broken_chain_and_is_not_vacuous(w, monkeypatch):
    seed(w, "verify", [row(1, "A", "n", 10)])
    w.run("verify")
    seed(w, "verify", [row(1, "B", "n", 20)])
    real = sc.apply_scd2

    def corrupt(spark, ctx, plan, staged, targets, obj, bid, ts):
        out = real(spark, ctx, plan, staged, targets, obj, bid, ts)
        spark.sql(f"UPDATE {ctx.fq} SET _valid_to = NULL WHERE name = 'A'")  # reopen the closed version after the merge
        return out

    monkeypatch.setattr(sc, "apply_scd2", corrupt)
    res = w.run("verify")
    assert res.status == "FAILED" and res.error_code == "ACC-7003"  # the chain violation is caught against the pre-merge decision
    bid = w.manifests.list("verify", "SILVER:live")[-1].batch_id
    assert w.receipts.get(bid, "SILVER") is None and LayerCursorRepo(w.store).applied_seq("verify", "SILVER") == 1
    monkeypatch.setattr(sc, "apply_scd2", real)
    # an operator repairs the damaged row (this is tampering, not a crash: a merge replay cannot know about it), then the unit completes
    w.spark.sql("UPDATE silver.verify SET _valid_to = timestamp'2026-01-01 00:20:00' WHERE name = 'A'")
    res2 = w.run("verify")
    assert res2.status == "DONE" and versions(w, "verify")[1] == [
        (10, 20, "A", "n", False, False),
        (20, None, "B", "n", False, True),
    ]


# ---- reference model ---------------------------------------------------------------------------------------------------------------------
def model(batches):
    """Independent pure-Python SCD2 (tracked column: name; untracked: note). Rows per key never move backwards in time."""
    vs: dict[int, list[dict]] = {}
    for rows in batches:
        for k, eff, name, note, op in sorted(rows, key=lambda r: r[1]):
            cur = vs.setdefault(k, [])
            h = ("D",) if op == "D" else ("U", name)
            if not cur:
                if op == "D":
                    continue
                cur.append({"vf": eff, "vt": None, "name": name, "note": note, "del": False, "h": h})
                continue
            last = cur[-1]
            if last["h"] == h:
                if op == "U":
                    last["note"] = note
                continue
            last["vt"] = eff
            cur.append(
                {
                    "vf": eff,
                    "vt": None,
                    "name": None if op == "D" else name,
                    "note": None if op == "D" else note,
                    "del": op == "D",
                    "h": h,
                }
            )
    return {
        k: [(v["vf"], v["vt"], v["name"], v["note"], v["del"], i == len(x) - 1) for i, v in enumerate(x)]
        for k, x in vs.items()
        if x
    }


stream = hs.lists(hs.lists(hs.tuples(hs.integers(1, 3), hs.integers(0, 2), hs.sampled_from(["A", "B", "C"]), hs.sampled_from(["n1", "n2"]),
                                      hs.sampled_from(["U", "U", "U", "D"])), min_size=1, max_size=3, unique_by=lambda t: t[0]), min_size=1, max_size=3)  # fmt: skip
_counter = {"i": 0}


def to_batches(raw):
    last: dict[int, int] = {}
    out = []
    for rows in raw:
        b = []
        for k, delta, name, note, op in rows:
            last[k] = last.get(k, 0) + delta
            b.append((k, last[k] + 1, name, note, op))
        out.append(b)
    return out


def seed_batch(w, code, batch):
    ups = [row(k, name, note, eff) for k, eff, name, note, op in batch if op == "U"]
    dels = [drow(k, eff) for k, eff, name, note, op in batch if op == "D"]
    if ups:
        seed(w, code, ups)
    if dels:
        seed(w, code, dels, op="D")


@settings(max_examples=N_PROP, deadline=None, suppress_health_check=list(HealthCheck), derandomize=True)
@given(stream)
def test_engine_matches_reference_model_and_is_unit_boundary_invariant(w, raw):
    i = _counter["i"]
    _counter["i"] += 1
    if i >= N_PROP:
        pytest.skip("object pool exhausted")
    batches = to_batches(raw)
    expected = model(batches)
    a, b = f"pa{i}", f"pb{i}"
    for batch in batches:  # a: one unit per batch
        seed_batch(w, a, batch)
        assert w.run(a).status in ("DONE", "NO_DATA")
    for batch in batches:  # b: everything in a single unit
        seed_batch(w, b, batch)
    assert w.run(b).status in ("DONE", "NO_DATA")
    got_a, got_b = versions(w, a), versions(w, b)
    assert got_a == expected, (batches, got_a, expected)
    assert got_b == expected, (batches, got_b, expected)
