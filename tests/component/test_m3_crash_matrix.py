"""M3 crash-recovery matrix on real Delta (audit tables on Delta too): 6 crash points x 6 strategies = 36 scenarios.

For every scenario the process "dies" (InjectedCrash is a BaseException: no cleanup runs) at one protocol point of the SECOND unit; a
fresh engine then recovers. Asserted for each: the final data equals an identical no-crash run (content comparison, not counts), exactly
one receipt for the unit, exactly one unit manifest, the cursor advanced exactly once, and the unit ends DONE.
Crash points: U_CP1 before manifest | U_CP2 after manifest | U_CP3 after build, before merge | U_CP4 after merge, before receipt |
U_CP5 after receipt, before cursor advance | U_CP6 after cursor advance, before DONE.
"""

import pytest

from de_accelerator.audit.layer_cursor import LayerCursorRepo
from de_accelerator.core.faults import FaultInjector, InjectedCrash
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.schema.tables import all_tables
from de_accelerator.metadata.store.spark import SparkDeltaTableStore
from de_accelerator.processing.gold.engine import GoldEngine
from de_accelerator.processing.silver.engine import SilverEngine
from tests.m3util import PROJECT, build_world, silver_obj

pytestmark = pytest.mark.component

POINTS = [
    "U_CP1_before_manifest",
    "U_CP2_after_manifest",
    "U_CP3_after_build",
    "U_CP4_after_merge",
    "U_CP5_after_receipt",
    "U_CP6_after_cursor",
]
SILVER_STRATS = ["app", "scd1", "scd2", "del"]
GOLD_STRATS = ["gfull", "gmerge"]
T1, T2 = "2026-01-01 10:00:00", "2026-01-02 10:00:00"
TECH_DROP = {
    "_silver_batch_id",
    "_silver_ts",
    "_src_batch_id",
    "_config_snapshot_id",
    "_gold_batch_id",
    "_gold_ts",
    "_op_seq",
    "_ingest_ts",
    "_row_hash",
}


def r(i, name, amt, mod):
    return {"Id": i, "Name": name, "Amount": amt, "Modified": mod}


A = [r(1, "a", "1.00", T1), r(2, "b", "2.00", T1)]
B = [r(1, "a2", "1.50", T2), r(3, "c", "3.00", T2)]
DEL = {"Id": 2, "Name": None, "Amount": None, "Modified": T2}


CODES = [(s, p) for s in SILVER_STRATS for p in [*POINTS, "ref"]]
GOLD_SQL = "SELECT id, name, amount FROM {{ input.%s }} WHERE NOT _is_deleted"


def objs():
    out = []
    for s, p in CODES:
        c = f"{s}_{p[:5].lower() if p != 'ref' else 'ref'}"
        merge = {"app": "APPEND", "scd1": "SCD1", "scd2": "SCD2", "del": "SCD1"}[s]
        kw = {"merge": merge}
        if s == "scd2":
            kw.update(effective="Modified", scd_columns=["Name"])
        out.append(silver_obj(c, **kw))
    for s in GOLD_STRATS:  # Gold inputs: one shared Silver object per Gold strategy (Gold cases only read it)
        out.append(silver_obj(f"in_{s}", merge="SCD1"))
    return out


def gold_objs():
    out = []
    for s in GOLD_STRATS:
        for p in [*POINTS, "ref"]:
            tag = p[:5].lower() if p != "ref" else "ref"
            merge = "OVERWRITE" if s == "gfull" else "MERGE"
            out.append({"code": f"{s}_{tag}", "merge": merge, "keys": ["id"], "dep_type": "HARD",
                        "steps": [{"type": "SQL", "text": GOLD_SQL % f"in_{s}", "inputs": [f"in_{s}"], "params": {}}]})  # fmt: skip
    return out


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
    world = build_world(tmp_path_factory.mktemp("crash"), spark, objs(), store=delta_audit, gold=gold_objs())
    yield world
    for o in objs():
        for t in (f"bronze.{o['code']}", f"silver.{o['code']}", f"dq.rej_{o['code']}"):
            spark.sql(f"DROP TABLE IF EXISTS {t}")
    for g in gold_objs():
        spark.sql(f"DROP TABLE IF EXISTS gold.{g['code']}")


def norm(rows):
    out = []
    for x in rows:
        out.append(tuple(sorted((k, repr(v)) for k, v in x.items() if k not in TECH_DROP)))
    return sorted(out)


def prepare_silver(w, c, strat):
    """Both batches (and the delete) are in ONE unit, so the crashing unit contains updates, new keys and deletes."""
    w.seed(c, A)
    w.seed(c, B)
    if strat == "del":
        w.seed(c, [DEL], op="D")


def ensure_input(w, strat):
    src = f"in_{strat}"
    if not w.spark.catalog.tableExists(f"silver.{src}"):
        w.seed(src, A)
        w.seed(src, B)
        assert w.run(src).status == "DONE"
    return src


def check_common(w, layer, c, stream, expected_seq_to):
    ms = w.manifests.list(c, stream)
    assert [m for m in ms if w.events.state(m.batch_id) not in ("DONE",)] == []  # nothing left open
    last = ms[-1]
    assert len([m for m in ms if m.batch_seq == last.batch_seq]) == 1  # exactly one manifest for the unit
    rcs = [w.receipts.get(m.batch_id, layer) for m in ms]
    assert all(rc is not None for rc in rcs)  # exactly one receipt per unit (insert-once repository)
    assert LayerCursorRepo(w.store).applied_seq(c, layer) == expected_seq_to


@pytest.mark.parametrize("point", POINTS)
@pytest.mark.parametrize("strat", SILVER_STRATS)
def test_silver_crash_recovery_equals_a_clean_run(w, spark, strat, point):
    tag = point[:5].lower()
    t0 = w.clock.now()
    c, ref = f"{strat}_{tag}", f"{strat}_ref"
    if not w.spark.catalog.tableExists(f"silver.{ref}"):
        prepare_silver(w, ref, strat)
        assert w.run(ref).status == "DONE"
    prepare_silver(w, c, strat)
    crashing = SilverEngine(w.store, spark, w.engine.cfg, w.clock, FaultInjector({point: 1}))
    with pytest.raises(InjectedCrash):
        crashing.run_object(w.reader, w.snap, PROJECT, c, "crash-run")
    w.clock.advance(minutes=31)  # a crashed runner keeps its lease until the TTL (30 min) expires; the recovering run starts later
    try:
        res = w.run(c)  # a fresh process recovers
    finally:
        w.clock._now = t0  # ingest timestamps (effective time of non-SCD2 rows) must stay comparable with the reference run
    assert res.status == "DONE"
    assert res.recovered == (point != "U_CP1_before_manifest")
    assert norm(w.silver(c)) == norm(w.silver(ref)), (strat, point)
    last_seq = int(w.manifests.list(c, "SILVER:live")[-1].wm_to)
    check_common(w, "SILVER", c, "SILVER:live", last_seq)
    assert w.run(c).status == "NO_DATA"  # and it stays done


@pytest.mark.parametrize("point", POINTS)
@pytest.mark.parametrize("strat", GOLD_STRATS)
def test_gold_crash_recovery_equals_a_clean_run(w, spark, strat, point):
    tag = point[:5].lower()
    ensure_input(w, strat)
    g = f"{strat}_{tag}"
    if not spark.catalog.tableExists(f"gold.{strat}_ref"):
        assert w.run_gold(f"{strat}_ref").status == "DONE"
    crashing = GoldEngine(w.store, spark, w.gold.cfg, w.clock, FaultInjector({point: 1}))
    with pytest.raises(InjectedCrash):
        crashing.run_object(w.reader, w.snap, PROJECT, g, "crash-run")
    t0 = w.clock.now()
    w.clock.advance(minutes=31)  # crashed runner's lease expires
    try:
        res = w.run_gold(g)
    finally:
        w.clock._now = t0
    assert res.status == "DONE" and res.recovered == (point != "U_CP1_before_manifest")
    assert norm(w.table(f"gold.{g}")) == norm(w.table(f"gold.{strat}_ref")), (strat, point)
    check_common(w, "GOLD", g, "GOLD:main", 1)
    assert w.run_gold(g).status == "NO_DATA"
