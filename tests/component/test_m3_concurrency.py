"""Parallel execution on real Delta (audit tables on Delta too): 4-way Silver, mixed Silver+Gold levels, same-object double start."""

import threading
import uuid

import pytest

from de_accelerator.audit.layer_cursor import LayerCursorRepo
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.schema.tables import all_tables
from de_accelerator.metadata.store.spark import SparkDeltaTableStore
from tests.m3util import PROJECT, build_world, silver_obj

pytestmark = pytest.mark.component

D = "2026-01-01 10:00:00"
PAR = ["par1", "par2", "par3", "par4"]
GOLD = [{"code": "mx_g", "merge": "OVERWRITE", "keys": ["id"], "dep_type": "HARD",
         "steps": [{"type": "SQL", "text": "SELECT id, name FROM {{ input.mx_s }}", "inputs": ["mx_s"], "params": {}}]}]  # fmt: skip


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
    objs = [silver_obj(c) for c in [*PAR, "mx_s", "dbl"]]
    world = build_world(tmp_path_factory.mktemp("conc"), spark, objs, store=delta_audit, gold=GOLD)
    yield world
    for c in [*PAR, "mx_s", "dbl"]:
        for t in (f"bronze.{c}", f"silver.{c}", f"dq.rej_{c}"):
            spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql("DROP TABLE IF EXISTS gold.mx_g")


def row(i, name="a"):
    return {"Id": i, "Name": name, "Amount": "1", "Modified": D}


def test_four_way_parallel_silver_runs_twice_without_duplicates_or_lost_units(w):
    for rnd in (1, 2):
        for c in PAR:
            w.seed(c, [row(rnd * 10 + 1, f"{c}-{rnd}"), row(rnd * 10 + 2)])
        s = w.runner.run(objects=tuple(PAR), layer="SILVER", concurrency=4)
        assert s.status == "SUCCEEDED" and {r.status for r in s.results} == {"DONE"} and len(s.results) == 4
        for c in PAR:
            assert LayerCursorRepo(w.store).applied_seq(c, "SILVER") == rnd
            assert len(w.silver(c)) == 2 * rnd  # exactly the rows of the units applied so far: nothing doubled, nothing lost
            assert len(w.manifests.list(c, "SILVER:live")) == rnd


def test_mixed_levels_run_silver_before_gold_in_one_run(w):
    w.seed("mx_s", [row(1, "one"), row(2, "two")])
    s = w.runner.run(objects=("mx_s", "mx_g"), concurrency=4)
    by = {(r.layer, r.object_code): r.status for r in s.results}
    assert by == {("SILVER", "mx_s"): "DONE", ("GOLD", "mx_g"): "DONE"} and s.status == "SUCCEEDED"
    assert sorted(x["name"] for x in w.table("gold.mx_g")) == ["ONE", "TWO"]  # Gold saw the Silver unit committed moments earlier


def test_same_object_started_twice_at_once_processes_the_unit_exactly_once(w):
    w.seed("dbl", [row(1), row(2), row(3)])
    out: list = []

    def go():
        out.append(w.engine.run_object(w.reader, w.snap, PROJECT, "dbl", str(uuid.uuid4())))

    ts = [threading.Thread(target=go) for _ in range(2)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert {r.status for r in out} <= {"DONE", "BUSY", "NO_DATA"} and any(r.status == "DONE" for r in out)
    assert len(w.silver("dbl")) == 3 and len(w.manifests.list("dbl", "SILVER:live")) == 1
    assert LayerCursorRepo(w.store).applied_seq("dbl", "SILVER") == 1
