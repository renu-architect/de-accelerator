"""Gold on real local Delta: reviewed SQL/PySpark files, FULL and KEY_MERGE, typed parameters, version pinning, no-change skip, trust
controls, dependency runner (HARD skip) and the transform CLI."""

from decimal import Decimal

import pytest

from de_accelerator.audit.layer_cursor import LayerCursorRepo
from de_accelerator.cli.main import run as cli_run
from de_accelerator.core.faults import FaultInjector, InjectedCrash
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.schema.tables import all_tables
from de_accelerator.metadata.store.spark import SparkDeltaTableStore
from de_accelerator.processing.common.errors import TransformConfigError
from de_accelerator.processing.gold.engine import GoldEngine, typed_params
from tests.m3util import build_world, silver_obj

pytestmark = pytest.mark.component

D = "2026-01-01 10:00:00"
SQL_FULL = "SELECT id, name, amount FROM {{ input.ord }} WHERE NOT _is_deleted AND amount >= :min_amount"
SQL_JOIN = ("SELECT o.id, o.amount, c.name AS cust_name FROM {{ input.ord }} o "
            "LEFT JOIN {{ input.cust }} c ON c.id = o.id WHERE NOT o._is_deleted")  # fmt: skip
PY = (
    "def transform(inputs, params, ctx):\n"
    "    return inputs['ord'].where('NOT _is_deleted').selectExpr('id', 'upper(name) AS shout')\n"
)
SILVER = ["ord", "cust", "ordf"]


def g(code, merge, steps, keys=("id",), dep="HARD"):
    return {"code": code, "merge": merge, "steps": steps, "keys": list(keys), "dep_type": dep}


def sql(text, inputs, params=None):
    return {"type": "SQL", "text": text, "inputs": inputs, "params": params or {}}


GOLD = [
    g("g_full", "OVERWRITE", [sql(SQL_FULL, ["ord"], {"min_amount": {"type": "decimal", "value": "0"}})]),
    g("g_merge", "MERGE", [sql(SQL_JOIN, ["ord", "cust"])]),
    g("g_py", "OVERWRITE", [{"type": "PYSPARK", "text": PY, "inputs": ["ord"]}]),
    g(
        "g_multi",
        "OVERWRITE",
        [sql("SELECT id, amount FROM {{ input.ord }}", ["ord"]), sql("SELECT sum(amount) AS total FROM {{ step.1 }}", [])],
        keys=(),
    ),
    g("g_dup", "MERGE", [sql("SELECT 1 AS id, name FROM {{ input.ord }}", ["ord"])]),
    g("g_tamper", "OVERWRITE", [sql("SELECT id FROM {{ input.ord }}", ["ord"])]),
    g(
        "g_evil",
        "OVERWRITE",
        [sql("SELECT id FROM {{ input.ord }} WHERE id > 0 AND 1 = (SELECT 1); DELETE FROM silver.ord", ["ord"])],
    ),
    g("g_pin", "OVERWRITE", [sql("SELECT id, name FROM {{ input.ord }}", ["ord"])]),
    g("g_dep", "OVERWRITE", [sql("SELECT id FROM {{ input.ordf }}", ["ordf"])], dep="HARD"),
    g("g_soft", "OVERWRITE", [sql("SELECT id FROM {{ input.ordf }}", ["ordf"])], dep="SOFT"),
    g("g_vfull", "OVERWRITE", [sql("SELECT id, name FROM {{ input.cust }}", ["cust"])]),
    g("g_vmerge", "MERGE", [sql("SELECT id, name FROM {{ input.cust }}", ["cust"])]),
    g("g_cli", "OVERWRITE", [sql("SELECT id, name FROM {{ input.cust }}", ["cust"])]),
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
    objs = [silver_obj(c) for c in SILVER]
    objs[2]["on_cast_error"] = "FAIL"
    world = build_world(tmp_path_factory.mktemp("gold"), spark, objs, store=delta_audit, gold=GOLD)
    yield world
    for c in SILVER:
        for t in (f"bronze.{c}", f"silver.{c}", f"dq.rej_{c}"):
            spark.sql(f"DROP TABLE IF EXISTS {t}")
    for x in GOLD:
        spark.sql(f"DROP TABLE IF EXISTS gold.{x['code']}")


def r(i, name="a", amt="10", mod=D):
    return {"Id": i, "Name": name, "Amount": amt, "Modified": mod}


def cur(w, code):
    return LayerCursorRepo(w.store).get(code, "GOLD")


# ---- FULL / typed parameters / no-change ---------------------------------------------------------------------------------------------------
def test_full_overwrite_pins_versions_and_skips_when_nothing_changed(w):
    w.seed("ord", [r(1, "a", "10"), r(2, "b", "5"), r(3, "c", "-1")])
    assert w.run("ord").status == "DONE"
    res = w.run_gold("g_full")
    assert res.status == "DONE" and res.rows_applied == 2  # amount >= :min_amount (typed decimal parameter)
    assert sorted((x["id"], x["amount"]) for x in w.table("gold.g_full")) == [(1, Decimal("10.00")), (2, Decimal("5.00"))]
    m = w.manifests.get(res.batch_id)
    assert (
        set(m.plan_json["inputs"]) == {"ord"}
        and isinstance(m.plan_json["inputs"]["ord"], int)
        and len(m.plan_json["logic_hash"]) == 64
    )
    c = cur(w, "g_full")
    assert c.applied_seq == 1 and c.input_versions == m.plan_json["inputs"] and c.applied_batch_id == res.batch_id
    rc = w.receipts.get(res.batch_id, "GOLD")
    assert rc is not None and rc["rows_written"] == 2 and rc["content_checksum"].startswith("2:")
    assert w.run_gold("g_full").status == "NO_DATA"  # same input versions, same logic: nothing to do, no manifest, no commit
    assert len(w.manifests.list("g_full", "GOLD:main")) == 1
    w.seed("ord", [r(4, "d", "7")])
    w.run("ord")
    res2 = w.run_gold("g_full")
    assert res2.status == "DONE" and res2.rows_applied == 3 and cur(w, "g_full").applied_seq == 2
    assert w.store.select("run_object", {"object_code": "g_full"})[0]["layer"] == "GOLD"


def test_typed_parameters_are_values_never_sql_text():
    assert typed_params({"a": {"type": "decimal", "value": "1.50"}, "b": {"type": "int", "value": 3}, "c": {"type": "bool", "value": "true"},
                         "d": {"type": "date", "value": "2026-01-02"}, "e": {"type": "string", "value": "x'; DROP TABLE t; --"}})["a"] == Decimal("1.50")  # fmt: skip
    with pytest.raises(Exception):  # a decimal parameter cannot carry SQL text
        typed_params({"a": {"type": "decimal", "value": "0; DROP TABLE t"}})


# ---- KEY_MERGE ---------------------------------------------------------------------------------------------------------------------------
def test_key_merge_upserts_and_leaves_stale_rows(w):
    w.seed("cust", [r(1, "alice"), r(2, "bob")])
    w.run("cust")
    assert w.run_gold("g_merge").status == "DONE"
    rows = {x["id"]: x for x in w.table("gold.g_merge")}
    assert (
        rows[1]["cust_name"] == "ALICE" and rows[2]["cust_name"] == "BOB" and len(rows) == 4
    )  # ord has ids 1..4 (see FULL test)
    w.seed("cust", [r(1, "alicia")])
    w.run("cust")
    assert w.run_gold("g_merge").status == "DONE"
    assert {x["id"]: x["cust_name"] for x in w.table("gold.g_merge")}[1] == "ALICIA"
    # a Silver soft delete removes the row from the result, but KEY_MERGE does not remove stale target rows (documented; use FULL)
    w.seed("ord", [{"Id": 4, "Name": None, "Amount": None, "Modified": None}], op="D")
    w.run("ord")
    assert w.run_gold("g_merge").status == "DONE"
    assert 4 in {x["id"] for x in w.table("gold.g_merge")}


def test_key_merge_rejects_a_result_with_duplicate_keys(w):
    res = w.run_gold("g_dup")
    assert res.status == "FAILED" and res.error_class == "DATA" and cur(w, "g_dup") is None
    assert not w.spark.catalog.tableExists("gold.g_dup")


# ---- PySpark / multi-step ---------------------------------------------------------------------------------------------------------------
def test_checksum_verified_pyspark_file_runs(w):
    assert w.run_gold("g_py").status == "DONE"
    assert {x["id"]: x["shout"] for x in w.table("gold.g_py")}[1] == "A"


def test_steps_chain_through_step_placeholders(w):
    res = w.run_gold("g_multi")
    assert res.status == "DONE"
    assert w.table("gold.g_multi")[0]["total"] == Decimal("21.00")  # 10 + 5 - 1 + 7 (a Silver tombstone keeps its last values)


# ---- trust controls ------------------------------------------------------------------------------------------------------------------------
def test_modified_file_is_refused_because_it_no_longer_matches_the_approved_checksum(w):
    f = w.pack / "transform_files" / "gold" / "g_tamper_1.sql"
    f.write_text("SELECT id FROM {{ input.ord }} -- edited after review\n")
    res = w.run_gold("g_tamper")
    assert res.status == "FAILED" and res.error_code == "ACC-7002" and "checksum" in (res.message or "")
    assert not w.spark.catalog.tableExists("gold.g_tamper")


def test_non_query_sql_is_refused_before_anything_runs(w):
    before = w.spark.table("silver.ord").count()
    res = w.run_gold("g_evil")
    assert res.status == "FAILED" and res.error_class == "CONFIG" and w.spark.table("silver.ord").count() == before
    assert not w.spark.catalog.tableExists("gold.g_evil")


def test_path_traversal_and_missing_transform_root_are_refused(tmp_path):
    from de_accelerator.processing.gold.loader import read_artifact

    (tmp_path / "ok.sql").write_text("SELECT 1")
    outside = tmp_path.parent / "outside.sql"
    outside.write_text("SELECT 2")
    for ref in ("../outside.sql", "/etc/passwd"):
        with pytest.raises(TransformConfigError):
            read_artifact(str(tmp_path), ref, "x")
    link = tmp_path / "link.sql"
    link.symlink_to(outside)
    with pytest.raises(TransformConfigError, match="outside"):
        read_artifact(str(tmp_path), "link.sql", "x")
    with pytest.raises(TransformConfigError, match="transform_root"):
        read_artifact(None, "ok.sql", "x")
    with pytest.raises(TransformConfigError, match="no recorded checksum"):
        read_artifact(str(tmp_path), "ok.sql", None)


# ---- version pinning + restart consistency ------------------------------------------------------------------------------------------------------
def test_recovery_reads_the_pinned_versions_not_the_newest_ones(w, spark):
    f = FaultInjector({"U_CP2_after_manifest": 1})
    crashing = GoldEngine(w.store, spark, w.gold.cfg, w.clock, f)
    with pytest.raises(InjectedCrash):
        crashing.run_object(w.reader, w.snap, "m3", "g_pin", "r-crash")
    pinned = w.manifests.list("g_pin", "GOLD:main")[0].plan_json["inputs"]["ord"]
    w.seed("ord", [r(99, "late-arrival", "1")])  # Silver moves on while the Gold unit is open
    w.run("ord")
    res = w.run_gold("g_pin")  # recovery of the open unit
    assert res.status == "DONE" and res.recovered
    assert 99 not in {x["id"] for x in w.table("gold.g_pin")}  # built from the pinned version
    assert cur(w, "g_pin").input_versions == {"ord": pinned}
    res2 = w.run_gold("g_pin")  # next run sees the newer head and picks the row up
    assert res2.status == "DONE" and 99 in {x["id"] for x in w.table("gold.g_pin")}


# ---- dependency-aware runner --------------------------------------------------------------------------------------------------------------------
def test_runner_orders_levels_and_skips_hard_dependents_of_failed_upstreams(w):
    w.seed("ordf", [r(1, "x", "bad-amount")])  # cast error under on_cast_error=FAIL -> Silver unit fails
    s = w.runner.run(objects=("ordf", "g_dep", "g_soft"))
    by = {(x.layer, x.object_code): x for x in s.results}
    assert by[("SILVER", "ordf")].status == "FAILED"
    assert by[("GOLD", "g_dep")].status == "SKIPPED_UPSTREAM_FAILED"  # HARD dependent of a failed upstream
    assert by[("GOLD", "g_soft")].status != "SKIPPED_UPSTREAM_FAILED"  # SOFT dependent still runs (on the current Silver state)
    assert s.status in ("PARTIAL", "FAILED")
    skipped = [x for x in w.store.select("run_object", {"run_id": s.run_id}) if x["status"] == "SKIPPED_UPSTREAM_FAILED"]
    assert skipped and skipped[0]["layer"] == "GOLD"


def test_levels_detect_cycles_and_order_dependencies():
    from de_accelerator.processing.runner import levels

    a, b, c = ("SILVER", "p", "a"), ("GOLD", "p", "b"), ("GOLD", "p", "c")
    assert levels([c, b, a], {b: {(a, True)}, c: {(b, True)}}) == [[a], [b], [c]]
    with pytest.raises(TransformConfigError, match="cycle"):
        levels([b, c], {b: {(c, True)}, c: {(b, True)}})


def test_transform_cli_runs_status_and_replan(w, delta_audit, tmp_path):
    cfg = tmp_path / "boot.yaml"
    cfg.write_text(f"client_code: acme\nenvironment: DEV\nflavor: LOCAL\nwarehouse_dir: {tmp_path}\ntransform_root: {w.pack}\n")
    lines: list[str] = []
    argv = ["--bootstrap", str(cfg), "--json"]
    w.seed("cust", [r(7, "cli-user")])
    rc = cli_run([*argv, "transform", "--objects", "cust,g_cli"], store_factory=lambda c: delta_audit, out=lines.append)
    assert rc == 0, lines
    import json

    data = json.loads(lines[-1])
    assert data["status"] == "SUCCEEDED" and {(u["layer"], u["object"]) for u in data["units"]} == {
        ("SILVER", "cust"),
        ("GOLD", "g_cli"),
    }
    lines.clear()
    assert (
        cli_run(
            [*argv, "transform-status", "--object", "cust", "--layer", "SILVER"],
            store_factory=lambda c: delta_audit,
            out=lines.append,
        )
        == 0
    )
    st = json.loads(lines[-1])
    assert st["cursor"] >= 1 and st["units"][-1]["state"] == "DONE"
    lines.clear()
    rc = cli_run(
        [*argv, "transform-replan", "--object", "cust", "--layer", "SILVER", "--reason", "x"],
        store_factory=lambda c: delta_audit,
        out=lines.append,
    )  # nothing is open: the operator action is refused, not silently ignored
    assert rc == 1 and "no open unit" in lines[-1]


@pytest.mark.parametrize("code, method", [("g_vfull", "_full"), ("g_vmerge", "_key_merge")])
def test_gold_verification_fails_on_missing_content_and_recovery_heals(w, monkeypatch, code, method):
    w.clock.advance(days=2)  # the CLI test above used the real system clock for its leases; step past them
    w.seed("cust", [r(31, "v-one"), r(32, "v-two")])
    assert w.run("cust").status == "DONE"
    real = getattr(GoldEngine, method)
    lost: list[int] = []

    def lossy(self, c, out, *a):
        applied = real(self, c, out, *a)
        lost.append(self.spark.sql(f"DELETE FROM {c.fq} WHERE id = 32").collect()[0][0])  # content lost after the write
        return applied

    monkeypatch.setattr(GoldEngine, method, lossy)
    res = w.run_gold(code)
    assert lost == [1], "the simulated loss must actually remove a row"
    assert res.status == "FAILED" and res.error_code == "ACC-7003" and cur(w, code) is None
    monkeypatch.setattr(GoldEngine, method, real)
    res2 = w.run_gold(code)  # the open unit is recovered: written again from the pinned versions and verified
    assert (
        res2.status == "DONE"
        and res2.recovered
        and 32 in {x["id"] for x in w.table(f"gold.{code}")}
        and cur(w, code).applied_seq == 1
    )
