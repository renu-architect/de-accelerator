"""Every validation rule has a negative test (mutating the clean example pack); a meta-test enforces full coverage."""

import shutil
from pathlib import Path

import pytest

from de_accelerator.metadata.validation import RULES, validate_pack
from de_accelerator.metadata.validation import engine as vengine
from tests.packutil import EXAMPLE, copy_pack, edit_items, edit_yaml

P = "projects/sales/"
OBJ, COL, LS, LM, TR = (P + "objects/objects.yaml", P + "objects/columns.yaml", P + "load/load_strategies.yaml",
                        P + "load/layer_mappings.yaml", P + "load/transformations.yaml")  # fmt: skip
DQ, RC, DEP, EX, SRC = (P + "quality/dq.yaml", P + "quality/recon.yaml", P + "orchestration/dependencies.yaml",
                        P + "orchestration/executions.yaml", P + "sources/sources.yaml")  # fmt: skip
ENV, RET, SK, SEM, TGT = (P + "config/environments.yaml", P + "governance/retention.yaml", P + "governance/subject_keys.yaml",
                          P + "semantic/semantic.yaml", P + "semantic/targets.yaml")  # fmt: skip
RETRY = P + "config/retry.yaml"


def item(items, **match):
    return next(i for i in items if all(i.get(k) == v for k, v in match.items()))


def mut(rel, fn):
    return lambda root: edit_items(root, rel, fn)


def manifest(**kw):
    return lambda root: edit_yaml(root / "pack.yaml", lambda d: d.update(kw))


# code -> (mutation(root), validate kwargs)
CASES: dict[str, tuple] = {
    "MDV-2001": (mut(OBJ, lambda it: item(it, code="orders").update(source="nope")), {}),
    "MDV-2002": (lambda root: shutil.rmtree(root / "projects"), {}),
    "MDV-2003": (mut(ENV, lambda it: it.remove(item(it, code="DEV"))), {}),
    "MDV-2004": (mut(ENV, lambda it: item(it, code="DEV").update(catalog_name="wrong")), {}),
    "MDV-2005": (mut(OBJ, lambda it: item(it, code="orders").update(business_keys=["Nope"])), {}),
    "MDV-2006": (mut(DQ, lambda it: item(it, code="ord_nn_01").update(columns=["Nope"])), {}),
    "MDV-2007": (mut(COL, lambda it: item(it, object="orders", name="Amount").update(ordinal=1)), {}),
    "MDV-2008": (mut(COL, lambda it: item(it, object="orders", name="CustomerID").pop("fk_column")), {}),
    "MDV-2009": (mut(SK, lambda it: it[0].update(column_name="Nope")), {}),
    "MDV-2010": (mut(TR, lambda it: it[0].update(body_ref="transform_files/missing.sql")), {}),
    "MDV-2011": (mut(TR, lambda it: it[0].update(checksum="abc")), {}),
    "MDV-2012": (mut(SEM, lambda it: it[0].update(object="orders")), {}),
    "MDV-2013": (mut(COL, lambda it: it.append({"object": "orders", "name": "orderid", "ordinal": 9})), {}),
    "MDV-2014": (mut(COL, lambda it: item(it, object="orders", name="CustomerID").update(is_pk=True)), {}),
    "MDV-3001": (mut(LS, lambda it: item(it, object="orders").pop("watermark_column")), {}),
    "MDV-3002": (mut(LS, lambda it: item(it, object="orders").update(load_type="CDC")), {}),
    "MDV-3003": (mut(LS, lambda it: item(it, object="orders").update(load_type="FILE_INCR")), {}),
    "MDV-3004": (mut(LS, lambda it: item(it, object="orders").update(load_type="API")), {}),
    "MDV-3005": (lambda root: (edit_items(root, OBJ, lambda it: item(it, code="customers").update(business_keys=[], primary_keys=[])),
                               edit_items(root, LM, lambda it: item(it, object="customers", layer="SILVER").update(dedupe_keys=[]))), {}),
    "MDV-3006": (mut(LM, lambda it: item(it, object="orders", layer="SILVER").update(scd_columns=[])), {}),
    "MDV-3007": (mut(DEP, lambda it: it.append({"object": "orders", "depends_on": "orders"})), {}),
    "MDV-3008": (mut(DEP, lambda it: it.append({"object": "customers", "depends_on": "orders"})), {}),
    "MDV-3009": (mut(LM, lambda it: it.remove(item(it, object="orders", layer="SILVER"))), {}),
    "MDV-3010": (mut(LM, lambda it: it.append({"object": "orders", "layer": "GOLD", "target_table": "gold.x"})), {}),
    "MDV-3011": (mut(DQ, lambda it: item(it, code="ord_rng_01").update(threshold_pct=150)), {}),
    "MDV-3012": (mut(DQ, lambda it: item(it, code="ord_rng_01").update(params={})), {}),
    "MDV-3013": (mut(DQ, lambda it: item(it, code="ord_nn_01").update(severity="INFO")), {}),
    "MDV-3014": (mut(RC, lambda it: item(it, code="rc_cnt_01").update(tolerance_abs=-1)), {}),
    "MDV-3015": (mut(RC, lambda it: item(it, code="rc_sum_01").update(on_fail="WARN")), {}),
    "MDV-3016": (mut(LS, lambda it: item(it, object="orders").update(backfill_from="2026-01-01")), {}),
    "MDV-3017": (mut(RETRY, lambda it: it[0].update(max_attempts=0)), {}),
    "MDV-3018": (mut(EX, lambda it: it[0].update(scope={})), {}),
    "MDV-3019": (mut(EX, lambda it: it[0].update(scope={"objects": ["ghost"]})), {}),
    "MDV-3020": (mut(RET, lambda it: it[0].update(retain_days=0)), {}),
    "MDV-3022": (mut(SK, lambda it: it.clear()), {}),
    "MDV-3023": (mut(RET, lambda it: it.clear()), {}),
    "MDV-3024": (mut(TR, lambda it: it.append({**it[0], "step_no": 3})), {}),
    "MDV-3025": (mut(TR, lambda it: it[0].pop("body_ref")), {}),
    "MDV-3026": (mut(LS, lambda it: item(it, object="orders").update(load_type="CDC", cdc_op_column="op", cdc_seq_column="seq")), {}),
    "MDV-3027": (mut(LS, lambda it: item(it, object="orders").update(lookback_window="two hours")), {}),
    "MDV-3028": (lambda root: (edit_items(root, LM, lambda it: item(it, object="customers", layer="SILVER").pop("target_table")),
                               edit_items(root, OBJ, lambda it: item(it, code="customers").pop("target_silver"))), {}),
    "MDV-3029": (mut(TGT, lambda it: it[0].update(entities=[])), {}),
    "MDV-3030": (mut(ENV, lambda it: item(it, code="DEV").update(default_concurrency=0)), {}),
    "MDV-3033": (mut(SRC, lambda it: it[0].update(connection={})), {}),
    "MDV-3034": (mut(LS, lambda it: it.remove(item(it, object="customers"))), {}),
    "MDV-4001": (mut(SRC, lambda it: it[0]["connection"].update(password="hunter2")), {}),
    "MDV-4002": (mut(SRC, lambda it: it[0]["options"].update(note="Server=x;Password=abc123")), {}),
    "MDV-4003": (mut(SRC, lambda it: it[0]["connection"].update(user="kv:bad name!")), {}),
    "MDV-4004": (mut(COL, lambda it: item(it, name="Email").pop("classification")), {}),
    "MDV-4005": (mut(COL, lambda it: item(it, name="Email").pop("mask_policy")), {}),
    "MDV-4006": (mut(OBJ, lambda it: item(it, code="customers").update(description="test 123-45-6789")), {}),
    "MDV-4007": (mut(OBJ, lambda it: item(it, code="customers").update(sensitivity="public")), {}),
    "MDV-4008": (mut(COL, lambda it: item(it, name="Amount").update(is_pii=True, classification="confidential")), {}),
    "MDV-4009": (mut(ENV, lambda it: item(it, code="PROD").pop("secret_scope")), {"env": "PROD"}),
    "MDV-6001": (mut(SRC, lambda it: it[0].update(connector_type="rest")), {}),
    "MDV-6002": (mut(LM, lambda it: item(it, object="orders", layer="SILVER").update(target_table="gold.orders")), {}),
    "MDV-6003": (mut(COL, lambda it: it.append({"object": "orders", "name": "_hidden", "ordinal": 9})), {}),
    "MDV-6010": (mut(SRC, lambda it: it[0]["connection"].pop("password")), {}),
    "MDV-5001": (lambda root: None, {"core_version": "2.0.0"}),
    "MDV-5002": (manifest(metadata_schema_version=3), {}),
    "MDV-5003": (manifest(pack_version="v1"), {}),
    "MDV-5004": (manifest(pack_format=2), {}),
    "MDV-5005": (lambda root: None, {"deprecated_keys": {"object.priority": "use 'tags.priority'"}}),
    "MDV-5006": (manifest(environments=["QA", "PROD"]), {}),
}
# rules whose mutation only fires when the object field is non-default
CASES["MDV-5005"] = (mut(OBJ, lambda it: item(it, code="orders").update(priority=5)), CASES["MDV-5005"][1])


def run(root: Path, env="DEV", **kw):
    return validate_pack(root, env, **kw)[0]


def test_clean_example_has_no_findings():
    for env in ("DEV", "QA", "PROD"):
        r = validate_pack(EXAMPLE, env)[0]
        assert r.ok and r.findings == [], r.to_text()


def test_every_rule_has_a_negative_test():
    assert {r.code for r in RULES} == set(CASES), "rule catalogue and CASES are out of sync"
    assert len(RULES) >= 64
    assert len({r.code for r in RULES}) == len(RULES)
    assert all(r.layer in {"REFERENTIAL", "SEMANTIC", "POLICY", "COMPAT"} for r in RULES)


@pytest.mark.parametrize("code", sorted(CASES))
def test_rule_fires(tmp_path, code):
    mutate, kw = CASES[code]
    root = copy_pack(tmp_path)
    mutate(root)
    kw = dict(kw)
    env = kw.pop("env", "DEV")
    report = run(root, env, **kw)
    assert code in report.codes(), f"{code} not raised; got:\n{report.to_text()}"
    sev = next(r.severity for r in RULES if r.code == code)
    assert any(f.code == code and f.severity == sev for f in report.findings)
    assert report.ok == (sev != "ERROR" and not report.errors)


def test_deployed_schema_check(tmp_path):
    r = run(copy_pack(tmp_path), deployed_schema_version=0)
    assert "MDV-5002" in r.codes()


def test_syntax_errors_short_circuit_rules(tmp_path):
    root = copy_pack(tmp_path)
    edit_items(root, OBJ, lambda it: it[0].update(bogus=1))
    r = run(root)
    assert not r.ok and r.codes() == {"MDV-1002"}  # semantic rules not run on a syntactically broken pack


def test_severity_override_and_disable(tmp_path):
    root = copy_pack(tmp_path)
    edit_items(root, COL, lambda it: item(it, name="Email").pop("mask_policy"))
    assert run(root).ok  # MDV-4005 is a warning by default
    r = run(root, options={"severity_overrides": {"MDV-4005": "ERROR"}})
    assert not r.ok and "MDV-4005" in r.codes()
    assert "MDV-4005" not in run(root, options={"disabled_rules": ["MDV-4005"]}).codes()


def test_secret_value_never_echoed(tmp_path):
    root = copy_pack(tmp_path)
    edit_items(root, SRC, lambda it: it[0]["options"].update(note="Server=x;Password=SuperSecretValue99"))
    text = run(root).to_text() + run(root).to_json()
    assert "SuperSecretValue99" not in text


def test_card_like_value_detected_but_plain_numbers_ok(tmp_path):
    root = copy_pack(tmp_path)
    edit_items(root, OBJ, lambda it: item(it, code="orders").update(description="order 4111 1111 1111 1111 sample"))
    assert "MDV-4006" in run(root).codes()
    root2 = copy_pack(tmp_path / "b")
    edit_items(root2, OBJ, lambda it: item(it, code="orders").update(description="batch 1234567890123456 id"))  # fails Luhn
    assert "MDV-4006" not in run(root2).codes()


def test_report_formats(tmp_path):
    root = copy_pack(tmp_path)
    edit_items(root, LS, lambda it: item(it, object="orders").pop("watermark_column"))
    r = run(root)
    assert "MDV-3001" in r.to_text() and '"MDV-3001"' in r.to_json()
    d = r.to_dict()
    assert d["ok"] is False and d["errors"] >= 1 and d["stats"]["rules_run"] == len(RULES)


def test_engine_rejects_duplicate_rule_codes():
    with pytest.raises(ValueError):
        vengine.rule("MDV-3001", "ERROR", "SEMANTIC", "dup")(lambda ctx: iter(()))


def test_connector_specific_checks_cover_options_and_watermark_types(tmp_path):
    root = copy_pack(tmp_path)
    edit_items(root, SRC, lambda it: it[0]["options"].update(bogus_option=1))
    edit_items(root, LS, lambda it: item(it, object="orders").update(watermark_type="uuid", read_options={"nope": 1}))
    msgs = " | ".join(f.message for f in run(root).findings if f.code == "MDV-6010")
    assert "bogus_option" in msgs and "watermark_type 'uuid'" in msgs and "read_options key 'nope'" in msgs


def test_hostile_identifiers_are_rejected_in_metadata(tmp_path):
    root = copy_pack(tmp_path)
    edit_items(root, OBJ, lambda it: item(it, code="orders").update(object_name="Orders; DROP TABLE x--"))
    assert any("unsafe or invalid SQL identifier" in f.message for f in run(root).findings if f.code == "MDV-6010")
