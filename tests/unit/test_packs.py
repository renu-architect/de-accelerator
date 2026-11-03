import json

import pytest
import yaml

from de_accelerator.core.errors import PackError
from de_accelerator.metadata.packs.bundle import PackBundle, build_bundle
from de_accelerator.metadata.packs.ids import canonical_json, entity_id
from tests.packutil import EXAMPLE, codes, copy_pack, edit_items, edit_yaml

OBJ = "projects/sales/objects/objects.yaml"


def test_example_builds_clean_for_all_envs():
    for env in ("DEV", "QA", "PROD"):
        b, f = build_bundle(EXAMPLE, env)
        assert b is not None and f == []
        assert b.counts()["environment"] == 1 and b.rows("environment")[0]["code"] == env


def test_env_overrides_resolved_at_load_deep_merge():
    dev, _ = build_bundle(EXAMPLE, "DEV")
    qa, _ = build_bundle(EXAMPLE, "QA")
    prod, _ = build_bundle(EXAMPLE, "PROD")
    src = lambda b: b.rows("source")[0]  # noqa: E731
    assert src(dev)["connection"]["host"] == "kv:erp-host"
    assert src(qa)["connection"]["host"] == "kv:erp-host-qa" and src(qa)["connection"]["user"] == "kv:erp-user"  # merged
    assert src(prod)["options"]["fetchsize"] == 50000
    assert len({dev.bundle_sha256, qa.bundle_sha256, prod.bundle_sha256}) == 3


def test_deterministic_bytes_and_ids():
    a, _ = build_bundle(EXAMPLE, "DEV")
    b, _ = build_bundle(EXAMPLE, "DEV")
    assert a.to_json() == b.to_json() and a.bundle_sha256 == b.bundle_sha256
    qa, _ = build_bundle(EXAMPLE, "QA")
    ids = lambda bundle: {r["object_id"] for r in bundle.rows("object")}  # noqa: E731
    assert ids(a) == ids(qa)  # same entity => same id across envs
    assert entity_id("acme", "object", "sales", ["orders"]) == entity_id("acme", "object", "sales", ["orders"])
    assert entity_id("acme", "object", "sales", ["orders"]) != entity_id("other", "object", "sales", ["orders"])


def test_hash_ignores_yaml_formatting_but_not_content(tmp_path):
    root = copy_pack(tmp_path)
    base, _ = build_bundle(root, "DEV")
    p = root / OBJ
    p.write_text("# reformatted\n" + yaml.safe_dump(yaml.safe_load(p.read_text()), sort_keys=True, default_flow_style=True))
    same, _ = build_bundle(root, "DEV")
    assert same.bundle_sha256 == base.bundle_sha256
    edit_items(root, OBJ, lambda items: items[0].update(priority=7))
    changed, _ = build_bundle(root, "DEV")
    assert changed.bundle_sha256 != base.bundle_sha256


def test_defaults_and_transform_checksum_filled():
    b, _ = build_bundle(EXAMPLE, "DEV")
    o = next(r for r in b.rows("object") if r["code"] == "orders")
    assert o["delete_mode"] == "soft" and o["partition_cols"] == [] and o["personal_data"] is False
    t = b.rows("transformation")[0]
    assert t["checksum"] == b.files[t["body_ref"]] and len(t["checksum"]) == 64


def test_bundle_roundtrip_and_tamper(tmp_path):
    b, _ = build_bundle(EXAMPLE, "DEV")
    b.write(tmp_path / "b.json")
    assert PackBundle.read(tmp_path / "b.json").bundle_sha256 == b.bundle_sha256
    assert (tmp_path / "b.json.sha256").read_text().strip() == b.bundle_sha256
    d = json.loads(b.to_json())
    d["entities"]["source"][0]["source_type"] = "oracle"
    with pytest.raises(PackError, match="hash mismatch"):
        PackBundle.from_json(json.dumps(d))
    with pytest.raises(PackError):
        PackBundle.from_json("{not json")


def test_canonical_json_stable():
    assert canonical_json({"b": 1, "a": [3, 2]}) == canonical_json({"a": [3, 2], "b": 1}) == '{"a":[3,2],"b":1}'
    with pytest.raises(ValueError):
        canonical_json({"x": float("nan")})


# ---- syntax findings -------------------------------------------------------------------------------
def test_missing_manifest(tmp_path):
    b, f = build_bundle(tmp_path, "DEV")
    assert b is None and codes(f) == {"MDV-1005"}


def test_bad_manifest(tmp_path):
    root = copy_pack(tmp_path)
    edit_yaml(root / "pack.yaml", lambda d: d.update(pack_name="Bad Name", surprise=1))
    b, f = build_bundle(root, "DEV")
    assert b is None and codes(f) == {"MDV-1005"}


def test_yaml_syntax_error(tmp_path):
    root = copy_pack(tmp_path)
    (root / OBJ).write_text("kind: object\nitems: [unclosed\n")
    _, f = build_bundle(root, "DEV")
    assert "MDV-1001" in codes(f)


def test_schema_violations(tmp_path):
    root = copy_pack(tmp_path)
    edit_items(root, OBJ, lambda items: (items[0].update(sensitivity="top-secret", bogus=1), items[1].pop("source")))
    _, f = build_bundle(root, "DEV")
    msgs = " ".join(x.message for x in f if x.code == "MDV-1002")
    assert "top-secret" in msgs and "bogus" in msgs and "source" in msgs


def test_unknown_kind_and_bad_file_shape(tmp_path):
    root = copy_pack(tmp_path)
    (root / "projects/sales/x.yaml").write_text("kind: nonsense\nitems: []\n")
    (root / "projects/sales/y.yaml").write_text("- just a list\n")
    _, f = build_bundle(root, "DEV")
    assert codes(f) == {"MDV-1003"} and len(f) >= 2


def test_duplicate_key(tmp_path):
    root = copy_pack(tmp_path)
    edit_items(root, OBJ, lambda items: items.append(dict(items[0])))
    _, f = build_bundle(root, "DEV")
    assert "MDV-1004" in codes(f)


def test_missing_project_yaml(tmp_path):
    root = copy_pack(tmp_path)
    (root / "projects/sales/project.yaml").unlink()
    _, f = build_bundle(root, "DEV")
    assert "MDV-1007" in codes(f)


# ---- override findings -----------------------------------------------------------------------------
@pytest.mark.parametrize("ov, needle", [
    ({"kind": "source", "project": "sales", "key": {"code": "nope"}, "set": {}}, "matched 0"),
    ({"kind": "source", "key": {"code": "erp_sql"}, "set": {}}, "needs 'project'"),
    ({"kind": "source", "project": "sales", "key": {"code": "erp_sql"}, "set": {"code": "renamed"}}, "key fields"),
    ({"kind": "source", "project": "sales", "key": {"code": "erp_sql"}, "set": {"source_type": "nonsense"}}, "invalid source"),
    ({"kind": "source", "project": "sales", "key": {"bogus": "x"}, "set": {}}, "business-key"),
])
def test_bad_overrides(tmp_path, ov, needle):
    root = copy_pack(tmp_path)
    edit_yaml(root / "overrides/DEV.yaml", lambda d: d.update(overrides=[ov]))
    _, f = build_bundle(root, "DEV")
    assert codes(f) == {"MDV-1006"} and any(needle in x.message for x in f)


def test_override_remove_and_source_not_mutated(tmp_path):
    root = copy_pack(tmp_path)
    edit_yaml(root / "overrides/DEV.yaml", lambda d: d.update(overrides=[
        {"kind": "external_consumer", "project": "sales", "key": {"code": "pbi_sales"}, "op": "remove"}]))
    dev, f = build_bundle(root, "DEV")
    qa, _ = build_bundle(root, "QA")
    assert f == [] and dev.rows("external_consumer") == [] and len(qa.rows("external_consumer")) == 1
