"""Reader contract: ONE suite, three implementations (store-backed, snapshot-backed, in-memory)."""

import ast
import dataclasses
from pathlib import Path

import pytest

from de_accelerator.core.errors import NotFoundError
from de_accelerator.metadata.reader import (
    MODELS,
    DeltaMetadataReader,
    InMemoryMetadataReader,
    SnapshotMetadataReader,
)
from de_accelerator.metadata.reader.source import StoreConfigSource
from de_accelerator.metadata.release import PackLoader, ReleaseManager
from de_accelerator.metadata.schema.entities import ENTITIES
from de_accelerator.metadata.service import MetadataService
from de_accelerator.metadata.snapshot import SnapshotBuilder, SnapshotStore
from tests.unit.test_release import ENV, make_bundle, migrated

P = "sales"


@pytest.fixture(scope="module")
def world():
    s = migrated()
    b = make_bundle()
    rel = PackLoader(s, ENV).stage(b)
    ReleaseManager(s, ENV).activate(rel["release_id"])
    snap = SnapshotBuilder(StoreConfigSource(s, ENV)).build()
    SnapshotStore(s).persist(snap)
    return s, b, rel, snap


@pytest.fixture(params=["delta", "snapshot", "memory"])
def reader(request, world):
    s, b, rel, snap = world
    if request.param == "delta":
        return DeltaMetadataReader(s, ENV)
    if request.param == "snapshot":
        return SnapshotMetadataReader(SnapshotStore(s).load(snap.snapshot_id))
    return InMemoryMetadataReader.from_bundle(b, rel["release_id"])


def test_identity(reader, world):
    _, _, rel, _ = world
    assert reader.release_id == rel["release_id"] and reader.env == ENV
    assert reader.client().code == "acme" and [p.code for p in reader.projects()] == [P]


def test_objects_and_typed_fields(reader):
    objs = reader.objects(P)
    assert [o.code for o in objs] == ["customers", "fact_sales", "orders"] or {o.code for o in objs} == {"customers", "orders", "fact_sales"}
    o = reader.object(P, "orders")
    assert o.__class__.__name__ == "ObjectConfig" and dataclasses.is_dataclass(o)
    assert o.business_keys == ("OrderID",) and isinstance(o.enabled_layers, tuple) and o.delete_mode == "soft"
    assert o.personal_data is False and o.tags == {} and o.priority == 100
    with pytest.raises(dataclasses.FrozenInstanceError):
        o.priority = 1  # type: ignore[misc]
    assert [x.code for x in reader.objects(P, layer="GOLD")] == ["fact_sales"]
    assert {x.code for x in reader.objects(P, layer="BRONZE")} == {"customers", "orders"}
    with pytest.raises(NotFoundError):
        reader.object(P, "ghost")


def test_children_and_ordering(reader):
    assert [c.name for c in reader.columns(P, "orders")] == ["OrderID", "CustomerID", "Amount", "ModifiedDate"]
    ls = reader.load_strategy(P, "orders")
    assert ls.load_type == "INCREMENTAL" and ls.watermark_column == "ModifiedDate" and ls.on_retention_gap == "FAIL"
    assert ls.cdc_retention_check is True and ls.page_config == {}
    assert [m.layer for m in reader.layer_mappings(P, "orders")] == ["BRONZE", "SILVER"]
    silver = reader.layer_mappings(P, "orders")[1]
    assert silver.merge_type == "SCD2" and silver.scd_columns == ("Amount",)
    assert [t.step_no for t in reader.transformations(P, "fact_sales")] == [1]
    assert len(reader.transformations(P, "fact_sales")[0].checksum) == 64
    assert [r.code for r in reader.dq_rules(P, "orders")] == ["ord_nn_01", "ord_rng_01"]
    assert reader.dq_rules(P, "orders", layer="BRONZE") == []
    rng = reader.get("dq_rule", project_code=P, object="orders", code="ord_rng_01")
    assert rng.params == {"min": 0, "max": 1000000} and rng.threshold_pct == 0.5
    assert [r.code for r in reader.recon_rules(P, "orders", scope="BRZ_SLV")] == ["rc_sum_01"]
    assert [d.depends_on for d in reader.upstream_of(P, "fact_sales")] == ["orders"]
    assert len(reader.dependencies(P)) == 2


def test_environment_source_execution_and_governance(reader):
    assert reader.environment(P).code == ENV and reader.environment(P).catalog_name == "acme_dev"
    src = reader.source(P, "erp_sql")
    assert src.connection["password"] == "kv:erp-pwd" and "kv:" in src.connection["host"]  # only key vault references
    ex = reader.execution(P, "daily_sales")
    assert ex.scope == {"objects": ["customers", "orders", "fact_sales"]} and ex.retry_policy == "standard"
    assert reader.retry_policy(P, "standard").max_attempts == 3
    assert reader.retention_policies(P, "customers")[0].retain_days == 2555
    assert reader.subject_keys(P, "customers")[0].subject_role == "customer_id"
    assert reader.semantic_entities(P)[0].grain == ("OrderID",) and reader.measures(P, "sales")[0].is_kpi is True


def test_all_kinds_have_models_and_unknown_kind_rejected(reader):
    assert set(MODELS) == {e.kind for e in ENTITIES}
    for e in ENTITIES:
        assert isinstance(reader.all(e.kind), list)
    with pytest.raises(NotFoundError):
        reader.all("nonsense")
    with pytest.raises(NotFoundError):
        reader.get("object", project_code=P)  # ambiguous: more than one match


def test_all_three_readers_agree_exactly(world):
    s, b, rel, snap = world
    d = DeltaMetadataReader(s, ENV)
    sn = SnapshotMetadataReader(SnapshotStore(s).load(snap.snapshot_id))
    m = InMemoryMetadataReader.from_bundle(b, rel["release_id"])
    for e in ENTITIES:
        scope_field = "project_code" if e.project_scoped else "client_code"

        def key(x, fields=(scope_field, *e.key)):
            return tuple(str(getattr(x, f)) for f in fields)

        a, bb, c = (sorted(r.all(e.kind), key=key) for r in (d, sn, m))
        assert a == bb == c, e.kind


def test_explicit_release_reader_and_missing_active_release(world):
    s, _, rel, _ = world
    assert DeltaMetadataReader(s, ENV, rel["release_id"]).release_id == rel["release_id"]
    with pytest.raises(NotFoundError):
        DeltaMetadataReader(migrated(), ENV)  # no active release


def test_no_environment_override_logic_outside_pack_loading():
    """Approved decision: overrides are resolved at pack load only; runtime/readers must not know about them."""
    root = Path(__file__).resolve().parents[2] / "src" / "de_accelerator"
    offenders = []
    for f in root.rglob("*.py"):
        rel = f.relative_to(root).as_posix()
        if rel.startswith("metadata/packs/"):
            continue
        for node in ast.walk(ast.parse(f.read_text())):
            names = []
            if isinstance(node, ast.ImportFrom):
                names = [node.module or ""] + [a.name for a in node.names]
            elif isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            if any("packs.overrides" in n or n == "apply_overrides" for n in names):
                offenders.append(rel)
    assert offenders == []


def test_service_is_read_only_facade(world, tmp_path):
    s, b, rel, snap = world
    svc = MetadataService(s, ENV)
    from tests.packutil import EXAMPLE

    report, bundle = svc.validate(EXAMPLE)
    assert report.ok and bundle is not None and bundle.bundle_sha256 == b.bundle_sha256
    st = svc.release_status()
    assert st["active_release_id"] == rel["release_id"] and st["activation_seq"] == 1
    assert [r["release_id"] for r in svc.list_releases()] == [rel["release_id"]]
    assert len(svc.diff_releases(None, rel["release_id"])) == sum(len(v) for v in b.entities.values())
    assert svc.history("sales/orders")[0]["change_type"] == "ADDED"
    assert svc.reader().object(P, "orders").code == "orders"
    assert not any(n in dir(svc) for n in ("stage", "activate", "rollback", "insert", "update"))  # no write path
