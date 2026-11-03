import gzip

import pytest

from de_accelerator.core.errors import CompatibilityError, NotFoundError, SnapshotIntegrityError, StoreError
from de_accelerator.metadata.packs.ids import canonical_json, sha256_hex
from de_accelerator.metadata.reader.source import SnapshotConfigSource, StoreConfigSource
from de_accelerator.metadata.release import PackLoader, ReleaseManager
from de_accelerator.metadata.schema.entities import ENTITIES
from de_accelerator.metadata.snapshot import SnapshotBuilder, SnapshotScope, SnapshotStore
from tests.unit.test_release import ENV, make_bundle, migrated, total_rows


@pytest.fixture
def env_with_release():
    s = migrated()
    ld, rm = PackLoader(s, ENV), ReleaseManager(s, ENV)
    a = ld.stage(make_bundle())
    rm.activate(a["release_id"])
    return s, a["release_id"]


def build(s, scope=None):
    return SnapshotBuilder(StoreConfigSource(s, ENV)).build(scope)


def test_full_snapshot_is_deterministic_and_binds_release(env_with_release):
    s, rid = env_with_release
    a, b = build(s), build(s)
    assert a.snapshot_id == b.snapshot_id and len(a.snapshot_id) == 64
    assert a.release_id == rid and a.content["release_id"] == rid and a.env == ENV
    assert sum(a.counts().values()) == total_rows(make_bundle())
    assert sha256_hex(canonical_json(a.content)) == a.snapshot_id


def test_scoped_snapshot_contains_only_scope(env_with_release):
    s, _ = env_with_release
    snap = build(s, SnapshotScope(objects=("orders",)))
    e = snap.content["entities"]
    assert [o["code"] for o in e["object"]] == ["orders"]
    assert {c["object"] for c in e["column"]} == {"orders"} and {d["object"] for d in e["dq_rule"]} == {"orders"}
    assert [x["code"] for x in e["source"]] == ["erp_sql"]
    assert e["subject_key"] == [] and e["semantic_entity"] == [] and e["semantic_measure"] == []  # customers / fact_sales not in scope
    assert {d["object"] for d in e["dependency"]} == {"orders"}  # deps owned by in-scope objects (upstream may be out of scope)
    assert e["client"] and e["project"] and e["environment"] and e["execution"]  # shared context always present
    assert build(s, SnapshotScope(objects=("orders",))).snapshot_id == snap.snapshot_id
    assert build(s, SnapshotScope(objects=("customers",))).snapshot_id != snap.snapshot_id


def test_execution_scope_and_unknowns(env_with_release):
    s, _ = env_with_release
    snap = build(s, SnapshotScope(execution="daily_sales"))
    assert {o["code"] for o in snap.content["entities"]["object"]} == {"customers", "orders", "fact_sales"}
    for bad in (SnapshotScope(projects=("nope",)), SnapshotScope(objects=("ghost",)), SnapshotScope(execution="ghost")):
        with pytest.raises(NotFoundError):
            build(s, bad)


def test_persist_load_roundtrip_idempotent_and_deterministic_bytes(env_with_release):
    s, rid = env_with_release
    snap = build(s)
    ss = SnapshotStore(s)
    ref = ss.persist(snap, "tester")
    assert ref.snapshot_id == snap.snapshot_id and ref.release_id == rid
    ss.persist(snap)  # idempotent
    assert s.count("run_config_snapshot") == 1
    loaded = ss.load(snap.snapshot_id)
    assert loaded.content == snap.content and loaded.release_id == rid
    row = s.select("run_config_snapshot")[0]
    assert row["created_by"] == "tester" and row["size_bytes"] > 1000 and row["content_sha256"] == snap.snapshot_id
    other = InMemory = migrated()
    SnapshotStore(other).persist(snap)
    assert other.select("run_config_snapshot")[0]["content_gz"] == row["content_gz"]  # byte-identical
    assert InMemory is other


def test_snapshot_immune_to_later_metadata_changes(env_with_release):
    s, a_id = env_with_release
    ss = SnapshotStore(s)
    ref = ss.persist(build(s))
    b = PackLoader(s, ENV).stage(make_bundle(priority=7))
    ReleaseManager(s, ENV).activate(b["release_id"])
    assert ReleaseManager(s, ENV).active_release_id() == b["release_id"]
    snap = ss.load(ref.snapshot_id)
    assert snap.release_id == a_id
    assert next(o for o in snap.content["entities"]["object"] if o["code"] == "orders")["priority"] == 100
    new = build(s)
    assert new.release_id == b["release_id"] and new.snapshot_id != ref.snapshot_id  # identical scope, different release


def test_snapshot_source_matches_store_source(env_with_release):
    s, _ = env_with_release
    snap = build(s)
    src, store_src = SnapshotConfigSource(snap), StoreConfigSource(s, ENV)
    for e in ENTITIES:
        assert src.rows(e.kind) == store_src.rows(e.kind)
    assert src.release_id == store_src.release_id and src.snapshot_id == snap.snapshot_id


def _tamper(store, snapshot_id, **changes):
    for r in store._data["run_config_snapshot"]:  # bypass append-only on purpose: simulates storage-level tampering
        if r["snapshot_id"] == snapshot_id:
            r.update(changes)


def test_tampering_is_detected(env_with_release):
    s, _ = env_with_release
    snap = build(s)
    ss = SnapshotStore(s)
    ss.persist(snap)
    good = s.select("run_config_snapshot")[0]["content_gz"]

    altered = dict(snap.content)
    altered["env"] = "PROD"
    _tamper(s, snap.snapshot_id, content_gz=gzip.compress(canonical_json(altered).encode(), mtime=0))
    with pytest.raises(SnapshotIntegrityError, match="hash mismatch"):
        ss.load(snap.snapshot_id)

    _tamper(s, snap.snapshot_id, content_gz=b"not gzip at all")
    with pytest.raises(SnapshotIntegrityError, match="corrupt"):
        ss.load(snap.snapshot_id)

    _tamper(s, snap.snapshot_id, content_gz=good, release_id="other-release")
    with pytest.raises(SnapshotIntegrityError, match="disagree"):
        ss.load(snap.snapshot_id)

    _tamper(s, snap.snapshot_id, release_id=snap.release_id)
    assert ss.load(snap.snapshot_id).content == snap.content  # restored => loads again


def test_persist_refuses_inconsistent_snapshot(env_with_release):
    s, _ = env_with_release
    snap = build(s)
    from dataclasses import replace

    with pytest.raises(SnapshotIntegrityError):
        SnapshotStore(s).persist(replace(snap, snapshot_id="0" * 64))


def test_snapshot_table_is_append_only(env_with_release):
    s, _ = env_with_release
    SnapshotStore(s).persist(build(s))
    with pytest.raises(StoreError, match="append-only"):
        s.update_where("run_config_snapshot", {"created_by": "x"}, {})
    with pytest.raises(StoreError, match="append-only"):
        s.delete_where("run_config_snapshot", {})
    with pytest.raises(NotFoundError):
        SnapshotStore(s).load("f" * 64)


def test_build_enforces_compatibility(env_with_release):
    s, rid = env_with_release
    s.update_where("cfg_pack_release", {"requires_core": ">=9.0"}, {"release_id": rid})
    with pytest.raises(CompatibilityError):
        build(s)
