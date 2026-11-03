import copy
import json
import threading

import pytest

from de_accelerator.core.errors import (
    ActivationConflictError,
    CompatibilityError,
    PackError,
    ReleaseError,
    StoreError,
    ValidationFailedError,
)
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.packs.bundle import PackBundle, build_bundle
from de_accelerator.metadata.release import PackLoader, ReleaseManager
from de_accelerator.metadata.release.common import ACTIVE, ROLLED_BACK, STAGED, SUPERSEDED, release_id_for
from de_accelerator.metadata.schema.entities import ENTITIES
from de_accelerator.metadata.store.memory import InMemoryTableStore
from tests.faults import FaultyStore, InjectedCrash
from tests.packutil import EXAMPLE

ENV = "DEV"


def migrated():
    s = InMemoryTableStore()
    Migrator(s, MIGRATIONS).apply()
    return s


def make_bundle(priority: int | None = None, extra_source_opt: str | None = None, env: str = ENV) -> PackBundle:
    b, f = build_bundle(EXAMPLE, env)
    assert b is not None and not f
    b = copy.deepcopy(b)
    if priority is not None:
        for r in b.entities["object"]:
            if r["code"] == "orders":
                r["priority"] = priority
                r["row_hash"] = _rehash(r)
    if extra_source_opt:
        b.entities["source"][0]["options"] = {**b.entities["source"][0]["options"], "tag": extra_source_opt}
        b.entities["source"][0]["row_hash"] = _rehash(b.entities["source"][0])
    b.bundle_sha256 = b.compute_hash()
    return b


def _rehash(row):
    from de_accelerator.metadata.packs.ids import canonical_json, sha256_hex

    return sha256_hex(canonical_json({k: v for k, v in row.items() if k != "row_hash"}))


def total_rows(b: PackBundle) -> int:
    return sum(len(v) for v in b.entities.values())


def table_rows(store, rid):
    return sum(store.count(e.table, {"pack_release_id": rid}) for e in ENTITIES)


# ----------------------------------------------------------------------------------------------------
def test_stage_creates_inactive_verified_release():
    s, b = migrated(), make_bundle()
    rel = PackLoader(s, ENV, "alice").stage(b, git_sha="abc123", approved_by="bob", ticket="T-1")
    assert rel["status"] == STAGED and rel["release_id"] == release_id_for(ENV, b.bundle_sha256)
    assert rel["git_sha"] == "abc123" and rel["approved_by"] == "bob" and rel["loaded_by"] == "alice"
    assert table_rows(s, rel["release_id"]) == total_rows(b)
    assert ReleaseManager(s, ENV).pointer() is None  # staged != active
    obj = s.select("cfg_object", {"pack_release_id": rel["release_id"]})[0]
    assert isinstance(obj["tags"], str) and json.loads(obj["tags"]) == {} and isinstance(obj["enabled_layers"], list)
    assert obj["row_version"] == 1 and obj["created_ts"] is not None


def test_stage_is_idempotent():
    s, b = migrated(), make_bundle()
    ld = PackLoader(s, ENV)
    r1, n1 = ld.stage(b), sum(s.count(e.table) for e in ENTITIES)
    r2 = ld.stage(b)
    assert r1["release_id"] == r2["release_id"] and sum(s.count(e.table) for e in ENTITIES) == n1
    assert s.count("cfg_pack_release") == 1


def test_stage_rejects_invalid_wrong_env_and_tampered():
    s = migrated()
    bad = make_bundle()
    bad.entities["object"][0]["source"] = "ghost"  # dangling reference
    bad.bundle_sha256 = bad.compute_hash()
    with pytest.raises(ValidationFailedError) as e:
        PackLoader(s, ENV).stage(bad)
    assert "MDV-2001" in e.value.context["codes"] and s.count("cfg_pack_release") == 0 and s.count("cfg_object") == 0
    with pytest.raises(PackError, match="env"):
        PackLoader(s, "QA").stage(make_bundle())
    t = make_bundle()
    t.entities["source"][0]["source_type"] = "oracle"  # modified without re-hashing
    with pytest.raises(PackError, match="hash mismatch"):
        PackLoader(s, ENV).stage(t)


def test_first_activation_and_history():
    s = migrated()
    b = make_bundle()
    rel = PackLoader(s, ENV, "alice").stage(b, ticket="T-1")
    rm = ReleaseManager(s, ENV, "alice")
    ptr = rm.activate(rel["release_id"])
    assert ptr["activation_seq"] == 1 and ptr["previous_release_id"] is None and ptr["release_id"] == rel["release_id"]
    assert rm.get_release(rel["release_id"])["status"] == ACTIVE and rm.get_release(rel["release_id"])["activated_ts"]
    hist = s.select("cfg_config_history")
    assert len(hist) == total_rows(b) and {h["change_type"] for h in hist} == {"ADDED"} and hist[0]["ticket"] == "T-1"
    types = [e["event_type"] for e in rm.events(rel["release_id"])]
    assert {"STAGING_STARTED", "STAGED", "ACTIVATE_REQUESTED", "ACTIVATED"} <= set(types)
    assert rm.activate(rel["release_id"])["activation_seq"] == 1  # idempotent


def test_second_release_supersedes_records_modifications_and_row_versions():
    s = migrated()
    ld, rm = PackLoader(s, ENV), ReleaseManager(s, ENV)
    r1 = ld.stage(make_bundle())
    rm.activate(r1["release_id"])
    r2 = ld.stage(make_bundle(priority=7))
    rows2 = {r["code"]: r for r in s.select("cfg_object", {"pack_release_id": r2["release_id"]})}
    assert rows2["orders"]["row_version"] == 2 and rows2["customers"]["row_version"] == 1  # only the changed row bumps
    ptr = rm.activate(r2["release_id"])
    assert ptr["activation_seq"] == 2 and ptr["previous_release_id"] == r1["release_id"]
    assert rm.get_release(r1["release_id"])["status"] == SUPERSEDED and rm.get_release(r2["release_id"])["status"] == ACTIVE
    mod = [h for h in s.select("cfg_config_history", {"release_to": r2["release_id"]})]
    assert len(mod) == 1 and mod[0]["change_type"] == "MODIFIED" and mod[0]["entity_key"] == "sales/orders"
    assert json.loads(mod[0]["before_json"])["priority"] == 100 and json.loads(mod[0]["after_json"])["priority"] == 7


def test_rollback_and_reactivation():
    s = migrated()
    ld, rm = PackLoader(s, ENV), ReleaseManager(s, ENV, "ops")
    r1, r2 = ld.stage(make_bundle()), ld.stage(make_bundle(priority=7))
    rm.activate(r1["release_id"])
    rm.activate(r2["release_id"])
    ptr = rm.rollback(reason="bad priority", ticket="INC-9")
    assert ptr["release_id"] == r1["release_id"] and ptr["activation_seq"] == 3 and ptr["previous_release_id"] == r2["release_id"]
    assert rm.get_release(r1["release_id"])["status"] == ACTIVE and rm.get_release(r2["release_id"])["status"] == ROLLED_BACK
    back = s.select("cfg_config_history", {"release_to": r1["release_id"], "change_type": "MODIFIED"})
    assert len(back) == 1 and json.loads(back[0]["after_json"])["priority"] == 100  # the rollback itself is audited
    assert any(e["event_type"] == "ROLLBACK_REQUESTED" for e in rm.events(r1["release_id"]))
    rm.activate(r2["release_id"])  # a rolled-back release may be re-activated explicitly
    assert rm.active_release_id() == r2["release_id"]
    with pytest.raises(ReleaseError, match="already active"):
        rm.rollback(to_release_id=r2["release_id"])


def test_rollback_without_target_fails():
    s = migrated()
    rm = ReleaseManager(s, ENV)
    with pytest.raises(ReleaseError, match="no active"):
        rm.rollback()
    r1 = PackLoader(s, ENV).stage(make_bundle())
    rm.activate(r1["release_id"])
    with pytest.raises(ReleaseError, match="no previous"):
        rm.rollback()


def test_cannot_activate_unstaged_or_foreign_env():
    s = migrated()
    r1 = PackLoader(s, ENV).stage(make_bundle())
    s.update_where("cfg_pack_release", {"status": "STAGING"}, {"release_id": r1["release_id"]})
    with pytest.raises(ReleaseError, match="cannot be activated"):
        ReleaseManager(s, ENV).activate(r1["release_id"])
    s.update_where("cfg_pack_release", {"status": "STAGED", "env_id": "QA"}, {"release_id": r1["release_id"]})
    with pytest.raises(ReleaseError, match="belongs to env"):
        ReleaseManager(s, ENV).activate(r1["release_id"])


def test_expected_seq_conflict_changes_nothing():
    s = migrated()
    ld, rm = PackLoader(s, ENV), ReleaseManager(s, ENV)
    r1, r2 = ld.stage(make_bundle()), ld.stage(make_bundle(priority=7))
    rm.activate(r1["release_id"])
    with pytest.raises(ActivationConflictError):
        rm.activate(r2["release_id"], expected_seq=0)
    assert rm.active_release_id() == r1["release_id"] and rm.pointer()["activation_seq"] == 1
    assert any(e["event_type"] == "ACTIVATE_CONFLICT" for e in rm.events(r2["release_id"]))
    assert rm.activate(r2["release_id"], expected_seq=1)["activation_seq"] == 2


def test_concurrent_activations_exactly_one_wins():
    s = migrated()
    ld = PackLoader(s, ENV)
    base = ld.stage(make_bundle())
    ReleaseManager(s, ENV).activate(base["release_id"])
    cands = [ld.stage(make_bundle(priority=p)) for p in (11, 12, 13, 14)]
    barrier, results = threading.Barrier(len(cands)), []

    def go(rid):
        rm = ReleaseManager(s, ENV, f"t-{rid[:4]}")
        barrier.wait()
        try:
            rm.activate(rid, expected_seq=1)
            results.append(("won", rid))
        except ActivationConflictError:
            results.append(("lost", rid))

    ts = [threading.Thread(target=go, args=(c["release_id"],)) for c in cands]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sorted(r[0] for r in results) == ["lost", "lost", "lost", "won"]
    winner = next(r[1] for r in results if r[0] == "won")
    rm = ReleaseManager(s, ENV)
    assert rm.active_release_id() == winner and rm.pointer()["activation_seq"] == 2
    assert [r["status"] for r in rm.list_releases()].count(ACTIVE) == 1


def test_compatibility_gate():
    s = migrated()
    r1 = PackLoader(s, ENV).stage(make_bundle())
    s.update_where("cfg_pack_release", {"requires_core": ">=9.0"}, {"release_id": r1["release_id"]})
    with pytest.raises(CompatibilityError, match="requires core"):
        ReleaseManager(s, ENV).activate(r1["release_id"])
    s.update_where("cfg_pack_release", {"requires_core": ">=0.1.0.dev0,<1.0"}, {"release_id": r1["release_id"]})
    with pytest.raises(CompatibilityError, match="schema"):
        ReleaseManager(s, ENV, deployed_schema_version=lambda: 0).activate(r1["release_id"])
    assert ReleaseManager(s, ENV).activate(r1["release_id"])["activation_seq"] == 1
    # staging against an un-migrated schema is rejected by validation (MDV-5002)
    with pytest.raises(ValidationFailedError):
        PackLoader(migrated(), ENV, deployed_schema_version=lambda: 0).stage(make_bundle())


def test_config_tables_are_append_only():
    s = migrated()
    PackLoader(s, ENV).stage(make_bundle())
    with pytest.raises(StoreError, match="append-only"):
        s.update_where("cfg_object", {"priority": 1}, {"code": "orders"})
    with pytest.raises(StoreError, match="append-only"):
        s.delete_where("cfg_config_history", {})


# ---- crash injection over every write operation (metadata scope) ------------------------------------
def _fresh_with_active_a():
    s = migrated()
    ld, rm = PackLoader(s, ENV), ReleaseManager(s, ENV)
    a = ld.stage(make_bundle())
    rm.activate(a["release_id"])
    return s, a["release_id"]


def _assert_consistent(s, a_id, b_bundle_rows=None):
    rm = ReleaseManager(s, ENV)
    rm.reconcile()
    ptr = rm.pointer()
    assert ptr is not None and ptr["activation_seq"] in (1, 2, 3)
    statuses = {r["release_id"]: r["status"] for r in rm.list_releases()}
    assert list(statuses.values()).count(ACTIVE) == 1 and statuses[ptr["release_id"]] == ACTIVE  # pointer is truth
    for rid, st in statuses.items():  # nothing half-visible: every STAGED/ACTIVE release has all its rows
        if st in (ACTIVE, STAGED, SUPERSEDED, ROLLED_BACK):
            rel = s.select("cfg_pack_release", {"release_id": rid})[0]
            assert table_rows(s, rid) == sum(json.loads(rel["entity_counts"]).values())
    hist_for_active = s.count("cfg_config_history", {"release_to": ptr["release_id"]})
    assert hist_for_active >= 0
    return ptr, statuses


def _sweep(op, expect_final):
    """Run `op(store_wrapper)` crashing before each write k=1..N; recover; assert invariants; re-run converges."""
    s0, _ = _fresh_with_active_a()
    probe = FaultyStore(s0)
    op(probe)
    n = probe.writes
    assert n > 5
    for k in range(1, n + 1):
        s, a_id = _fresh_with_active_a()
        crashed = False
        try:
            op(FaultyStore(s, crash_at=k))
        except InjectedCrash:
            crashed = True
        assert crashed, f"no crash at write {k}"
        ptr, _ = _assert_consistent(s, a_id)  # recovery state is consistent right after the crash + reconcile
        op(s)  # operator re-runs the same command on the raw store: must converge
        ptr, statuses = _assert_consistent(s, a_id)
        expect_final(s, ptr, statuses, a_id)
        before = (sum(s.count(e.table) for e in ENTITIES), s.count("cfg_config_history"), s.count("cfg_pack_release"))
        op(s)  # and re-running again is a no-op
        after = (sum(s.count(e.table) for e in ENTITIES), s.count("cfg_config_history"), s.count("cfg_pack_release"))
        assert before == after, f"not idempotent after crash at write {k}"
    return n


def test_crash_sweep_stage_and_activate_new_release():
    b = make_bundle(priority=7)
    b_id = release_id_for(ENV, b.bundle_sha256)

    def op(store):
        rel = PackLoader(store, ENV).stage(b)
        ReleaseManager(store, ENV).activate(rel["release_id"])

    def final(s, ptr, statuses, a_id):
        assert ptr["release_id"] == b_id and ptr["activation_seq"] == 2
        assert statuses[a_id] == SUPERSEDED and statuses[b_id] == ACTIVE
        assert s.count("cfg_config_history", {"release_to": b_id, "change_type": "MODIFIED"}) == 1

    n = _sweep(op, final)
    assert n >= 25  # many distinct write points were exercised


def test_crash_sweep_rollback():
    b = make_bundle(priority=7)
    b_id = release_id_for(ENV, b.bundle_sha256)

    def prepare():
        s, a_id = _fresh_with_active_a()
        rel = PackLoader(s, ENV).stage(b)
        ReleaseManager(s, ENV).activate(rel["release_id"])
        return s, a_id

    s0, _ = prepare()
    probe = FaultyStore(s0)
    ReleaseManager(probe, ENV).rollback(reason="test")
    for k in range(1, probe.writes + 1):
        s, a_id = prepare()
        with pytest.raises(InjectedCrash):
            ReleaseManager(FaultyStore(s, crash_at=k), ENV).rollback(reason="test")
        ptr, st = _assert_consistent(s, a_id)
        assert ptr["release_id"] in (a_id, b_id)
        if ptr["release_id"] == b_id:  # crashed before the pointer flip: re-run rollback completes it
            ReleaseManager(s, ENV).rollback(reason="test")
        ptr, st = _assert_consistent(s, a_id)
        assert ptr["release_id"] == a_id and st[b_id] == ROLLED_BACK and st[a_id] == ACTIVE and ptr["activation_seq"] == 3
