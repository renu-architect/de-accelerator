"""Release lifecycle on a real local Delta store (component tier). One scenario so the migration is paid once."""

import json

import pytest

from de_accelerator.core.errors import ActivationConflictError
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.release import PackLoader, ReleaseManager
from de_accelerator.metadata.schema.entities import ENTITIES
from de_accelerator.metadata.schema.tables import all_tables
from de_accelerator.metadata.store.spark import SparkDeltaTableStore
from tests.unit.test_release import ENV, make_bundle, table_rows, total_rows

pytestmark = pytest.mark.component


@pytest.fixture(scope="module")
def delta_meta(spark):
    s = SparkDeltaTableStore(spark, flavor="LOCAL")
    for t in reversed(all_tables()):
        s.drop_table(t)
    Migrator(s, MIGRATIONS).apply()
    yield s
    for t in reversed(all_tables()):
        s.drop_table(t)


def test_full_release_lifecycle_on_delta(delta_meta):
    s = delta_meta
    ld, rm = PackLoader(s, ENV, "ci"), ReleaseManager(s, ENV, "ci")
    ba, bb = make_bundle(), make_bundle(priority=7)

    a = ld.stage(ba, git_sha="aaa111", ticket="T-1")
    assert a["status"] == "STAGED" and table_rows(s, a["release_id"]) == total_rows(ba)
    assert rm.pointer() is None
    assert ld.stage(ba)["release_id"] == a["release_id"] and s.count("cfg_pack_release") == 1  # idempotent

    assert rm.activate(a["release_id"])["activation_seq"] == 1
    b = ld.stage(bb)
    obj = {r["code"]: r for r in s.select("cfg_object", {"pack_release_id": b["release_id"]})}
    assert obj["orders"]["row_version"] == 2 and obj["customers"]["row_version"] == 1

    with pytest.raises(ActivationConflictError):
        rm.activate(b["release_id"], expected_seq=0)  # stale pointer read
    assert rm.activate(b["release_id"], expected_seq=1)["activation_seq"] == 2
    st = {r["release_id"]: r["status"] for r in rm.list_releases()}
    assert st[a["release_id"]] == "SUPERSEDED" and st[b["release_id"]] == "ACTIVE"

    mod = s.select("cfg_config_history", {"release_to": b["release_id"], "change_type": "MODIFIED"})
    assert len(mod) == 1 and json.loads(mod[0]["after_json"])["priority"] == 7

    assert rm.rollback(reason="test")["release_id"] == a["release_id"]
    st = {r["release_id"]: r["status"] for r in rm.list_releases()}
    assert st[a["release_id"]] == "ACTIVE" and st[b["release_id"]] == "ROLLED_BACK"
    back = s.select("cfg_config_history", {"release_to": a["release_id"], "activation_seq": 3})
    assert len(back) == 1 and json.loads(back[0]["after_json"])["priority"] == 100

    # append-only enforcement by Delta itself (delta.appendOnly)
    with pytest.raises(Exception):
        s.delete_where("cfg_object", {"code": "orders"})
    with pytest.raises(Exception):
        s.update_where("cfg_object", {"priority": 1}, {"code": "orders"})
    assert sum(s.count(e.table, {"pack_release_id": a["release_id"]}) for e in ENTITIES) == total_rows(ba)


def test_snapshot_readers_and_cli_on_delta(delta_meta):
    """Continues the module scenario: release A is active (after the rollback in the previous test)."""
    from de_accelerator.cli.main import run
    from de_accelerator.metadata.reader import DeltaMetadataReader, InMemoryMetadataReader, SnapshotMetadataReader
    from de_accelerator.metadata.reader.source import StoreConfigSource
    from de_accelerator.metadata.schema.entities import ENTITIES
    from de_accelerator.metadata.snapshot import SnapshotBuilder, SnapshotStore

    s = delta_meta
    rm = ReleaseManager(s, ENV)
    rid = rm.active_release_id()
    assert rid is not None

    # snapshot (BINARY gzip column) persists, reloads byte-exact and verifies on Delta
    snap = SnapshotBuilder(StoreConfigSource(s, ENV)).build()
    ss = SnapshotStore(s)
    ref = ss.persist(snap, "ci")
    ss.persist(snap)  # idempotent
    assert s.count("run_config_snapshot") == 1 and ss.load(ref.snapshot_id).content == snap.content

    # readers agree across Delta-backed store, snapshot and in-memory bundle
    d = DeltaMetadataReader(s, ENV)
    sn = SnapshotMetadataReader(ss.load(ref.snapshot_id))
    m = InMemoryMetadataReader.from_bundle(make_bundle(), rid)
    for e in ENTITIES:
        scope = "project_code" if e.project_scoped else "client_code"

        def key(x, f=(scope, *e.key)):
            return tuple(str(getattr(x, k)) for k in f)

        assert sorted(d.all(e.kind), key=key) == sorted(sn.all(e.kind), key=key) == sorted(m.all(e.kind), key=key), e.kind

    # CLI over the Delta store
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as td:
        (Path(td) / "base.yaml").write_text("client_code: acme\nenvironment: DEV\nflavor: LOCAL\nwarehouse_dir: /unused\n")
        lines: list[str] = []
        base = ["--bootstrap", str(Path(td) / "base.yaml")]
        assert run([*base, "status"], lambda cfg: s, out=lines.append) == 0 and f"active_release={rid}" in lines[-1]
        assert run([*base, "snapshot", "--object", "orders"], lambda cfg: s, out=lines.append) == 0
        assert run([*base, "diff", "--to", rid], lambda cfg: s, out=lines.append) == 0 and "ADDED" in lines[-1]
