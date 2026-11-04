import json

import pytest

from de_accelerator.cli.main import run
from de_accelerator.metadata.store.memory import InMemoryTableStore
from tests.packutil import EXAMPLE, copy_pack, edit_items


@pytest.fixture
def cli(tmp_path):
    (tmp_path / "base.yaml").write_text("client_code: acme\nenvironment: DEV\nflavor: LOCAL\nwarehouse_dir: /unused\n")
    store = InMemoryTableStore()
    lines: list[str] = []

    def call(*argv, expect=0, factory=True):
        lines.clear()
        rc = run(["--bootstrap", str(tmp_path / "base.yaml"), "--actor", "tester", *argv], (lambda cfg: store) if factory else None,
                 out=lines.append)  # fmt: skip
        assert rc == expect, "\n".join(lines)
        return "\n".join(lines)

    call.store, call.tmp = store, tmp_path
    return call


def test_validate_config_all_envs_ok_and_json():
    lines = []
    assert run(["validate-config", str(EXAMPLE)], out=lines.append) == 0
    assert sum("OK" in ln for ln in lines) == 3
    lines.clear()
    assert run(["--json", "validate-config", str(EXAMPLE), "--env", "QA"], out=lines.append) == 0
    data = json.loads(lines[0])
    assert data[0]["ok"] is True and data[0]["env"] == "QA"


def test_validate_config_reports_failure_exit_code(tmp_path):
    root = copy_pack(tmp_path)
    edit_items(root, "projects/sales/load/load_strategies.yaml", lambda it: it[1].pop("watermark_column"))
    lines = []
    assert run(["validate-config", str(root), "--env", "DEV"], out=lines.append) == 1
    assert any("MDV-3001" in ln for ln in lines)
    assert run(["validate-config", str(root), "--env", "DEV", "--core-version", "9.9.9"], out=lines.append) == 1


def test_build_bundle(tmp_path):
    out = tmp_path / "bundles"
    lines = []
    assert run(["build-bundle", str(EXAMPLE), "--out", str(out)], out=lines.append) == 0
    assert {p.name for p in out.glob("*.json")} == {"bundle_DEV.json", "bundle_QA.json", "bundle_PROD.json"}
    assert len(list(out.glob("*.sha256"))) == 3
    root = copy_pack(tmp_path / "bad")
    edit_items(root, "projects/sales/objects/objects.yaml", lambda it: it[0].update(source="ghost"))
    assert run(["build-bundle", str(root), "--env", "DEV", "--out", str(tmp_path / "o2")], out=lines.append) == 1
    assert not list((tmp_path / "o2").glob("*.json"))


def test_store_commands_need_bootstrap():
    lines = []
    assert run(["status"], out=lines.append) == 1 and "--bootstrap" in lines[0]


def test_full_operator_flow(cli, tmp_path):
    assert "DRY RUN" in cli("migrate", "--dry-run") and not cli.store.table_exists("cfg_object")
    assert "schema version = 3" in cli("migrate")
    assert "schema=3" in cli("status")

    out = cli("build-bundle", str(EXAMPLE), "--env", "DEV", "--out", str(tmp_path / "b"), factory=False)
    assert "built" in out
    staged = cli("load-pack", str(tmp_path / "b" / "bundle_DEV.json"), "--git-sha", "abc", "--approved-by", "bob", "--ticket", "T-1")
    rid = staged.split()[2]
    assert "status=STAGED" in staged and "active_release=None" in cli("status")

    assert "seq=1" in cli("activate", rid, "--expected-seq", "0")
    assert f"active_release={rid}" in cli("status")
    assert "STAGED" not in cli("releases") and "ACTIVE" in cli("releases")

    snap = cli("snapshot", "--object", "orders")
    assert snap.startswith("snapshot ") and cli.store.count("run_config_snapshot") == 1
    assert json.loads(cli("--json", "snapshot", "--object", "orders"))["release_id"] == rid
    assert cli.store.count("run_config_snapshot") == 1  # idempotent

    diff = cli("diff", "--to", rid)
    assert "ADDED" in diff and "cfg_object" in diff

    # modified pack -> second release -> activate, stale seq => conflict (exit 2), rollback
    root = copy_pack(tmp_path / "p2")
    edit_items(root, "projects/sales/objects/objects.yaml", lambda it: it[1].update(priority=7))
    out2 = cli("load-pack", str(root), "--activate")
    rid2 = out2.split()[2]
    assert "activated" in out2 and rid2 != rid
    cli("activate", rid, "--expected-seq", "0", expect=2)  # stale pointer => CONFLICT
    assert "seq=3" in cli("rollback", "--reason", "test", "--ticket", "INC-1")
    assert f"active_release={rid}" in cli("status")
    d = cli("diff", "--from", rid, "--to", rid2)
    assert "MODIFIED" in d and "sales/orders" in d


def test_load_pack_rejects_invalid_directory(cli, tmp_path):
    cli("migrate")
    root = copy_pack(tmp_path / "bad")
    edit_items(root, "projects/sales/objects/objects.yaml", lambda it: it[0].update(source="ghost"))
    out = cli("load-pack", str(root), expect=1)
    assert "MDV-2001" in out and cli.store.count("cfg_pack_release") == 0


def test_error_mapping_unexpected_and_rollback_guard(cli):
    cli("migrate")
    assert "no active" in cli("rollback", expect=1)
    assert "destroys" in cli("migrate", "--rollback-to", "0", expect=1)
    assert "rolled back" in cli("migrate", "--rollback-to", "0", "--allow-destroy")
    lines = []
    assert run(["--bootstrap", "/does/not/exist.yaml", "status"], lambda cfg: None, out=lines.append) == 1  # type: ignore[arg-type,return-value]
