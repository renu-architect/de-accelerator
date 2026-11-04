"""Backward compatibility of schema v3: old packs on a new core, new packs on an old deployment, frozen migrations."""

import pytest

from de_accelerator.core.errors import ValidationFailedError
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.packs.bundle import build_bundle
from de_accelerator.metadata.release import PackLoader, ReleaseManager
from de_accelerator.metadata.store.memory import InMemoryTableStore
from tests.packutil import EXAMPLE

M3_PACK = EXAMPLE.parents[2] / "docs" / "architecture" / "m3" / "examples" / "transform_example_pack"
SQLSERVER_PACK = EXAMPLE.parent / "sqlserver_demo_pack"
ENV = "DEV"


def migrated(upto: int | None = None):
    s = InMemoryTableStore()
    Migrator(s, MIGRATIONS).apply(target=upto)
    return s


@pytest.mark.parametrize("pack", [EXAMPLE, SQLSERVER_PACK], ids=["acme_pack_v1", "sqlserver_demo_pack"])
def test_older_packs_load_and_activate_on_a_v3_core(pack):
    s = migrated()
    b, findings = build_bundle(pack, ENV)
    assert b is not None and not [f for f in findings if f.severity == "ERROR"]
    rel = PackLoader(s, ENV).stage(b, approved_by="t")
    ReleaseManager(s, ENV).activate(rel["release_id"])
    assert ReleaseManager(s, ENV).pointer()["release_id"] == rel["release_id"]  # type: ignore[index]


def test_v3_pack_loads_and_activates_on_a_v3_deployment():
    s = migrated()
    b, _ = build_bundle(M3_PACK, ENV)
    assert b is not None and b.manifest["metadata_schema_version"] == 3
    rel = PackLoader(s, ENV).stage(b, approved_by="t")
    ReleaseManager(s, ENV).activate(rel["release_id"])
    cols = s.select("cfg_column", {})
    assert any(c["target_name"] == "customer_id" for c in cols)  # the v3 field is persisted
    lm = [r for r in s.select("cfg_layer_mapping") if r["layer"] == "SILVER"]
    assert {r["on_cast_error"] for r in lm} == {"REJECT"} and {r["effective_column"] for r in lm} == {"ModifiedDate"}


def test_v3_pack_is_refused_by_a_v2_deployment():
    s = migrated(upto=2)
    b, _ = build_bundle(M3_PACK, ENV)
    with pytest.raises(ValidationFailedError) as e:
        PackLoader(s, ENV).stage(b)
    assert "MDV-5002" in e.value.context["codes"]  # pack needs schema 3; deployed is 2 (run migrate first)


def test_frozen_migrations_are_unchanged():
    from pathlib import Path

    g = Path(__file__).resolve().parents[1] / "golden"
    assert [m.checksum() for m in MIGRATIONS[:2]] == [(g / "migration_0001.sha256").read_text().strip(),
                                                      (g / "migration_0002.sha256").read_text().strip()]  # fmt: skip


def test_old_pack_rows_have_null_v3_fields_and_typed_readers_still_work():
    from de_accelerator.metadata.reader import DeltaMetadataReader

    s = migrated()
    b, _ = build_bundle(EXAMPLE, ENV)
    rel = PackLoader(s, ENV).stage(b, approved_by="t")
    ReleaseManager(s, ENV).activate(rel["release_id"])
    rows = s.select("cfg_layer_mapping")
    assert rows and all(
        r.get("on_cast_error") is None and r.get("column_mode") is None for r in rows
    )  # engines apply the defaults
    r = DeltaMetadataReader(s, ENV)
    m = r.layer_mappings("sales", "orders")
    assert m and all(x.on_cast_error is None and x.effective_column is None for x in m)
    assert all(c.target_name is None for c in r.columns("sales", "orders"))
