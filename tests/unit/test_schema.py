import pytest

from de_accelerator.core.naming import SCHEMAS
from de_accelerator.metadata.schema.entities import BY_KIND, ENTITIES
from de_accelerator.metadata.schema.tables import all_tables, table_map
from de_accelerator.metadata.schema.tablespec import Col, TableSpec, render_create, render_pk_constraint


def test_every_entity_has_cfg_table_and_refs_resolve():
    tm = table_map()
    for e in ENTITIES:
        assert e.table in tm
        for f in e.fields:
            if f.ref:
                assert f.ref in BY_KIND, f"{e.kind}.{f.name} -> {f.ref}"
                assert BY_KIND[f.ref].key == ("code",), "ref targets must be keyed by code"
        for k in e.key:
            e.field(k)  # key fields exist


def test_table_set_and_schemas():
    ts = all_tables()
    names = [t.name for t in ts]
    assert len(names) == len(set(names)) == 53
    assert {t.schema for t in ts} <= set(SCHEMAS)
    assert all(t.append_only for t in ts if t.name.startswith("cfg_") and t.name not in
               {"cfg_framework_version", "cfg_schema_migration", "cfg_pack_release", "cfg_active_release"})
    ddl_only = [t.name for t in ts if t.status == "DDL_ONLY"]
    assert "wm_layer_cursor" in ddl_only and "run_batch_manifest" in ddl_only and "gov_suppression_list" in ddl_only
    assert table_map()["cfg_active_release"].append_only is False


def test_render_is_deterministic_and_quoted():
    t = table_map()["cfg_object"]
    a, b = render_create(t, "acme_dev"), render_create(t, "acme_dev")
    assert a == b and "acme_dev.meta.cfg_object" in a and "`object_kind`" in a
    assert "delta.appendOnly" in a
    local = render_create(t, "acme_dev", "LOCAL")
    assert "acme_dev." not in local and "meta.cfg_object" in local
    assert render_pk_constraint(t, "acme_dev").startswith("ALTER TABLE acme_dev.meta.cfg_object")
    assert render_pk_constraint(t, "x", "LOCAL") is None


def test_tablespec_validation():
    with pytest.raises(ValueError):
        TableSpec("nope", "t", (Col("a", "str"),))
    with pytest.raises(ValueError):
        TableSpec("meta", "t", (Col("a", "str"), Col("a", "int")))
    with pytest.raises(ValueError):
        TableSpec("meta", "t", (Col("a", "str"),), pk=("zzz",))


def test_ddl_only_marked_in_comment():
    assert "DDL_ONLY" in render_create(table_map()["wm_layer_cursor"], "c")
