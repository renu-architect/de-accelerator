import pytest

from de_accelerator.core.errors import MigrationError
from de_accelerator.metadata.migrations import MIGRATIONS, Migration, Migrator
from de_accelerator.metadata.schema.tables import all_tables


@pytest.fixture
def clean(store):
    def wipe():
        for s in reversed(all_tables()):
            store.drop_table(s)

    wipe()
    yield store
    wipe()


def test_migration_numbering_enforced(clean):
    with pytest.raises(MigrationError):
        Migrator(clean, [Migration(2, "x", all_tables)])


def test_dry_run_changes_nothing(clean):
    stmts = Migrator(clean, MIGRATIONS).apply(dry_run=True)
    assert len(stmts) == 53 + 4 + 3 and not clean.table_exists("cfg_object")  # 53 CREATE (0001) + 4 ALTER (0002) + 3 ALTER (0003)


def test_apply_is_idempotent_and_registers_framework(clean):
    m = Migrator(clean, MIGRATIONS, actor="tester")
    assert m.current_version() == 0
    m.apply()
    assert m.current_version() == 3 and clean.table_exists("cfg_object") and clean.table_exists("wm_layer_cursor")
    rows = sorted(clean.select("cfg_schema_migration"), key=lambda r: r["version"])
    assert len(rows) == 3 and rows[0]["applied_by"] == "tester" and [r["checksum"] for r in rows] == [m.checksum() for m in MIGRATIONS]
    assert clean.count("cfg_framework_version") == 1
    assert m.apply() == []  # nothing pending
    assert clean.count("cfg_schema_migration") == 3 and clean.count("cfg_framework_version") == 1


def test_crash_after_up_before_bookkeeping_is_healed(clean):
    MIGRATIONS[0].up(clean)  # simulate crash: tables exist, no bookkeeping row
    m = Migrator(clean, MIGRATIONS)
    assert m.current_version() == 0
    m.apply()
    assert m.current_version() == 3


def test_up_down_up(clean):
    m = Migrator(clean, MIGRATIONS)
    m.apply()
    with pytest.raises(MigrationError):
        m.rollback(0)  # destructive needs explicit consent
    m.rollback(0, allow_destroy=True)
    assert not clean.table_exists("cfg_object") and not clean.table_exists("cfg_schema_migration")
    m.apply()
    assert m.current_version() == 3 and clean.table_exists("cfg_object")


def test_checksum_drift_detected(clean):
    Migrator(clean, MIGRATIONS).apply()
    clean.update_where("cfg_schema_migration", {"checksum": "0" * 64}, {"version": 2})
    with pytest.raises(MigrationError, match="drift"):
        Migrator(clean, MIGRATIONS).apply()


def test_unknown_applied_migration_rejected(clean):
    Migrator(clean, MIGRATIONS).apply()
    clean.insert("cfg_schema_migration", [{"version": 99, "name": "future", "checksum": "x", "applied_ts": None,
                                           "applied_by": "x", "framework_version": "9", "status": "APPLIED"}])
    with pytest.raises(MigrationError, match="unknown"):
        Migrator(clean, MIGRATIONS).plan()


def test_checksum_stable_and_flavor_independent():
    c = MIGRATIONS[0].checksum()
    assert len(c) == 64 and c == MIGRATIONS[0].checksum()


def test_migration_0001_checksum_is_pinned():
    """Schema v1 is frozen once released. If this fails you changed table specs: add migration 0002 instead
    (or, before the first release only, regenerate tests/golden/migration_0001.sha256 with approval)."""
    from pathlib import Path

    pinned = (Path(__file__).resolve().parents[1] / "golden" / "migration_0001.sha256").read_text().strip()
    assert MIGRATIONS[0].checksum() == pinned


def physical_columns(store, table):
    """Actual columns of the table as stored (in-memory spec or real Delta schema)."""
    if hasattr(store, "spark"):
        return set(store.spark.table(store._fq(table)).columns)
    return {c.name for c in store._specs[table].columns}


def test_v2_upgrade_path_preserves_data_and_adds_columns(clean):
    """A database created at schema v1 is upgraded in place (expand-only): old rows survive, new columns are NULL."""
    m = Migrator(clean, MIGRATIONS)
    m.apply(target=1)
    assert m.current_version() == 1
    assert "read_options" not in physical_columns(clean, "cfg_load_strategy")
    clean.insert("wm_watermark", [{"object_code": "o", "stream_key": "live", "committed_value": "x", "committed_seq": 1,
                                   "committed_batch_id": "b", "updated_ts": None}])  # fmt: skip
    m.apply(target=2)
    assert m.current_version() == 2
    assert {"read_options", "cdc_op_map"} <= physical_columns(clean, "cfg_load_strategy")
    assert {"release_id", "plan_json"} <= physical_columns(clean, "run_batch_manifest")
    assert {"txn_app_id", "txn_version", "rows_read"} <= physical_columns(clean, "run_target_receipt")
    row = clean.select("wm_watermark")[0]
    assert row["committed_value"] == "x" and row["epoch"] is None  # old data intact, new column NULL
    clean.update_where("wm_watermark", {"epoch": 3}, {"object_code": "o"})
    assert clean.select("wm_watermark")[0]["epoch"] == 3


def test_rollback_of_additive_migration_keeps_columns(clean):
    m = Migrator(clean, MIGRATIONS)
    m.apply()
    m.rollback(1)  # expand-only: 0002 is marked rolled back, columns stay (older code still works)
    assert m.current_version() == 1
    assert "epoch" in physical_columns(clean, "wm_watermark")
    m.apply()  # re-apply is idempotent
    assert m.current_version() == 3


def test_migration_0002_checksum_is_pinned():
    from pathlib import Path

    pinned = (Path(__file__).resolve().parents[1] / "golden" / "migration_0002.sha256").read_text().strip()
    assert MIGRATIONS[1].checksum() == pinned


def test_migration_0003_checksum_is_pinned():
    from pathlib import Path

    pinned = (Path(__file__).resolve().parents[1] / "golden" / "migration_0003.sha256").read_text().strip()
    assert MIGRATIONS[2].checksum() == pinned


def test_v3_upgrade_path_preserves_data_and_adds_columns(clean):
    """M3: a database at schema v2 is upgraded in place (additive): old rows survive, new columns are NULL."""
    m = Migrator(clean, MIGRATIONS)
    m.apply(target=2)
    assert m.current_version() == 2
    assert "input_versions" not in physical_columns(clean, "wm_layer_cursor")
    clean.insert("wm_layer_cursor", [{"object_code": "o", "layer": "SILVER", "applied_seq": 4, "applied_batch_id": "b", "updated_ts": None}])
    m.apply()
    assert m.current_version() == 3
    assert {"target_name"} <= physical_columns(clean, "cfg_column")
    assert {"effective_column", "on_cast_error", "column_mode"} <= physical_columns(clean, "cfg_layer_mapping")
    row = clean.select("wm_layer_cursor")[0]
    assert row["applied_seq"] == 4 and row["input_versions"] is None
