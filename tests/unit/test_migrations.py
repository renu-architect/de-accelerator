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
    assert len(stmts) == 53 and not clean.table_exists("cfg_object")


def test_apply_is_idempotent_and_registers_framework(clean):
    m = Migrator(clean, MIGRATIONS, actor="tester")
    assert m.current_version() == 0
    m.apply()
    assert m.current_version() == 1 and clean.table_exists("cfg_object") and clean.table_exists("wm_layer_cursor")
    rows = clean.select("cfg_schema_migration")
    assert len(rows) == 1 and rows[0]["applied_by"] == "tester" and rows[0]["checksum"] == MIGRATIONS[0].checksum()
    assert clean.count("cfg_framework_version") == 1
    assert m.apply() == []  # nothing pending
    assert clean.count("cfg_schema_migration") == 1 and clean.count("cfg_framework_version") == 1


def test_crash_after_up_before_bookkeeping_is_healed(clean):
    MIGRATIONS[0].up(clean)  # simulate crash: tables exist, no bookkeeping row
    m = Migrator(clean, MIGRATIONS)
    assert m.current_version() == 0
    m.apply()
    assert m.current_version() == 1


def test_up_down_up(clean):
    m = Migrator(clean, MIGRATIONS)
    m.apply()
    with pytest.raises(MigrationError):
        m.rollback(0)  # destructive needs explicit consent
    m.rollback(0, allow_destroy=True)
    assert not clean.table_exists("cfg_object") and not clean.table_exists("cfg_schema_migration")
    m.apply()
    assert m.current_version() == 1 and clean.table_exists("cfg_object")


def test_checksum_drift_detected(clean):
    Migrator(clean, MIGRATIONS).apply()
    clean.update_where("cfg_schema_migration", {"checksum": "0" * 64}, {"version": 1})
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
