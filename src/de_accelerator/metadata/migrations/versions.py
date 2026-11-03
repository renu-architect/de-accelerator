"""Migration catalogue. NEVER edit an applied migration: add a new one (checksums + a pinned golden test guard this)."""

from __future__ import annotations

from de_accelerator.metadata.migrations.migrator import Migration
from de_accelerator.metadata.schema.tables import tables_v1, v2_added_columns

MIGRATIONS: list[Migration] = [
    # 0001 is FROZEN: rendered from tables_v1() (latest specs minus the v2 columns); checksum pinned in tests/golden.
    Migration(1, "initial_metadata_schema", tables_v1),
    # 0002 (CR-M2-01, additive): load-strategy read_options/cdc_op_map + audit columns for manifest/receipt/watermark.
    Migration(2, "m2_ingestion_columns", add_columns=v2_added_columns),
]
