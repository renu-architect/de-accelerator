"""Migration catalogue. NEVER edit an applied migration: add a new one (checksum guards this)."""

from __future__ import annotations

from de_accelerator.metadata.migrations.migrator import Migration
from de_accelerator.metadata.schema.tables import all_tables

# 0001 = schema version 1. When schema version 2 is introduced, freeze the v1 specs into this module
# (or a snapshot file) before changing tables.py; tests pin the v1 checksum to catch accidental edits.
MIGRATIONS: list[Migration] = [
    Migration(1, "initial_metadata_schema", all_tables),
]
