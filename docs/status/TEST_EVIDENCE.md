# Test Evidence
Entries are appended by `scripts/record_evidence.py` (command, environment, result). Evidence is **local only** unless stated;
Databricks integration is `DATABRICKS_INTEGRATION_PENDING` for every module (no workspace available).

### WP0+WP1 core — PASS — 2026-11-03
- Command: `pytest -q tests/unit/test_core.py`
- Environment: Python 3.11.18, pyspark 3.5.9, delta-spark (local); local only (no Databricks)
- Result: `10 passed in 0.17s`

### WP2 DDL specs + TableStore + migrations (in-memory + local Delta) — PASS — 2026-11-03
- Command: `pytest -q tests/unit/test_schema.py tests/unit/test_store_contract.py tests/unit/test_migrations.py`
- Environment: Python 3.11.18, pyspark 3.5.9, delta-spark (local); local only (no Databricks)
- Result: `30 passed in 101.38s (0:01:41)`

### WP3 YAML pack model (schemas, parser, ids, overrides, bundle) — PASS — 2026-11-03
- Command: `pytest -q tests/unit/test_packs.py`
- Environment: Python 3.11.18, pyspark 3.5.9, delta-spark (local); local only (no Databricks)
- Result: `20 passed in 0.86s`

### WP4 validator (60 coded rules, each with a negative test) — PASS — 2026-11-03
- Command: `pytest -q tests/unit/test_validator.py`
- Environment: Python 3.11.18, pyspark 3.5.9, delta-spark (local); local only (no Databricks)
- Result: `69 passed in 2.33s`

### WP5 loader / release manager / history (in-memory; incl. crash sweeps over every write op, concurrency, rollback, compat gate) — PASS — 2026-11-03
- Command: `pytest -q tests/unit/test_release.py`
- Environment: Python 3.11.18, pyspark 3.5.9, delta-spark (local); local only (no Databricks)
- Result: `14 passed in 3.00s`

### WP6 immutable snapshots (in-memory) — PASS — 2026-11-03
- Command: `pytest -q tests/unit/test_snapshot.py`
- Environment: Python 3.11.18, pyspark 3.5.9, delta-spark (local); local only (no Databricks)
- Result: `10 passed in 0.72s`

### WP5 release lifecycle on local Delta (component) — PASS — 2026-11-03
- Command: `pytest -q tests/component/test_release_delta.py`
- Environment: Python 3.11.18, pyspark 3.5.9, delta-spark 3.2 (local); local only (no Databricks)
- Result: `1 passed in 196.57s` (stage/idempotent restage/activate/stale-seq conflict/row_version/history/rollback/appendOnly enforced by Delta)

### M1 FULL SUITE (unit + local-Delta component, with coverage) — PASS — 2026-11-03
- Command: `pytest -q tests --cov=de_accelerator` (also `ruff check` and `mypy --strict`: clean)
- Environment: Python 3.11.18, pyspark 3.5.9, delta-spark 3.2, local only (no Databricks, not run on Azure DevOps)
- Result: `182 passed in 308.56s`; line coverage **95%** (2407 statements, 109 missed)
- Includes: reader contract on 3 readers (19), CLI (7), migration checksum pin, snapshot/readers/CLI on real local Delta.
- Not covered by this evidence: Databricks/Unity Catalog behaviour, Azure DevOps pipeline execution, scale benchmark (optional, not run).
