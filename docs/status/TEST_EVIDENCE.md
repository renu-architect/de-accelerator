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

### M2.1 + M2.2 slice — FULL SUITE (all tiers incl. real PostgreSQL) — PASS — 2026-11-03
- Command: `pytest -q tests --cov=de_accelerator` (also `ruff check src tests` and `mypy --strict`: clean)
- Environment: Python 3.11.18, pyspark 3.5.9, delta-spark 3.2, **PostgreSQL 16 server** (local), PostgreSQL JDBC 42.7.3; local only (no Databricks, no SQL Server, not run on Azure DevOps)
- Result: `335 passed in 554.64s` (0 failed, 0 skipped); line coverage **93%** (3,947 statements, 259 missed)
- Tiers: unit (incl. M1), component on real local Delta (Bronze writer 16, ingestion engine 12, crash-recovery matrix 37), integration on real PostgreSQL (6), SQL-generation tests for SQL Server/Azure SQL/PostgreSQL (29)
- Defects found and fixed by these tests during the increment: lease released with wall-clock instead of engine clock; exhausted transient errors wrongly marked fatal; handled failures kept the lease; migrator could not re-apply a rolled-back migration >= 2; Spark store wrote columns missing from an unmigrated table; `validate-config --env all` ignored the pack's declared environments; **py4j DEBUG logging exposed the JDBC password** (now silenced + Spark option redaction).
- NOT covered by this evidence: real SQL Server/Azure SQL, Databricks (Key Vault scope, Unity Catalog, job run, drivers, managed identity), Azure DevOps pipeline, scale/performance.

### M2 CLOSURE REVIEW — independent re-run + probes — 2026-11-04
- Commit reviewed: `e0e3c27` (branch `m2-ingestion`, clean tree equal to remote).
- Command: `pytest -q tests --cov=de_accelerator`; `ruff check src tests`; `mypy --strict`
- Result: **335 passed, 0 failed, 0 skipped** (532 s); coverage **93 %** (3,947 statements, 259 missed); ruff clean; mypy clean (72 files).
- Coverage facts: `cli/main.py` 78 % (all four new CLI commands uncovered), `core/secrets.py` 50 %, `ingestion/windows.py` 78 %, `connectors/jdbc/connector.py` 87 %.
- Probes (docs/architecture/m2/review_probes/): audit repositories on **real Delta** — manifest, receipt, watermark, run-log PASS; **EventLog.append FAIL (50 junk rows + RuntimeError), LeaseRepo.acquire for another owner FAIL (TypeError)**; runner on Delta audit store: concurrency 1 **FAIL**, concurrency 4 **ConcurrentAppendException (fatal)**; CLI `ingest/batch-status/reprocess/abandon-batch` happy paths work (in-memory audit) with run-id/audit defects.
- Interpretation: the passing suite does **not** demonstrate that the engine works on a Delta-backed audit store. Real SQL Server, Databricks, Azure DevOps and scale remain unverified.
- Conclusion: M2 NOT READY to close (see closure review §1, §7).

### M2 CLOSURE — FINAL FULL SUITE after fixes A1–A5, B1–B5 — PASS — 2026-11-04
- Command: `pytest -q tests --cov=de_accelerator` (also `ruff check src tests` and `mypy --strict`: clean, 72 source files)
- Environment: Python 3.11.18, pyspark 3.5.9, delta-spark 3.2, local PostgreSQL 16 server, PostgreSQL JDBC 42.7.3; **local only** (no Databricks, no SQL Server, not run on Azure DevOps)
- Result: **398 passed, 0 failed, 0 skipped** in 1233.75 s; line coverage **95 %** (4,022 statements, 208 missed)
- New since the closure review (+63 tests): Delta-audit module 10 (engine, lease, timestamps under 3 time zones, crash recovery at 3 points, 4-way parallel x2 with 38 absorbed Delta commit conflicts, CLI); resilience/integrity 6; CLI ingestion 10; timestamp/lease/conflict-retry unit tests 18; validator and dialect rejection tests 12+; secrets 14.
- Coverage of closure-relevant files: `core/secrets.py` 50→96 %, `cli/main.py` 78→88 %, `ingestion/engine.py` 97 %, `audit/repos.py` 97 %, `metadata/store/spark.py` 93 %, `processing/bronze/writer.py` 96 %; weakest: `ingestion/windows.py` 78 %, `connectors/jdbc/connector.py` 87 %.
- Still NOT covered by this evidence: real SQL Server/Azure SQL, Databricks (Key Vault scope, Unity Catalog, drivers, job), Azure DevOps pipeline, scale/performance.
