# Implementation Status (source of truth)

**Verification statuses (approved vocabulary):**
- `LOCAL_VERIFIED` — code reviewed; unit + component tests green locally (Python 3.11, Spark 3.5, local Delta); evidence in `TEST_EVIDENCE.md`.
- `DATABRICKS_INTEGRATION_PENDING` — no Databricks workspace/SP available; integration tests (UC, secrets, grants, Workflows) not yet run.
- `PRODUCTION_READY` — LOCAL_VERIFIED + Databricks integration passed + security review + user approval. **Nothing is production-ready.**
Other states: `DESIGNED` (docs only) · `DDL_ONLY` (schema defined, no logic) · `IN_PROGRESS`.

Baseline: Rev 1 approved (C1, C3 and decisions in docs 07/13). Current milestone: **M3 (Bronze->Silver->Gold transformation) — DESIGN PROPOSED, no code.** M1 and **M2 are CLOSED as LOCAL_VERIFIED** (M2: 398 tests, 95% coverage, ruff and strict mypy clean, commit 51270e6). Pending external validation: real SQL Server / Azure SQL, Databricks / Unity Catalog, Azure DevOps, scale. Production readiness is NOT established for any module.

| Module / work package | Implementation | Verification | Databricks | Notes |
|---|---|---|---|---|
| **M1** metadata engine (core, schema v1, packs, validator, releases, snapshots, readers, CLI) | IMPLEMENTED | LOCAL_VERIFIED (closed) | DATABRICKS_INTEGRATION_PENDING | 8 schemas / 53 tables / 22 schema-only; `docs` schema = planned future capability |
| M2 schema v2 (migration 0002), loader schema check, validator hook + rules 6001/6002/6003/6004/6010 (65 rules), `--env all`, migrator re-apply | IMPLEMENTED | LOCAL_VERIFIED (incl. in-place v1->v2 on real Delta) | DATABRICKS_INTEGRATION_PENDING | migration 0001 checksum unchanged |
| M2 Delta store fixes: UTC-aware timestamps (epoch micros), bounded retry of Delta commit conflicts (`StoreConflictError`) | IMPLEMENTED | LOCAL_VERIFIED (4-way parallel run absorbed 38 conflicts) | DATABRICKS_INTEGRATION_PENDING | M1 store touched (additive); timestamps from Delta are now tz-aware |
| M2 (CLOSED, LOCAL_VERIFIED) connector interface, registry, mock connector, secrets/clock/faults | IMPLEMENTED | LOCAL_VERIFIED (secrets 96% covered incl. PROD guard; no shared connector contract suite) | DATABRICKS_INTEGRATION_PENDING | Databricks secret provider tested with a fake dbutils only |
| M2 Bronze writer (audit columns, idempotent append, commit verification, observed-vs-commit integrity check, schema evolution) | IMPLEMENTED | LOCAL_VERIFIED (real local Delta) | DATABRICKS_INTEGRATION_PENDING | type widening unavailable locally |
| M2 batch protocol (manifest, events, receipts, watermark, lease, run log) + ingestion engine + runner (retry, recovery, duplicate prevention, parallelism) | IMPLEMENTED | LOCAL_VERIFIED on in-memory AND real local Delta audit stores (crash matrix 37; Delta: recovery at 3 points, 4-way parallel x2) | DATABRICKS_INTEGRATION_PENDING | closure fixes A1-A5 done |
| M2 audit-outage resilience (failure path never aborts a run) + runner safety net | IMPLEMENTED | LOCAL_VERIFIED | — | A4 |
| M2 JDBC connector: PostgreSQL (SECRET auth; FULL/SNAPSHOT/INCREMENTAL) | IMPLEMENTED | INTEGRATION TESTED on a real PostgreSQL 16 server (audit store in-memory) | DATABRICKS_INTEGRATION_PENDING |  |
| M2 JDBC connector: SQL Server, Azure SQL (SECRET auth; FULL/SNAPSHOT/INCREMENTAL) | IMPLEMENTED | UNIT-TESTED ONLY (exact SQL strings, validation). NOT executed on a real SQL Server | PENDING EXTERNAL VALIDATION |  |
| M2 SERVICE_PRINCIPAL / MANAGED_IDENTITY auth for JDBC | NOT IMPLEMENTED | rejected by the validator (code path unverified) | PENDING EXTERNAL VALIDATION | needs a real Azure/Databricks environment |
| M2 CLI ingest / batch-status / abandon-batch | IMPLEMENTED | LOCAL_VERIFIED (in-memory and Delta audit stores) | DATABRICKS_INTEGRATION_PENDING |  |
| M2 CLI reprocess | IMPLEMENTED -- EXPERIMENTAL | UNTESTED in the suite (happy path confirmed by a probe); constant run id, no run_pipeline row, reason not audited | — | deferred (B7) |
| M2 demo pack, quickstart, Databricks job template, wheel | IMPLEMENTED (files) | wheel builds; pack validates; template NOT validated and incomplete; demo never run on SQL Server/Databricks | DATABRICKS_INTEGRATION_PENDING | docs/guides/QUICKSTART_SQLSERVER_TO_BRONZE.md |
| M2 schema discovery + metadata auto-generation | NOT IMPLEMENTED | — | — | deferred |
| M2 CSV / JSON / Parquet, REST, CDC + deletes, late-arrival beyond lookback, source-count reconciliation, backfill chunking | NOT IMPLEMENTED | — | — | deferred; rejected/absent in the validator |
| `docs` schema + `docs.*` tables (ADR-0018 #6) | PLANNED (future; not implemented by decision) | — | — | Code stays at 8 schemas |
| **M3** transformation engine: Bronze->Silver (cleansing, dedupe, keys, SCD1/SCD2, independent layer cursors) and Silver->Gold | DESIGN PROPOSED (docs/architecture/m3, ADR-0020) — awaiting approval | — | — | no M3 code exists |
| remaining: quality, reconciliation, orchestration planner, semantic, documents, monitoring, security, IaC | DESIGNED | — | — | not started |
