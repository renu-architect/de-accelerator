# Known Limitations (design stage)
- Nothing implemented or tested; all capabilities are proposals.
- Streaming not covered (out of scope). Env overrides, DR/backup, GDPR erasure and UI-readiness are now designed in Rev 1 (docs 09–11) but not implemented.
- Performance targets unvalidated; sizing assumptions (500–5,000 objects, ≤10 TB) unconfirmed.
- Delta lacks multi-table transactions; mitigated by ADR-0009 (supersedes ADR-0004 details) (to be proven by crash-injection tests).
- "Minimal customization across clients" is an unproven goal until pilot validation.

## Added in Rev 1
- Sizing (500–5,000 objects, ≤10 TB/client) are design targets only; unmeasured.
- Exactly-once is *effectively-once*; relies on Spike S1 (MERGE commit tagging) – unverified.
- RPO/RTO values are proposals until DR drills measure them.
- Power BI deployment/DAX tests (T3–T5) need Fabric/Power BI capacity and tenant enablement; not available in design sandbox.
- Databricks-side integration tests need a DEV workspace; sandbox has no Databricks and no PySpark/Delta installed (Python 3.13, Java present).
- Erasure scope covers lineage-known datasets; external copies (e.g. Power BI imports, exports) need the consumer registry + manual/notified action; legal interpretation is the client's.
- Metadata UI not in MVP (Git workflow only).

## M1 limitations (metadata engine)
- No Databricks/Unity Catalog verification: UC grants, PK constraints, secret scopes, Workflows untested (`DATABRICKS_INTEGRATION_PENDING`).
- Azure DevOps pipeline file written but never executed. Runtime matrix profile (Python 3.11 / Spark 3.5) not yet verified against a specific Databricks LTS.
- Component (Delta) tests are slow (~5 min total); concurrency is tested with in-memory atomics, not concurrent Delta writers.
- Scale benchmark (500/2,500/5,000 objects) not run; sizing remains an unmeasured design target.
- Snapshot ids include the release id (no cross-release dedupe); per-layer cursors, CDC retention behaviour and crash-injection of ingestion are M2 scope.
- The `docs` schema (document runtime tables) is a planned future capability, not implemented: code stays at 8 schemas / 53 tables / 22 schema-only.

## M2 limitations (source-to-Bronze, current state)
- **No real SQL Server / Azure SQL execution yet.** The T-SQL generation, TLS URLs, rowversion safe bound and validation are unit-tested; execution is verified only against a real **PostgreSQL** server through the same JDBC connector. Docker is unavailable in the sandbox, so a SQL Server container test must run on a dev machine/CI agent.
- **No Databricks execution.** Key Vault secret scope, Unity Catalog, JDBC driver availability on the runtime, the Azure SQL managed-identity token path (UC service credential), Delta type widening and the job template (`deployment/databricks/databricks.yml`) are untested.
- Not implemented yet: CDC and deletes, file connectors (CSV/JSON/Parquet) and file tracking, REST, schema discovery / metadata auto-generation, SQL Server native CDC, backfill chunking, per-source circuit breaker, scale/performance measurement.
- INCREMENTAL loads cannot detect source deletes (documented; remedy = periodic full/snapshot or CDC once implemented).
- A crashed (not failed) runner leaves its lease until the TTL (30 min) expires; recovery by a new run waits for that. A handled failure releases the lease immediately.
- Timestamp watermarks are carried at microsecond precision; `datetime2(7)` sources lose sub-microsecond precision (mitigated by lookback).
- Delta idempotent writes (`txnAppId/txnVersion`) were verified on local Delta 3.2 only; the Databricks runtime must be re-verified.
- `_DDL_ONLY` comments on activated audit tables are not updated in the physical table comments (status is tracked in `IMPLEMENTATION_STATUS.md`).
- py4j DEBUG logging can expose JDBC credentials; the engine silences py4j wire logging and sets Spark option redaction, but a client enabling DEBUG on other libraries must keep that in mind.

## M2 closure review (2026-11-04) — findings AS FOUND (historical; resolution in the next section)
Full detail, evidence and fix plan: `docs/architecture/m2/07_M2_closure_review.md`.
- **Critical (block M2 closure):** C1 event log fails on Delta and writes 50 junk rows; C2 lease repository fails for another owner on Delta; C3 parallel audit writes hit `ConcurrentAppendException` classified fatal; C4 failure handling can abort the whole run; C5 no test exercised the engine with Delta-backed audit tables, so "LOCAL_VERIFIED" for the engine/runner/crash-matrix holds **only with the in-memory audit store**.
- **High:** H1 `late_arrival_window` uses the framework clock (possible data loss if the source clock is behind UTC); H2 validator accepts options/load types not implemented (`source_timezone`, `max_window`, `max_batches_per_run`, `change_table`, `capture_instance`, `isolation_level`, `CDC`); H3 SERVICE_PRINCIPAL not functional, MANAGED_IDENTITY unverified; H4 new CLI commands untested (reprocess: constant run id, no `run_pipeline` row, `--reason` not audited); H5 Databricks job template incomplete/unvalidated; H6 observed-vs-commit and source-count reconciliation not implemented; H7 integer-only partition columns, `max_connections` does not bound real DB connections.
- The end-to-end SQL Server → Bronze demo is **not executable today** (blocked by C1–C3 even before SQL Server/Databricks availability).
- Earlier statements in this file and in the quickstart that the engine is "verified" refer to local tests with the in-memory audit store.

## M2 closure — resolution and what remains (2026-11-04)
**Fixed and regression-tested:** C1 event log on Delta · C2 lease on Delta · C3 Delta commit conflicts (bounded retry, 38 absorbed in the 4-way parallel test) · C4 failure path can no longer abort a run · C5 Delta-audit test module (10 tests) · B1 unsupported settings rejected (CDC, service-principal/managed-identity auth, all JDBC `read_options`, `isolation_level`, `cdc_op_map`, `partition_cols`, non-APPEND `write_mode`, `backfill_from/to`; rule MDV-6004) · B2 `late_arrival_window` rejected for JDBC · B3 observed-vs-committed row-count integrity check · B4 CLI `ingest`/`batch-status`/`abandon-batch` tested · B5 secret-provider PROD guard tested. Result: 398 tests passing, 95 % coverage.

**Still pending external validation (not testable here):** real SQL Server / Azure SQL execution; every Databricks behaviour (Key Vault scope, Unity Catalog, JDBC drivers on the runtime, Delta behaviour on the runtime, job template, wheel install, service credential / managed identity); Azure DevOps pipeline; scale and performance; production-sized Delta conflict behaviour (≈25 small audit commits per batch; compaction is a later concern).

**Deferred by decision (tracked, not blocking M3):**
- B6 source-clock based late-arrival handling and timestamp-zone modelling — until then timestamp watermarks are treated as UTC labels and `late_arrival_window` is rejected.
- B7 `reprocess`: constant run id, no `run_pipeline` row, `--reason` not audited, no tests (labelled experimental).
- B8 service-principal and managed-identity authentication.
- B9 Databricks job template completion (Unity Catalog access mode, `migrate`/`load-pack` tasks, bootstrap/pack deployment).
- B10 non-integer partition columns give an unclassified error; `max_connections` caps objects, not real DB connections.
- B11 integer/rowversion watermark paths are only partly tested (`ingestion/windows.py` 78 %).
- B12 lease renewal result ignored / no heartbeat (30-min TTL; correctness protected by idempotent writes), connector contract-test suite, stale `DDL_ONLY` table comments, history-scan cost in `verify()`, connectors never closed, 10 of 15 designed crash points, case-sensitive `cast_to_table`.
- No local-CLI way to add JDBC driver jars (`flavor: LOCAL` starts a plain Spark session).
- Not implemented (planned M2.2/M2.3/M2.4 increments): discovery and metadata auto-generation, CSV/JSON/Parquet, REST, CDC and deletes, backfill chunking, source-count reconciliation.
