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
