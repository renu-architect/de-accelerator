#!/usr/bin/env python3
"""Regenerate docs/status/IMPLEMENTATION_STATUS.md from the table below (single place to edit statuses)."""
HEADER = """# Implementation Status (source of truth)

**Verification statuses (approved vocabulary):**
- `LOCAL_VERIFIED` — code reviewed; unit + component tests green locally (Python 3.11, Spark 3.5, local Delta); evidence in `TEST_EVIDENCE.md`.
- `DATABRICKS_INTEGRATION_PENDING` — no Databricks workspace/SP available; integration tests (UC, secrets, grants, Workflows) not yet run.
- `PRODUCTION_READY` — LOCAL_VERIFIED + Databricks integration passed + security review + user approval. **Nothing is production-ready.**
Other states: `DESIGNED` (docs only) · `DDL_ONLY` (schema defined, no logic) · `IN_PROGRESS`.

Baseline: Rev 1 approved (C1, C3 and decisions in docs 07/13). Current milestone: **M2 (source-to-Bronze ingestion) IN PROGRESS** — M2.1 core and the M2.2 JDBC slice are implemented and LOCAL_VERIFIED (see rows); M1 is closed. Databricks integration, real SQL Server execution, Azure DevOps pipeline execution and scale benchmark are PENDING.

| Module / work package | Implementation | Verification | Databricks | Notes |
|---|---|---|---|---|
"""
ROWS = [
    ("**M1** metadata engine (core, schema v1, packs, validator, releases, snapshots, readers, CLI)", "IMPLEMENTED", "LOCAL_VERIFIED (closed; 182 tests at close)", "DATABRICKS_INTEGRATION_PENDING", "8 schemas / 53 tables / 22 schema-only; `docs` schema = planned future capability"),
    ("M2 CR-M2-01: schema v2 (migration 0002), loader schema check, connector validation hook + rules MDV-6001/6002/6003/6010, `--env all` fix, Spark store tolerates unmigrated nullable columns, migrator re-apply fix", "IMPLEMENTED", "LOCAL_VERIFIED", "DATABRICKS_INTEGRATION_PENDING", "M1 code touched additively; migration 0001 checksum unchanged (pinned test)"),
    ("M2.1 core: connector interface + registry, secrets/clock/fault injection, audit repositories (manifest, events, receipts, watermark, lease, run log)", "IMPLEMENTED", "LOCAL_VERIFIED", "DATABRICKS_INTEGRATION_PENDING", ""),
    ("M2.1 Bronze writer (audit columns, schema evolution, idempotent transactional append, commit verification)", "IMPLEMENTED", "LOCAL_VERIFIED (real local Delta)", "DATABRICKS_INTEGRATION_PENDING", "Delta type widening unavailable locally: falls back to a clear error (pending Databricks)"),
    ("M2.1 ingestion engine + runner + retry/recovery; mock connector; CLI ingest / batch-status / abandon-batch / reprocess", "IMPLEMENTED", "LOCAL_VERIFIED (crash matrix: 10 points x FULL/INCREMENTAL/SNAPSHOT + double fault + races)", "DATABRICKS_INTEGRATION_PENDING", "Empty plans create no batch (refinement of doc 04)"),
    ("M2.2 JDBC connector: PostgreSQL", "IMPLEMENTED", "LOCAL_VERIFIED (**real PostgreSQL 16 server**: full, incremental + updates, partitioned reads, crash recovery, auth failure, connection refused, secret canary)", "DATABRICKS_INTEGRATION_PENDING", ""),
    ("M2.2 JDBC connector: SQL Server, Azure SQL", "IMPLEMENTED", "UNIT-TESTED ONLY (exact T-SQL generation, TLS URLs, safe bounds, validation). **Not executed against a real SQL Server** (none available)", "DATABRICKS_INTEGRATION_PENDING", "Azure SQL managed-identity token path is Databricks-only and unverified"),
    ("M2.2 demo: SQL Server demo pack, quickstart, Databricks job template, wheel build", "IMPLEMENTED (files)", "wheel builds; pack validates; job template NOT executed", "DATABRICKS_INTEGRATION_PENDING", "deployment/databricks/databricks.yml, docs/guides/QUICKSTART_SQLSERVER_TO_BRONZE.md"),
    ("M2.2 source schema discovery + metadata auto-generation (`de-accel discover`)", "NOT_STARTED", "—", "—", "planned next"),
    ("M2.3 connectors: CSV, JSON, Parquet (file incremental + tracking), REST (pagination, landing)", "NOT_STARTED", "—", "—", "designed (docs/architecture/m2)"),
    ("M2.4 CDC + deletes + retention-gap policies, late-arriving handling beyond lookback, scale/perf tests", "NOT_STARTED", "—", "—", "designed"),
    ("`docs` schema + `docs.*` tables (ADR-0018 #6)", "PLANNED (future; not implemented by decision)", "—", "—", "Code stays at 8 schemas"),
    ("remaining: processing (silver/gold) / quality / reconciliation / orchestration planner / semantic / documents / monitoring / security / IaC", "DESIGNED", "—", "—", "not started"),
]
if __name__ == "__main__":
    body = "".join("| " + " | ".join(r) + " |\n" for r in ROWS)
    open("docs/status/IMPLEMENTATION_STATUS.md", "w").write(HEADER + body)
