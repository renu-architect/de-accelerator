# Implementation Status (source of truth)

**Verification statuses (approved vocabulary):**
- `LOCAL_VERIFIED` — code reviewed; unit + component tests green locally (Python 3.11, Spark 3.5, local Delta); evidence in `TEST_EVIDENCE.md`.
- `DATABRICKS_INTEGRATION_PENDING` — no Databricks workspace/SP available; integration tests (UC, secrets, grants, Workflows) not yet run.
- `PRODUCTION_READY` — LOCAL_VERIFIED + Databricks integration passed + security review + user approval. **Nothing is production-ready.**
Other states: `DESIGNED` (docs only) · `DDL_ONLY` (schema defined, no logic) · `IN_PROGRESS`.

Baseline: Rev 1 approved (C1, C3 and decisions in docs 07/13). Current milestone: **M1 — metadata engine: implemented and LOCAL_VERIFIED (182 tests, 95% coverage), awaiting your acceptance review.** Ingestion work is blocked until M1 is accepted.

| Module / work package | Implementation | Verification | Databricks | Notes |
|---|---|---|---|---|
| WP0 scaffold, tooling, CI (Azure DevOps YAML), runtime matrix | IMPLEMENTED | LOCAL_VERIFIED (lint/type/tests run) | DATABRICKS_INTEGRATION_PENDING | Pipeline file not yet executed on Azure DevOps |
| WP1 core (errors, logging+redaction, naming, bootstrap, registry, versioning) | IMPLEMENTED | LOCAL_VERIFIED | DATABRICKS_INTEGRATION_PENDING |  |
| WP2 DDL specs (53 tables), TableStore (memory + Spark/Delta), migrations | IMPLEMENTED | LOCAL_VERIFIED (memory + local Delta) | DATABRICKS_INTEGRATION_PENDING | UC PK constraints/CLUSTER BY rendering not exercised; 22 runtime/gov tables are DDL_ONLY |
| WP3 YAML pack model (schemas, parser, ids, overrides, bundle) | IMPLEMENTED | LOCAL_VERIFIED | n/a | Env overrides resolved at load; no runtime override code |
| WP4 validator (60 coded rules) | IMPLEMENTED | LOCAL_VERIFIED (each rule has a negative test) | n/a |  |
| WP5 loader / release manager / audit history | IMPLEMENTED | LOCAL_VERIFIED (crash sweep over every write op, concurrency, rollback, compat; local Delta lifecycle) | DATABRICKS_INTEGRATION_PENDING |  |
| WP6 immutable run snapshots | IMPLEMENTED | LOCAL_VERIFIED (incl. tamper detection; BINARY column on local Delta) | DATABRICKS_INTEGRATION_PENDING |  |
| WP7 metadata readers + service facade | IMPLEMENTED | LOCAL_VERIFIED (one contract suite, 3 readers) | DATABRICKS_INTEGRATION_PENDING |  |
| WP8 CLI + sample pack | IMPLEMENTED | LOCAL_VERIFIED | n/a | Synthetic sample pack: metadata/examples/acme_pack |
| connectors / ingestion / processing / quality / reconciliation / audit / orchestration / semantic / documents / monitoring / security / IaC | DESIGNED | — | — | Not started (blocked until M1 acceptance) |
