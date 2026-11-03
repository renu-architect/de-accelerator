# M1 – Metadata Management Foundation

**Source:** `m1-metadata-engine` → **Target:** `main`  ·  **Proposed tag after merge:** `m1.0.0`
**Status: M1 accepted conditionally as `LOCAL_VERIFIED`. No module is `PRODUCTION_READY`.**

## Summary
First implementation milestone of the metadata-driven data engineering accelerator: the metadata repository and management engine, plus the approved Phase 1 / Revision 1 architecture documents it implements. No ingestion, medallion, DQ/reconciliation, orchestration, semantic or document-processing code is included.

### What is implemented (package `de_accelerator`)
- **core** – error taxonomy, secret-redacting logging, naming (`<client>_<env>`), bootstrap YAML loader, plugin registry interface, version/compatibility helpers.
- **metadata/schema + store + migrations** – entity definitions as single source of truth; 53 table specs; `TableStore` (in-memory and Spark/Delta); ordered, checksummed, idempotent migrations with dry-run and guarded rollback (schema v1 checksum pinned).
- **metadata/packs** – YAML metadata pack format with generated JSON-Schemas; deterministic ids/hashes; environment overrides resolved **at load time only**; immutable per-environment bundles.
- **metadata/validation** – 60 coded rules (`MDV-nnnn`), each with a negative test; secrets only as `kv:` references; personal-identifier patterns rejected.
- **metadata/release** – stage → verify → activate; rollback by active-release pointer; optimistic concurrency; crash healing; row-level audit history.
- **metadata/snapshot** – immutable, content-hashed run configuration snapshots (retain release id), tamper detection.
- **metadata/reader + service + cli** – typed readers (Delta, Snapshot, In-memory; one contract suite), read-only `MetadataService`, `de-accel` CLI.

### Schema-only (DDL_ONLY, no logic): 22 tables
`audit` (14), `dq` (1), `recon` (2), `gov` (5).

### Planned / future (not implemented)
`docs` schema and four `docs.*` tables (ADR-0018 #6) are a future capability; this PR keeps 8 schemas / 53 tables / 22 schema-only.

## Test results (re-verified on this tree, local only)
| Check | Result |
|---|---|
| pytest (unit + local-Delta component) | **182 passed, 0 failed, 0 skipped** (5 min 35 s) |
| Line coverage | **95%** (2,407 statements, 111 missed) |
| ruff | All checks passed |
| mypy `--strict` | No issues in 48 source files |
Environment: Python 3.11.18, pyspark 3.5.9, delta-spark 3.2. Highlights: crash injection before every write of stage/activate/rollback (state always consistent, re-run idempotent); 4-way concurrent activation (exactly one winner); snapshot tamper detection; release lifecycle, snapshots, readers and CLI on real local Delta tables. Evidence: `docs/status/TEST_EVIDENCE.md`.

## Architecture decisions
Approved Rev 1 decisions (docs 07–13, ADR-0007…0017) and ADR-0018 (approved): snapshot id includes release id; history key includes pointer `activation_seq`; dedicated `docs` schema planned as a future capability (originals in ADLS Gen2, metadata/state in Delta). Implemented catalog layout: `meta, audit, dq, recon, gov, bronze, silver, gold` (`docs` planned).

## Known limitations
- **Databricks integration pending** — Unity Catalog, secret scopes, grants and Workflows untested (`DATABRICKS_INTEGRATION_PENDING` on every module).
- **Azure DevOps pipeline pending** — `deployment/ci/azure-pipelines.yml` has never been executed.
- **Scale benchmark pending** — 500 / 2,500 / 5,000-object measurement not run; sizing remains an unmeasured design target.
- Runtime matrix (Python 3.11 / Spark 3.5) not verified against a specific Databricks LTS.
- Concurrency tested with in-memory atomics, not concurrent Delta writers.
- The `docs` schema is a planned future capability; the implemented catalog has 8 schemas / 53 tables.
- Per-layer cursors, CDC retention behaviour and ingestion crash-injection are M2 scope (schema/validation only here).

## Not in this PR
Any M2 work (connectors, ingestion engine), Terraform/IaC, UI, document processing behaviour.

## Review notes
Please do not merge until acceptance conditions are confirmed. Release tag `m1.0.0` only after approval and merge.
