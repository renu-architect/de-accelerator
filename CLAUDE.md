# CLAUDE.md — Enterprise Metadata-Driven Data Engineering Accelerator

Permanent project instructions. Read fully before any change.

## 1. What this is
ONE reusable accelerator codebase (Azure Databricks, Python, PySpark, Delta, ADLS Gen2, Unity Catalog, Key Vault).
Deployed **independently per client** (own Azure subscription/workspace/ADLS/UC/Key Vault/metadata/logs).
NOT a multi-tenant central platform. Client differences = metadata, config, or approved extension interfaces. Never fork or edit core per client.

## 2. Current phase
**M1 (metadata engine) and M2 (ingestion foundation, JDBC connector, batch engine, Bronze Delta ingestion) are CLOSED as `LOCAL_VERIFIED`** (M2: 398 tests, 95 % coverage, ruff + strict mypy clean, commit `51270e6` on `m2-ingestion`). **Pending external validation:** real SQL Server / Azure SQL, Databricks / Unity Catalog, Azure DevOps, scale. **Production readiness is not established for any module.** Discovery, file connectors, REST, CDC and deletes are future capabilities (see `docs/status/KNOWN_LIMITATIONS.md`).
**M3 (Bronze → Silver → Gold transformation) is IN PROGRESS on branch `m3-design`**: design APPROVED 2026-11-04 (`docs/architecture/m3/`, ADR-0020; phases P0–P5, estimate 43 person-days, range 38–48). M1/M2 behaviour must not change and no M2 file is edited; delete-by-absence is deferred to M4; status stays `LOCAL_VERIFIED` at best. Architecture baseline: Rev 1 + ADR-0018/0019. Module status: `docs/status/IMPLEMENTATION_STATUS.md`.

### Approved decisions (summary; details doc 07)
Git metadata packs = config source of truth → CI validation → Delta (release-versioned, insert-only, atomic activation pointer) → immutable per-run snapshot · core repo + one deployment repo per client; extensions via interfaces only · default separate DEV/QA/PROD workspaces, configurable topology, Terraform, Workflows MVP behind `OrchestrationProvider` · durable batch manifests, receipts, conditional watermarks, per-layer cursors, mandatory crash-injection matrix (doc 08) · erasure/retention/suppression, backup/DR (doc 11) · Azure AI Document Intelligence default OCR behind `OcrProvider`; advanced docs/RAG later · semantic = SQL views + metric views + TMDL with real Power BI deployment tests · sizing 500–5,000 objects / ≤10 TB are **design targets, not guarantees** · metadata UI outside MVP (UI-ready `MetadataService`).

### Catalog layout
`<client>_<env>` catalog; **implemented** schemas: `meta, audit, dq, recon, gov, bronze, silver, gold` (8). A `docs` schema for document runtime tables is a **planned future capability** (originals in ADLS Gen2, metadata/state in Delta) and is not implemented.

### M1 status
M1 closed as **LOCAL_VERIFIED** (no module is PRODUCTION_READY). Pending: Databricks integration, Azure DevOps pipeline execution, scale benchmark. M2 requires explicit design approval before any implementation.

### Baseline rule
Approved architecture = baseline. Any deviation requires a written change request (impact on docs/ADRs/modules/tests) and user approval BEFORE implementation. Supersede ADRs, never silently edit.

## 3. Governance rules (binding)
1. Never modify an approved architecture decision (ADR) or an existing core module without: (a) written impact analysis, (b) user approval. Supersede ADRs; never edit accepted ones silently.
2. Never duplicate existing functionality. Search the repo and `docs/architecture/` first.
3. Status vocabulary is `LOCAL_VERIFIED` / `DATABRICKS_INTEGRATION_PENDING` / `PRODUCTION_READY` (never `COMPLETE`); link test evidence in `docs/status/TEST_EVIDENCE.md`. Never claim PRODUCTION_READY from local tests alone.
4. A capability in a diagram is NOT implemented. Only `IMPLEMENTATION_STATUS.md` states truth.
5. AI-generated code/config/metadata must go through human review + CI + approval before PROD. No LLM is required for deterministic ETL runtime.
6. Never hardcode secrets, tokens, hostnames, client names, catalog names in core. Secrets only via Key Vault-backed secret scopes / UC service credentials / Managed Identity.
7. Keep these current: CLAUDE.md, ADRs (`docs/adr/`), design docs (`docs/architecture/`), module specs, `docs/status/*` (implementation status, known limitations, test evidence, release notes).

## 4. Architecture principles
Metadata-driven · config over code · modular/SOLID · separation of concerns · idempotent · restartable · pluggable connectors · client-independent deploys · environment-aware · secure by default · tested · centrally governed · observable · backward compatible.

## 5. Hard design invariants
- Static metadata (`cfg_*`) is separated from runtime state (`run_*`, `wm_*`). Core code never writes to `cfg_*` at runtime except via the metadata-engine API (versioned + history).
- Watermarks/cursors advance only AFTER a *verified* target commit receipt, via conditional update (ADR-0009, doc 08). Never advance on read. Batch windows are persisted in an immutable manifest before reading.
- Engines read only the immutable run config snapshot; only the metadata loader writes `cfg_*` (insert-only).
- No personal data in metadata/audit tables; erasure uses HMAC tokens (doc 11).
- Every write is idempotent by `(object_id, batch_id)`.
- Every public module ships with a spec in `docs/architecture/modules/` and unit tests.
- Core imports no client-specific code. Extensions load only through registered interfaces (connectors, transforms, DQ rule plugins).
- YAML is used for (a) bootstrap/deploy settings and (b) Git metadata *packs* (authoring/change-control format). Runtime metadata = Delta tables only; engines never read pack YAML at runtime.

## 6. Conventions (to apply once implementation is approved)
Python ≥3.10 packaged as wheel `de_accelerator`; PySpark; type hints; ruff + mypy; pytest; semantic versioning; metadata schema versioned independently (`metadata_schema_version`) and migrated via ordered migration scripts.

## 7. Key documents
- `docs/architecture/00_ambiguities_and_assumptions.md`
- `docs/architecture/01_architecture_overview.md`
- `docs/architecture/02_metadata_design.md`
- `docs/architecture/03_repository_and_responsibility.md`
- `docs/architecture/04_flows_and_workflows.md`
- `docs/architecture/05_cross_cutting_designs.md`
- `docs/architecture/06_capability_review_gaps_roadmap.md` (+ Rev 1 roadmap at end)
- `docs/architecture/m2/` (M2 ingestion design, proposed)
- `docs/architecture/07_revision1_decisions_and_impacts.md` · `08_recovery_protocol_and_crash_testing.md` · `09_metadata_engine_detailed_design.md` · `10_deployment_semantic_documents_sizing_ui.md` · `11_governance_erasure_retention_dr.md` · `12_M1_implementation_plan.md`
- `docs/adr/ADR-0001-to-0006.md`, `ADR-0007-to-0017.md` · `docs/status/*`
