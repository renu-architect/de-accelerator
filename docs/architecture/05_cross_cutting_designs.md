# 05 — Semantic, Security, Performance, Versioning, Testing (Deliverables 13, 14, 16, 17, 18)

> **Rev 1 notice:** Semantic outputs are Databricks SQL views, UC metric views **and** Power BI TMDL, with real deployment/validation tests (doc 10 §3). Security is extended with GDPR erasure propagation, retention, deletion audit, backup/DR (doc 11). Versioning/rollback additionally covers metadata release pointer flip (doc 09 §5) and post-restore consistency (doc 11 §4). Testing additionally mandates crash-injection (doc 08 §7), DR drills (doc 11 §5), Power BI T1–T5 (doc 10 §3) and scale tests (doc 10 §5).

## 13. Semantic layer design
- **Metadata:** `cfg_semantic_entity` (fact/dim, grain, source gold table), `_measure` (expression, aggregation rule, format, KPI flag/target), `_relationship` (from/to column, cardinality, filter direction), `_glossary` (term, definition, owner, linked columns).
- **Generators (pluggable `SemanticGenerator`):**
  1. *Databricks SQL views / UC metric views* (YAML-spec'd metrics) — primary, always available.
  2. *TMDL / PBIP* for Power BI (tables, columns, relationships, measures as DAX templated from aggregation rules; DirectLake not assumed; Import/DirectQuery via Databricks connector).
  3. Glossary/lineage export to UC comments/tags.
- **Flow:** validate semantic metadata against Gold schema (columns exist, types agree, keys unique on dim side) → generate artifacts → write to client deployment repo/PR → CI validates (TMDL parse, SQL compile) → deploy by client's BI pipeline.
- **Failure:** validation errors block generation; generated artifacts are versioned and never auto-published to PROD Power BI.
- **Limitations:** complex DAX (time intelligence, RLS) → handled via approved DAX snippet templates; arbitrary DAX = extension.

## 14. Security & governance design
| Control | Design |
|---|---|
| Secrets | Key Vault-backed Databricks secret scopes; metadata stores `kv:` references only; `SecretProvider` abstraction; no secrets in logs (redaction filter) |
| Identity | Managed Identity (Access Connector) for ADLS/UC storage credentials; Service Principal for CI/CD & jobs (OAuth M2M) with job `run_as`; MI for Key Vault where supported; user identities for interactive |
| RBAC / least privilege | Groups per role (admin, engineer, steward, consumer, ci); UC grants generated from metadata/IaC (catalog/schema/table); jobs run as SP with only needed grants; `meta.cfg_*` writable only by metadata-engine SP |
| Isolation | Per-client Azure subscription/workspace/metastore/storage/KV/logs; no shared services; per-env workspaces |
| Encryption | ADLS at rest (CMK optional), TLS in transit, Delta column-level none by default; PII encryption/tokenization as extension |
| Classification | `cfg_column.classification/is_pii` + UC tags; rule-based scan suggests, steward approves |
| Masking | UC column masks & row filters generated from classification + group membership; non-prod uses synthetic/masked copies |
| Audit trail | `run_*` tables, `cfg_config_history`, UC audit logs (system tables) retained per policy |
| Network | Private endpoints, VNet injection, secure cluster connectivity (IaC) |
| Supply chain | Pinned wheel, signed artifacts, dependency scan in CI |
| Docs/RAG | Document-level ACLs carried as metadata; vector search results filtered by entitlement |
| AI assist | Prompts contain schemas/metadata only, never data/secrets; outputs reviewed |
Test: grant tests (negative access tests) in integration suite; secret-scan in CI.

## 16. Performance & cost strategy
- **Partitioning:** default *no* partitioning < 1 TB; prefer liquid clustering on configured keys; date partition only for very large append tables.
- **File size/maintenance:** auto-optimize/predictive optimization where available; scheduled `OPTIMIZE`, `VACUUM` (retention per config), `ANALYZE` driven by `cfg_layer_mapping.maintenance` metadata.
- **Incremental by default**; full loads only when configured.
- **JDBC:** partitioned reads (`partitionColumn/numPartitions`), fetchsize, pushdown predicates.
- **Cluster:** job clusters per profile (small/medium/large) with autoscale and spot where allowed; Workflows `for_each` for parallel objects; shared-cluster batching for many small objects; concurrency cap in `cfg_execution`.
- **Spark:** AQE on, broadcast joins hints in transform metadata, avoid `collect`, DQ rules batched to minimize actions, cache only when reused.
- **Cost tracking:** job/cluster tags (`client, project, pipeline, object_group, env`); view over `system.billing.usage` + `run_pipeline` for cost-per-pipeline/GB; budgets/alerts in IaC.
- **Benchmarks required** before claiming targets (no performance claims in design).

## 17. Framework versioning & upgrade strategy
- **SemVer** on wheel (`MAJOR.MINOR.PATCH`); independent `metadata_schema_version`.
- **Compatibility:** MINOR/PATCH never break metadata or runtime tables; MAJOR may, with migration scripts + deprecation window of ≥1 MINOR. Engine refuses to run if wheel's `min_compatible_schema > deployed schema`.
- **Upgrade:** release notes → client PR bumps pin → CI validates metadata packs against new schema → `cli migrate --dry-run` → apply DEV → regression suite → QA → PROD (window). Migrations are ordered, idempotent, checksummed, recorded in `cfg_schema_migration`.
- **Rollback:** previous wheel kept; migrations have `down` scripts where possible; for destructive changes pre-migration Delta `RESTORE` points / table clones are recorded. Rollback runbook per release. Expand/contract pattern for schema changes (additive first).
- **Release train:** `main` (releasable) → tag → build → sign → publish. Hotfix branches. Release notes in `docs/status/RELEASE_NOTES.md`.

## 18. Testing strategy
| Level | Scope | Tooling |
|---|---|---|
| Unit | pure logic: validators, planners, watermark state machine, rule evaluators, retry | pytest, local Spark session |
| Component | each engine with local Delta (spark-delta), synthetic datasets | pytest + chispa |
| Contract | every `Connector/DqRule/ReconCheck/Extractor` implementation vs interface suite | shared contract tests, dockerized DBs (SQL Server, PG, MySQL, Oracle XE) |
| Integration | on Databricks DEV workspace via Databricks Connect/Bundles: UC, secrets, Workflows | pytest + DAB |
| E2E | synthetic client: source → gold → semantic; restart/failure injection; idempotency (run twice → same result) | scenario suite |
| Non-functional | performance (volume tiers), security negative tests, upgrade/rollback test | scheduled |
| Metadata | JSON-schema + referential validation of packs in client repos | CI gate |
| AI artifacts | same gates + mandatory human approval label | CI policy |
Rules: no module `COMPLETE` without passing tests recorded in `docs/status/TEST_EVIDENCE.md`; coverage gate (proposed 85% core logic); failure-injection tests mandatory for ingestion/orchestration.
