# 06 — Capability Review, Gap Analysis, Roadmap (Deliverables 19–20, Section 9)

> **Rev 1 notice — superseded parts:** §B (gaps) and §E (roadmap) are updated by the *Rev 1 gap analysis and roadmap* at the end of this file. Tier changes: TMDL → MVP (M5) with deployment tests; Terraform → MVP; crash-injection harness → M2 mandatory; erasure/retention/DR → designed (doc 11), erasure engine MVP-lite in M5, DR tests M6. Where §A cells conflict with Rev 1 docs (07–12), Rev 1 prevails.

All capabilities below are **DESIGNED, NOT IMPLEMENTED**. Tier: **MVP** / **V1** (post-MVP release) / **FUT**.

## A. Capability review matrix
Columns: Responsible component · Metadata required · Execution · On failure · Monitoring · Test · Reuse across clients · Security.

| Capability (Tier) | Component | Metadata | Execution | On failure | Monitoring | Test | Reuse | Security |
|---|---|---|---|---|---|---|---|---|
| Connectors: SQL Server/Azure SQL/PG/MySQL (MVP: SQLServer, AzureSQL, PG) ; Oracle, MySQL (V1) | `connectors/jdbc` | cfg_source, cfg_object | Spark JDBC w/ dialect | retry transient; fail CONFIG | run_error, connector test CLI | contract tests in Docker | add dialect only | KV secrets, MI/SP, TLS |
| Files: JSON/Parquet/CSV (MVP); Avro, XML, Excel (V1) | `connectors/files` | source options, schema | Auto Loader/batch read | bad file → dead-letter | run_file_tracking | contract + golden files | format plugin | UC external location ACLs |
| REST API (MVP basic; V1 full pagination variants) | `connectors/rest` | page_config, auth ref | paginator + checkpoint | backoff/retry; page resume | per-page metrics | mock server tests | paginator plugins | OAuth/secret refs, no secret logs |
| Discovery/profiling/metadata gen (MVP for JDBC+files) | `connectors/discovery` | none (outputs packs) | reads schema/stats | partial output w/ warnings | CLI report | golden pack tests | generic | read-only credential |
| Full/Incr/CDC/Snapshot/File-incr (MVP: Full, Incr, File-incr, CDC change-feed) | `ingestion` | cfg_load_strategy, wm | strategy + batch FSM | restart by state | run_batch | failure-injection | strategies pluggable | n/a |
| Watermark/batch/idempotency (MVP) | `ingestion/wm,batch` | wm_watermark, run_batch | 2-phase commit protocol | recovery on restart | wm lag metric | crash-at-each-step tests | generic | write only by engine SP |
| Schema evolution (MVP: add columns; V1: type widening, rename mapping) | `processing/common` | schema_evolution_mode | compare hash; mergeSchema policy | BREAK → fail+alert | run_schema_snapshot | evolution matrix tests | generic | n/a |
| Bronze/Silver/Gold engines (MVP) | `processing` | cfg_layer_mapping, cfg_column, cfg_transformation | generic builders + SQL/PySpark | stage restart | run_stage | component+e2e | generic | UC grants |
| SCD1/SCD2 (MVP) | `processing/silver/merge` | scd columns, keys | MERGE patterns | idempotent rerun | row-count audit | out-of-order tests | generic | n/a |
| DQ (MVP: null, dup, unique, range, regex, type, freshness, custom SQL; V1: RI, PySpark custom) | `quality` | cfg_dq_rule | batched evaluation | per action policy | dq_result, dashboards | rule contract tests | rule plugins | quarantine tables ACL'd (may hold PII) |
| Reconciliation (MVP: count/sum/distinct/null, S→B,B→S,S→G; V1: hash, PK diff, financial rules) | `reconciliation` | cfg_recon_rule | metric compute both sides | WARN/FAIL | rc_result | seeded-mismatch tests | check plugins | source query read-only |
| Audit/logging (MVP) | `audit` | schema fixed | buffered Delta writes, append | log failure never masks job error; local fallback | self-health check | unit/e2e | generic | no secrets/PII in messages |
| Orchestration (MVP: DAG, parallel, retry, isolate, object+stage restart) | `orchestration` | deps, cfg_execution, retry | planner → Workflows | see doc 04 | run_pipeline | DAG/failure tests | generic | run_as SP |
| Scheduling integration (MVP: Workflows cron; V1: ADF/Airflow trigger) | `orchestration` | schedule_ref | DAB-generated jobs | job alerts | job metrics | deploy tests | generic | SP |
| Semantic (MVP: SQL views/metric views; V1: TMDL) | `semantic` | cfg_semantic_* | generate | validation fail blocks | generation log | golden-file tests | generic | UC grants on views |
| Unstructured/doc path (V1: PDF/Word/text/images+classification; FUT: embeddings/RAG) | `document_processing` | doc source, taxonomy | Volumes → extract → pages table | per-doc fail isolated | doc_status | golden docs | extractor plugin | doc ACL metadata; PII redaction option |
| Security/governance (MVP: KV, MI/SP, UC grants, classification tags; V1: masks, row filters) | `security`, IaC | classification, groups | IaC + generators | deploy fails closed | UC audit | negative-access tests | generic | core |
| Monitoring (MVP: pipeline health, DQ, recon SQL dashboards + failure alerts; V1: SLA/freshness/anomaly/cost) | `monitoring` | SLA config | SQL over audit | n/a | self | query tests | generic | read-only role |
| Perf/cost (MVP: maintenance jobs, cluster profiles; V1: cost views) | `deployment`, `monitoring` | maintenance cfg | scheduled jobs | skip+alert | OPTIMIZE metrics | perf tests | generic | n/a |
| Lifecycle (MVP: SemVer, migrations, validation, DAB deploy, CI; V1: automated rollback tooling) | `deployment`, `metadata` | cfg_framework_version | CLI/CI | halt promotion | release log | upgrade/rollback tests | generic | signed artifacts |
| AI-assist (V1: docs, test, DQ/metadata suggestions with approval; FUT: failure analysis) | `docs/ai`, `cli` (optional) | none runtime | dev-time only | n/a | PR labels | review gate | generic | no data/secrets to LLM |

## B. Missing capabilities identified (not in the brief)
1. **Environment override mechanism** for per-env metadata params (design pending).
2. **Data contracts / schema registry** for upstream change notification.
3. **Metadata authoring UX** (CLI only in MVP; no UI).
4. **Backup/DR** for metadata & ADLS (client IaC concern; runbook needed).
5. **Data retention / GDPR delete & right-to-be-forgotten** across layers.
6. **Late-binding of secrets rotation** handling and connection health checks.
7. **Capacity/quota governance** (Workflows concurrency, JDBC source protection – max connections per source).
8. **Streaming** sources (explicitly out of scope).
9. **Multi-source Gold joins & cross-project dependencies** semantics.
10. **Dev sandbox/test-data management** with masking.
11. **SLA/runbook & support model** definition.

## C. Technical risks
| Risk | Impact | Mitigation |
|---|---|---|
| No multi-table ACID in Delta | partial commit | ADR-0004 recovery protocol; txnVersion idempotency; tests with crash injection |
| Delta MERGE concurrency conflicts for parallel objects on same target | retries/slowdown | one writer per target; row-level concurrency/liquid clustering; retry on conflict |
| JDBC extraction throughput/source impact | missed SLAs, source load | partitioned reads, per-source connection caps, off-peak schedule, CDC where possible |
| Over-generic engine complexity ("metadata hell") | slow dev, hard debug | strong validator, `plan --explain`, escape hatches via SQL/PySpark transforms |
| Library availability (Oracle driver licensing, XML/Excel libs) | blocked connectors | document driver install; V1 timing |
| OCR/AI cost & quality | spend, accuracy | pluggable, sampling, budget caps |
| Power BI TMDL/DAX generation fragility | rework | restrict to supported patterns; golden tests |
| Workflows limits (task counts, for_each) | scale cap | chunking objects into groups |
| Metadata–code version drift | runtime breakage | schema version gate at start |
| Scope: 15 source types × 3 load modes | schedule | MVP tiers above |
| "Minimal customization" claim unproven | expectation | validate with 2 pilot clients before claiming |

## D. Dependencies
Databricks Premium + UC; metastore admin rights per client; network access to sources; Key Vault & SP provisioning by client; Terraform provider for Databricks/Azure; artifact feed; licenses (Document Intelligence, Oracle driver); client acceptance of metadata-pack change process.

## E. MVP implementation roadmap
(Maps to your 10 phases; durations are indicative T-shirt sizes, to be re-estimated with team size.)

| Milestone | Phases | Contents | Exit criteria (tests required) |
|---|---|---|---|
| M0 | 1 | Design approval, decisions Q1–Q11, repo scaffold, CI skeleton, ADRs | Approved docs |
| M1 (L) | 2 | Metadata DDL, migrations, DAO, validator, pack loader, versioning/history, snapshot | Unit+component tests; migration up/down |
| M2 (L) | 3,4 | Connector ABC+registry; SQL Server/Azure SQL/PG, files(JSON/Parquet/CSV), REST basic; discovery; Full/Incr/File-incr/CDC-feed; watermark/batch FSM | Contract tests; crash-injection idempotency tests |
| M3 (L) | 5 | Bronze/Silver (cleanse, dedupe, SCD1/2, evolution)/Gold (SQL/PySpark transforms) | E2E synthetic source→gold |
| M4 (M) | 6,7 | DQ + recon engines (MVP rule sets); audit logging; planner/executor/retry/restart; Workflows generation | Failure-isolation & restart tests |
| M5 (M) | 8,9 | SQL/metric-view semantic; security gens, monitoring dashboards, maintenance jobs | Security negative tests; dashboards |
| M6 (M) | 10 | CI/CD, IaC modules, client template, upgrade/rollback test, pilot deployment into 1 synthetic + 1 real client DEV | Pilot signed off |
| V1 | – | Oracle/MySQL, Avro/XML/Excel, REST variants, TMDL, doc pipeline, recon hash/PK/financial, SLA/anomaly/cost, masks, AI-assist tooling | |
| FUT | – | Embeddings/RAG, streaming, UI, ADF/Airflow adapters, auto-rollback | |

## F. Decisions requested to start M0
1. Q1 metadata authoring model (packs-in-Git → Delta). 2. Q2 repo model. 3. Q4 env/workspace model. 4. Q5 orchestration. 5. Q9 OCR engine. 6. Q11 Power BI approach. 7. Confirm MVP tiering above. 8. Confirm IaC tool (Terraform). 9. Confirm sizing assumptions (Q15).

---
# Rev 1 — Gap analysis update and roadmap

## R1. Gap status after Rev 1
| Gap (original §B) | Rev 1 status |
|---|---|
| 1 Env override mechanism | **Closed by design**: resolved at load time (doc 07 C2, doc 09 §2) |
| 2 Data contracts / schema registry | Open (V1) |
| 3 Metadata authoring UX | **Decided**: Git workflow in MVP, UI later via `MetadataService` (ADR-0016) |
| 4 Backup/DR | **Designed** (doc 11); unproven until DR drills |
| 5 Retention / GDPR erasure | **Designed** (doc 11, ADR-0012); engine MVP-lite in M5; legal scope is client's |
| 6 Secret rotation/connection health | Open (V1) |
| 7 Capacity/quota governance (source connection caps) | Open (M2 design item) |
| 8 Streaming | Out of scope |
| 9 Cross-project Gold dependencies | Open (M4 design item) |
| 10 Dev sandbox/test-data management | Open (V1) |
| 11 SLA/runbook & support model | Open (M6) |
**New gaps/risks from Rev 1:** (N1) Spike S1: MERGE commit tagging/history retention (M2); (N2) Power BI/Fabric capacity + tenant settings needed for T3–T5; (N3) erasure vs. time-travel/backup lag needs client legal policy; (N4) deletion-vector/purge behaviour on target DBR to be verified; (N5) metadata scale (5,000 objects) unmeasured; (N6) Terraform provider limits for UC/metastore automation need client-side admin roles; (N7) Azure DI cost/throughput & private-endpoint availability; (N8) per-layer cursor semantics increase state-management complexity (mitigated by doc 08 tests).

## R2. Revised roadmap (MVP vs future)
| Milestone | Phases | Scope | Gate (evidence required) |
|---|---|---|---|
| M0 | 1 | Rev 1 design approval | Approval recorded |
| **M1** | 2 | Metadata repo & engine (doc 12) | unit+component tests; Databricks integration pending workspace |
| M2 | 3,4 | Connectors (SQL Server/Azure SQL/PG, files, REST basic), discovery, Full/Incr/File-incr/CDC-feed, **manifest/receipt/cursor protocol, FaultInjector + crash matrix**; Spike S1 | full crash-injection matrix green |
| M3 | 5 | Bronze/Silver/Gold, SCD1/2, schema evolution | E2E synthetic; crash matrix extended to Silver/Gold |
| M4 | 6,7 | DQ, recon, audit, planner, executor, retry/restart, `OrchestrationProvider` + Workflows provider | failure-isolation, restart, provider conformance tests; planner scale test |
| M5 | 8,9 | SQL views, metric views, **TMDL generator**, security generators, erasure/retention engine (MVP-lite), monitoring dashboards | T1–T2 semantic tests; erasure propagation tests; T3–T5 when Power BI capacity available |
| M6 | 10 | Terraform modules (all topologies), CI/CD, client template, upgrade/rollback, DR drills, scale/perf tests, pilot | DR metadata rebuild + Delta restore drills; measured RPO/RTO; scale results published |
| V1 | – | Oracle/MySQL, Avro/XML/Excel, REST variants, **document ingestion + Azure DI extraction + classification**, recon hash/PK/financial, SLA/anomaly/cost, masks/row filters, AI-assist tooling, data contracts | |
| V2/FUT | – | embeddings/vector/RAG, metadata UI (on `MetadataService`), ADF/Airflow providers, streaming, auto-rollback | |
