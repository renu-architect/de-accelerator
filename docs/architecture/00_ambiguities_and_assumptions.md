# 00 — Ambiguities, Assumptions, Decisions Needed

## Rev 1 resolution status
**Resolved by user approval:** Q1 (Git packs → Delta), Q2 (core + per-client repos), Q4 (separate workspaces per env by default; configurable topology), Q5 (Workflows MVP + provider interface), Q9 (Azure AI Document Intelligence default, replaceable), Q11 (SQL + metric views + TMDL, with real Power BI deployment tests), Q15 (500–5,000 objects, ≤10 TB as **design targets, not guarantees**). IaC = **Terraform**. Metadata UI = outside MVP. See doc 07.
**Still open (defaults stand unless you object):** Q3, Q6–Q8, Q10, Q12–Q14, Q16–Q18; M1 open items in doc 12 §8.
Added/updated assumptions: Rev 1 adds *effectively-once* semantics (doc 08 §8); RPO/RTO are proposed targets (doc 11); Power BI T3–T5 tests depend on a Fabric/Power BI capacity (doc 10 §3); Databricks integration tests depend on a DEV workspace (doc 12 §6).

## A. Ambiguities in the requirements (original Phase 1 list)

| # | Ambiguity | Why it matters | My recommended default |
|---|-----------|----------------|------------------------|
| Q1 | "YAML only for bootstrap" but "Delta as primary metadata". **How is metadata authored?** | Direct table edits give no review/promotion trail; DEV→QA→PROD needs controlled change. | Author as versioned **metadata packs** (JSON/CSV/YAML in Git, per client) → validated in CI → **loaded to Delta** by the metadata engine. Delta is the only runtime source; Git is the change-control source. Ad-hoc table edits are blocked by UC grants. (Exception to "YAML only bootstrap" — needs your OK.) |
| Q2 | "Common codebase" vs "client-specific deployments": one repo or repo + per-client overlay? | Affects release flow and isolation of client config. | Core repo (product) + **one private deployment repo per client** holding bootstrap YAML, metadata packs, extensions. Core consumed as a pinned wheel. |
| Q3 | Isolation granularity: one catalog per client-env, or per project? | UC governance, cost tags, grants. | Catalog per **env** (`<client>_<env>`), schemas per project+layer. Client = whole Azure environment so no cross-client catalogs exist. |
| Q4 | Environments: separate workspaces per DEV/QA/PROD or one workspace with catalogs? | Cost, security, promotion. | Separate workspace per env **inside** each client subscription (recommended); single-workspace supported by config. |
| Q5 | Orchestration engine: Databricks Workflows only, or pluggable (ADF/Airflow)? | Req. says Workflows; scheduling integration is vague. | Workflows is the executor; orchestration logic (graph, retry, restart) is in the framework; ADF/Airflow can trigger a Workflow job (thin adapter, post-MVP). |
| Q6 | Source scale/latency: batch only, or streaming (Event Hubs/Kafka)? Not listed. | CDC ≠ streaming. | **Batch/micro-batch only**. Streaming = future enhancement. |
| Q7 | CDC sources: log-based tools (Debezium, SQL Server CT/CDC, Oracle GoldenGate) or query-based? | Determines connector capability. | Support (a) change-feed tables already landed as files/tables (op-code column), (b) SQL Server CT/CDC via JDBC, (c) query-based diff via watermark/hash. Log-based capture products are out of scope; their landed output is in scope. |
| Q8 | Oracle/SQL Server drivers & networking (private link, SHIR/VNet injection). | Connectivity is client-specific. | JDBC drivers installed as cluster libraries; network is a deployment (IaC) concern. Lakehouse Federation optional. |
| Q9 | OCR / extraction engine for unstructured. | Cost, residency. | Pluggable `Extractor`; default **Azure AI Document Intelligence** (OCR/layout), fallback open-source (pdfplumber, python-docx, Tesseract). Databricks `ai_parse_document` as optional. |
| Q10 | Embeddings/Vector Search: Databricks Vector Search + Foundation Model API or Azure OpenAI / AI Search? | Optional feature; client policy varies. | Interface `EmbeddingProvider` + `VectorIndexer`; default Databricks Vector Search. Off by default. |
| Q11 | Power BI integration depth: generate TMDL/PBIP files, or push via XMLA/REST? | Determines semantic generator. | Generate **TMDL** (PBIP-compatible) + Databricks UC **metric views**/SQL views. Deployment via Fabric/Power BI deployment pipelines is client side. |
| Q12 | SCD2 for Gold dimensions or Silver only? | Requirement lists SCD1/2 under Silver. | SCD1/2 engine is a shared component usable by Silver and Gold dimensions. |
| Q13 | Masking: dynamic (UC column masks / row filters) vs physical? | Classification-driven. | UC column masks + row filters driven by classification tags; physical masking optional for non-prod copies. |
| Q14 | Cost tracking: how granular? | Needs tags. | Job/cluster custom tags (client, project, pipeline, object) + `system.billing.usage` join. |
| Q15 | Volumes and SLA targets (rows/day, # objects, max runtime). | Cluster sizing, concurrency defaults. | Not provided → design for 500–5,000 objects/client, ≤10 TB; to be validated. |
| Q16 | Delete handling: soft vs hard? | Silver/Gold semantics. | Per-object config `delete_mode` = `soft` (default) / `hard` / `ignore`. |
| Q17 | Data residency / Unity Catalog metastore topology (one metastore per region). | Governance. | One UC metastore per client region; created in IaC. |
| Q18 | PII classification: manual, rule-based or ML scanning? | Security. | Rule/regex + tag-driven (column-name & profile patterns) with human approval; optional Databricks data classification. |

> Org policy note: sample records and tests use synthetic data only; no SSN/Aadhaar/PAN/passport/licence/card/patient data is requested, stored or shown by this design.

## B. Working assumptions (valid until you correct them)
1. Azure only; Databricks on Premium tier with Unity Catalog enabled; serverless optional (config).
2. Delta Lake is the only table format; ADLS Gen2 accessed via UC External Locations / Volumes.
3. One framework wheel version per client-env at a time; upgrades promoted DEV→QA→PROD.
4. Client network/connectivity is built by IaC (Terraform or Bicep — decision pending, Terraform recommended given Databricks provider).
5. Python 3.10+, DBR LTS 15.4+ (or current LTS at build time), PySpark 3.5+.
6. Metadata volume is small (≤ ~100k rows); metadata reads are cached per run into a **frozen run-config snapshot**.
7. Framework produces deterministic ETL; Claude is dev-time/ops-assist only.
8. Delta has no multi-table transactions → cross-table consistency uses the recovery protocol in ADR-0004.

## C. Technical risks (summary; full register in doc 06)
No multi-table ACID; JDBC throughput on legacy DBs; Oracle/XML/Avro/Excel library dependencies; OCR cost; Power BI TMDL churn; Workflows task/for-each limits; framework↔metadata schema drift; "minimal customization" claim depends on connector coverage.

## D. Decisions requested
Please answer Q1, Q2, Q4, Q5, Q9, Q11 at minimum (others default as recommended). See end of doc 06.
