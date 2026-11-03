# 03 — Repository Structure & Responsibility Matrix (Deliverables 7–8)

## Repository model
- **Core repo** `de-accelerator` (this one): product, versioned releases (SemVer tags), wheel published to client artifact feed.
- **Client deployment repo** `de-accel-<client>` (one per client, private): bootstrap YAML, metadata packs, extensions, environment overrides, IaC variables. Pins core version.

## Core repo structure

```
de-accelerator/
├─ CLAUDE.md
├─ pyproject.toml                      # wheel: de_accelerator
├─ src/de_accelerator/
│  ├─ core/                 # context, bootstrap config (YAML), plugin registry, secrets, errors/retry, logging, spark factory, naming
│  ├─ metadata/             # repository DAO, DDL, migrations/, validator, snapshot builder, versioning/history, pack loader
│  ├─ connectors/
│  │   ├─ base/             # Connector ABC, capabilities, contracts
│  │   ├─ jdbc/             # sqlserver, azuresql, oracle, mysql, postgres dialects
│  │   ├─ files/            # json, xml, parquet, avro, excel, text
│  │   ├─ rest/             # auth, paginators (offset, cursor, link), rate limit
│  │   ├─ discovery/        # schema extraction, profiling, metadata auto-generation
│  │   └─ registry.py
│  ├─ ingestion/            # load strategies (full, incr, cdc, snapshot, file_incr, api), watermark mgr, batch mgr, lineage, backfill/reprocess
│  ├─ processing/
│  │   ├─ bronze/           # raw landing, audit columns, schema/file tracking
│  │   ├─ silver/           # cleanse, standardize, dedupe, typecast, BK validation, merge/SCD1/SCD2
│  │   ├─ gold/             # dims, facts, aggs, marts
│  │   ├─ transforms/       # SQL & PySpark runners, builtin transform library
│  │   └─ common/           # writers, schema evolution, delta helpers
│  ├─ quality/              # rule interface, builtin rules, quarantine, scoring, DQ logging
│  ├─ reconciliation/       # checks, comparators, tolerances, mismatch store
│  ├─ audit/                # pipeline/object/batch/dq/recon/error logging, retry history, lineage store
│  ├─ orchestration/        # planner, DAG, executor, concurrency, retry, restart manager, workflow job generator
│  ├─ semantic/             # entities, measures, KPI, glossary, generators (TMDL, SQL views, UC metric views)
│  ├─ document_processing/  # raw ingest, extractors, OCR, classification, chunking, embeddings, vector index, RAG adapters
│  ├─ monitoring/           # SLA, freshness, volume anomaly, alerting, cost tracking, dashboard SQL
│  ├─ security/             # classification, masking policies, grant generators, audit-trail helpers
│  └─ cli/                  # validate-config, deploy-metadata, migrate, plan, run, restart
├─ deployment/
│  ├─ iac/                  # terraform modules: workspace, UC, ADLS, KV, networking, SPs/MI, cluster policies
│  ├─ workflows/            # Databricks Asset Bundle templates (jobs, clusters)
│  ├─ ci/                   # azure-pipelines / GitHub Actions templates
│  ├─ migrations/           # metadata schema migrations (ordered, reversible)
│  └─ client_template/      # scaffold for a new client deployment repo
├─ metadata/
│  ├─ schema/               # JSON-schema for packs, DDL
│  └─ examples/             # synthetic sample packs
├─ tests/ { unit, integration, e2e, contract (connectors), data (synthetic), perf }
└─ docs/ { architecture/, adr/, modules/, runbooks/, status/, ai/ (prompt library & approval workflow) }
```

## Client deployment repo (template)
```
de-accel-<client>/
├─ bootstrap/{base.yaml, dev.yaml, qa.yaml, prod.yaml}   # workspace, catalog, KV scope, framework version, cluster profile
├─ metadata_packs/<project>/{sources,objects,columns,load,mappings,dq,recon,semantic,deps}/
├─ extensions/{connectors,transforms,dq_rules}/           # approved interface implementations only
├─ sql_transforms/ pyspark_transforms/
├─ iac/{terraform.tfvars}
└─ pipelines/ (azure-pipelines.yml)
```

## Responsibility matrix: Core vs Client

| Concern | Core accelerator | Client-specific (metadata/config/extension) |
|---|---|---|
| Connector implementations (standard 15 types) | ✔ | Custom connector via `Connector` interface (extension) |
| Connection details, credentials | Secret provider mechanism | Key Vault secrets, source config |
| Load logic (full/incr/CDC...) | ✔ engines | Choice + parameters per object |
| Medallion engines & merge/SCD | ✔ | Mapping config, keys, SCD cols |
| Business transformations | Runner + builtin library | SQL/PySpark transform files |
| DQ engine & rule types | ✔ | Rule rows, thresholds, custom rules |
| Reconciliation engine | ✔ | Rule rows, tolerances |
| Audit/logging schema | ✔ | Retention, alert recipients |
| Orchestration logic | ✔ | Dependencies, schedules, concurrency |
| Semantic engine & generators | ✔ | Entities, measures, KPIs, glossary |
| Document pipeline | ✔ | Classification taxonomies, extractor choice |
| Metadata schema & migration | ✔ | Metadata pack content |
| Infra modules | ✔ (IaC modules) | tfvars, network topology, naming |
| Security mechanisms | ✔ | Roles/groups, classifications, masking policies |
| Cluster/cost profile | Policy templates | Sizes, budgets |
| Framework version | Release | Pinned version & upgrade timing |
| Change to core behavior | Only via core release (approval) | **Not allowed** in client repo |

**Extension rule:** an extension is accepted only if it implements a published interface, passes the contract test-suite for that interface, and is registered via entry point; it never patches core.
