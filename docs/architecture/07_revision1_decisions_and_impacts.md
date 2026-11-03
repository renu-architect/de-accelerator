# 07 — Revision 1: Approved Decisions, Design Changes, Impact Analysis

Baseline: Phase 1 architecture (docs 00–06) **direction approved** by the user. Revision 1 incorporates the user's 10 refinements. Rev 1 is **pending final approval**; once approved it becomes **Baseline v1.0** and any change needs a change request (impact analysis + approval).

## 1. Decision log

| # | Decision (user-approved) | Realised in | ADR |
|---|---|---|---|
| D1 | Git metadata packs are the config source of truth; CI validates; approved packs load to Delta; immutable versioned snapshot per execution; runtime state separate | doc 02 §Rev1, doc 09 (M1) | 0002, 0007 |
| D2 | One core repo + one deployment repo per client; clients never modify core; versioned releases + backward compatibility | doc 03, doc 05 §17 | 0008 |
| D3 | Default independent DEV/QA/PROD workspaces; configurable topologies; Terraform; Workflows for MVP; extensible orchestration provider interface | doc 10 | 0010, 0011 |
| D4 | Durable batch manifests, idempotent target writes, crash-safe watermark commit, crash-injection tests for every state | doc 08 | 0009 |
| D5 | GDPR/deletion propagation, retention, deletion audit, backup considerations; UC, KV, MI, RBAC | doc 11 | 0012 |
| D6 | Structured, semi-structured, unstructured all in architecture; Azure AI Document Intelligence default OCR behind replaceable interface; advanced docs/RAG later | doc 10 §4 | 0014 |
| D7 | Semantic: Databricks SQL, metric views, Power BI TMDL, with real Power BI deployment + validation tests | doc 10 §3 | 0015 |
| D8 | Metadata backup, Delta recovery, infra recreation, RPO/RTO, DR testing | doc 11 §4–6 | 0013 |
| D9 | Sizing: 500–5,000 objects, ≤10 TB per client = **design targets requiring perf/scalability testing, not guaranteed limits** | doc 10 §5 | 0017 |
| D10 | Metadata UI outside MVP; Git + approval workflows; UI-ready interfaces | doc 10 §6 | 0016 |

Resolved questions from doc 00: Q1 ✔ packs; Q2 ✔ repo model; Q4 ✔ workspace per env (configurable); Q5 ✔ Workflows + provider interface; Q9 ✔ Azure DI; Q11 ✔ SQL + metric views + TMDL; Q15 ✔ sizing as design targets. Remaining defaults in doc 00 stand unless you object.

## 2. Design changes versus Phase 1 (explicit impact)

| Change | What changed | Impact on approved docs | Risk / note |
|---|---|---|---|
| C1 Release-versioned, insert-only config | Doc 02 described SCD2 in-place versioning (`effective_to`, `is_active` flips). Replaced by: every `cfg_*` row carries `pack_release_id`; rows are **insert-only**; "current config" = rows of the **active release** (pointer table + views). `effective_from/to` are derived from release activation history. `config_version` stays as a per-row informational version. | doc 02 "Common columns/Versioning" superseded by doc 02 §Rev1 | Makes atomic multi-table activation possible (one pointer flip) and snapshots trivial. Slightly more storage; negligible. |
| C2 Env overrides resolved at load time | Closes gap B1 (env overrides). Packs have base + `overrides/<env>.yaml`; the loader materialises resolved rows per environment. Runtime has no override logic. | doc 06 gap B1 closed | Overrides validated in CI for all envs. |
| C3 Per-layer cursors | Doc 04 advanced one watermark after "target commit". Now: **source watermark** advances after the **Bronze commit is confirmed**; Silver/Gold progress via **layer cursors** (`wm_layer_cursor`, last applied `batch_seq`) each advanced only after that layer's confirmed commit. | doc 04 flows 11 & doc 01 data-flow wording amended; ADR-0004 superseded in part by ADR-0009 | Decouples source extraction from downstream failures (a Silver failure doesn't force re-extract). Needs approval – it changes ADR-0004's single-FSM wording. |
| C4 Durable manifest + deterministic windows | Batch window (`wm_from`, `wm_to`, file list) is fixed and persisted **before** extraction; replays read the identical window. | doc 04 | Replay reads the same window but source data may have changed – covered by idempotent merge & recon. |
| C5 Added tables | `cfg_pack_release`, `cfg_active_release`, `run_config_snapshot`, `run_batch_manifest`, `run_batch_event`, `run_target_receipt`, `wm_layer_cursor`, `cfg_retention_policy`, `cfg_subject_key_map`, `gov_erasure_request/task/evidence`, `gov_suppression_list`, `gov_retention_run`, `cfg_doc_*`, `cfg_orchestration_provider` | doc 02 addendum | DDL only in M1 where noted in doc 12 |
| C6 Roadmap | TMDL moves into MVP scope (M5) with deployment tests; Terraform in MVP; crash-injection harness mandatory in M2; DR & erasure tests added | doc 06 §E superseded by §Rev1 roadmap | Power BI tests need a Fabric/Power BI capacity + tenant settings (dependency) |

## 3. Documents updated / added in Rev 1
Updated (banner + targeted edits): `CLAUDE.md`, 00, 01 (provider interface), 02 (§Rev1), 04 (recovery pointers), 05, 06 (gaps, roadmap), ADRs, status files.
Added: 07 (this), 08 recovery & crash testing, 09 metadata engine detailed design, 10 deployment/semantic/documents/sizing/UI, 11 governance: erasure, retention, DR, 12 M1 implementation plan.

> Numbering note: file names are `07…12` under `docs/architecture/` — see the index in CLAUDE.md.
