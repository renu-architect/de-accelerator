# 13 — M1 Revised Task Breakdown (supersedes the estimate table in doc 12 §3)

Approved by user: C1, C3, YAML packs, load-time env overrides, Azure DevOps, naming, local-first with Databricks integration **pending**. Implementation order is strictly one component at a time: **build → test → document → update `IMPLEMENTATION_STATUS.md`**. Ingestion work does not start until M1 acceptance tests pass.

## 1. Status vocabulary (replaces `COMPLETE`)
| Status | Meaning |
|---|---|
| `LOCAL_VERIFIED` | Code reviewed, unit + component tests green locally (Python 3.11 / Spark 3.5 / local Delta), evidence in `TEST_EVIDENCE.md`. |
| `DATABRICKS_INTEGRATION_PENDING` | Flag carried by every module until its integration tests pass on a real Databricks workspace (UC, secrets, grants, Workflows). |
| `PRODUCTION_READY` | `LOCAL_VERIFIED` + Databricks integration passed + security review + performance/DR evidence where applicable + user approval. **Never granted on local tests alone.** |
Earlier states (`DESIGNED`, `DDL_ONLY`, `IN_PROGRESS`, `IMPLEMENTED`) remain.

## 2. Effort breakdown (48 engineer-days; estimates, unvalidated)
Complexity: S ≤1.5d · M 2–3d · L 4–6d · XL >6d. Scope: **MVP** = required for M1 acceptance; **OPT** = optional enhancement, done only if MVP is accepted.

| WP | Module / deliverable | Cx | MVP d | OPT d | Depends on |
|---|---|---|---|---|---|
| WP0 | Project scaffold, `pyproject`, ruff/mypy/pytest config, local env, runtime-compat matrix file (no hardcoded versions), Azure DevOps pipeline YAML (portable steps in scripts) | M | 3.0 | – | – |
| WP1 | `core` minimal: error taxonomy + stable codes, structured logging + secret redaction, naming helpers (`<client>_<env>`, schemas), bootstrap-YAML loader+schema, plugin-registry interface | M | 2.5 | – | WP0 |
| WP2a | `TableSpec` model + Delta DDL generator (feature flags for local vs Databricks) | M | 2.0 | – | WP1 |
| WP2b | Table specs: all `cfg_*`, release/snapshot, plus runtime/gov/doc tables as `DDL_ONLY` | L | 3.0 | – | WP2a |
| WP2c | Migration framework: ordered up/down, checksums, `cfg_schema_migration`, dry-run, `0001_initial` | M | 2.0 | – | WP2a |
| WP3a | YAML pack format + JSON Schemas (versioned) | M | 2.0 | – | WP1 |
| WP3b | Pack parser, deterministic IDs (uuid5), canonical JSON | M | 1.5 | – | WP3a |
| WP3c | Env override resolution at load time (DEV/QA/PROD), `kv:` secret-reference handling | M | 1.0 | – | WP3b |
| WP3d | Bundle builder: manifest, per-file sha256, bundle hash, core-compat range | M | 1.5 | – | WP3b |
| WP4a | Validator engine + `ValidationReport` (text/JSON) + rule registry with codes `MDV-nnnn` | M | 2.0 | – | WP3b |
| WP4b | Syntax/schema + referential rules | M | 1.5 | – | WP4a |
| WP4c | Semantic rules (load strategy, SCD, DAG acyclic, DQ/recon params, retention/subject-key rules) | L | 2.5 | – | WP4a |
| WP4d | Policy + **compatibility** rules (`requires_core`, schema version, deprecated keys, secret-literal detection) | M | 1.0 | – | WP4a |
| WP4e | Extended rules up to ≥60 + JUnit XML report | M | – | 1.5 | WP4b–d |
| WP5a | `TableStore` abstraction (in-memory + Spark/Delta impls) | M | 2.0 | – | WP2 |
| WP5b | Loader: stage → verify → activate; idempotent re-load by bundle hash | L | 2.0 | – | WP5a, WP3d, WP4 |
| WP5c | **Release manager**: states, active pointer, **optimistic concurrency** (activation_seq), **rollback**, compat gate | L | 2.0 | – | WP5b |
| WP5d | **Audit history**: `ChangeDiffer` → `cfg_config_history`, release audit events | M | 1.0 | – | WP5c |
| WP6 | **Immutable snapshots**: canonical resolve, content-hash id, append-only store, integrity verify, release-id retention on `run_config_snapshot` | L | 3.5 | – | WP5c |
| WP7a | Typed config models + `MetadataReader` ×3 (Delta, Snapshot, InMemory) with shared contract tests | L | 4.0 | – | WP5, WP6 |
| WP7b | `MetadataService` façade (validate/diff/list/get; `propose_change` stub that only writes to Git is OPT) | M | 1.0 | 1.0 | WP7a |
| WP8 | CLI (`validate-config, migrate, build-bundle, load-pack, activate, rollback, snapshot, diff`) + sample synthetic pack | M | 2.5 | – | WP4–7 |
| WP9 | Docs/module specs, ADR addenda, evidence logging, review, coverage gate | M | 2.0 | – | all |
| WP9-opt | Scale benchmark 500/2,500/5,000 objects (measurement), extra property tests | M | – | 2.0 | WP7 |
| **Total** | | | **43.5** | **4.5** | **48.0** |

## 3. Minimum viable M1 vs optional
**MVP (43.5 d):** everything marked MVP above, i.e. the priority list you gave — DDL & migrations; YAML pack schema; validation; release management; loader; immutable snapshots; reader APIs; unit & component tests; CLI & library interfaces; including rollback, concurrency handling, compatibility validation, audit history.
**Optional (4.5 d):** validator rules beyond the core set + JUnit output; `propose_change` (PR creation) in `MetadataService`; scale benchmark; extra property-based tests.

## 4. Coverage of your added requirements
| Requirement | Where |
|---|---|
| Rollback via active-release pointer | WP5c (+ crash tests on every loader step) |
| Concurrency handling | WP5c optimistic `activation_seq`; concurrent-loader tests; same-bundle idempotency |
| Config compatibility validation | WP4d + WP5c gate (`requires_core`, `metadata_schema_version`); runtime matrix not hardcoded (WP0) |
| Audit history | WP5d (`cfg_config_history`, release events, actor/ticket) |
| Execution keeps release ID + snapshot | WP6 (`run_config_snapshot.release_id`, `snapshot_id`) |
| Env overrides at load only | WP3c; reader/runtime has no override code (asserted by test) |
| Secrets only as Key Vault refs | WP3c + WP4d (rejects literals) |
| Per-layer cursors, CDC retention/recovery safeguards, effectively-once validation | **DDL + validator rules in M1** (`wm_layer_cursor`, CDC retention fields `cdc_min_retention_check`, `max_replay_age`, `on_retention_gap` in `cfg_load_strategy`); **behaviour and crash-injection validation belong to M2** — not claimed in M1 |

## 5. Execution order and gates
WP0 → WP1 → WP2 → WP3 → WP4 → WP5 → WP6 → WP7 → WP8 → WP9. After each WP: tests green → module spec → evidence entry → status update. Stop and ask on any deviation from baseline.
**M1 acceptance** = all MVP deliverables `LOCAL_VERIFIED` with evidence, `DATABRICKS_INTEGRATION_PENDING` listed per module, no baseline deviations unapproved.
