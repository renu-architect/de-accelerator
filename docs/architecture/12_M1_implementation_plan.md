# 12 — M1 Implementation Plan: Metadata Repository & Metadata Management Engine

Status: **PLAN — awaiting final approval. No code has been written.** Design basis: doc 02, doc 09 (+ doc 07 changes C1, C2, C5). Phases 2 of the roadmap.

## 1. Objective & scope
Deliver a tested library + CLI that (a) defines the full metadata schema as versioned DDL with migrations, (b) validates Git metadata packs, (c) versions/loads/activates packs into Delta with release history, (d) builds immutable per-run config snapshots, (e) provides typed read access, with unit tests.

**In scope (M1):** package skeleton & minimum `core` needed (errors, logging, naming, bootstrap-YAML loader, plugin-registry stub *interface only*); metadata DDL for **all** tables in doc 02 + Rev 1 additions; migration framework; pack format + JSON Schemas; env-override resolution; validator; loader (stage/verify/activate/rollback); release manager; history differ; snapshot builder/store/verifier; `MetadataReader` (Delta, Snapshot, InMemory); `MetadataService` façade (library only); CLI `validate-config | migrate | build-bundle | load-pack | activate | rollback | snapshot | diff`; unit/component tests; sample synthetic pack; module specs; CI skeleton (lint, type-check, tests).
**Out of scope (M1):** any connector, ingestion, medallion, DQ/recon engines, orchestration, semantic generation, document processing, erasure/retention *execution* logic, Terraform, UI. For tables belonging to those modules M1 delivers **DDL + validator rules only**; their status stays `DDL_ONLY`, not implemented.

## 2. Deliverables
| ID | Deliverable | Acceptance |
|---|---|---|
| M1-D1 | Package skeleton `de_accelerator` (`core`, `metadata`), `pyproject.toml`, tooling | installs; `ruff`, `mypy`, `pytest` run in CI |
| M1-D2 | DDL: versioned table specs (Python spec → generated Delta SQL) for all `cfg_*`, `run_*`, `wm_*`, `dq_*`, `rc_*`, `gov_*`, `doc_*` tables; table properties (`appendOnly` for cfg), UC comments, informational PK/FK | DDL applies on empty catalog; snapshot test of generated SQL; idempotent re-apply |
| M1-D3 | Migration framework + `0001_initial` (up/down), checksums, `cfg_schema_migration` | up→down→up leaves identical schema; checksum drift detected |
| M1-D4 | Pack format, JSON Schemas, parser, deterministic IDs, env-override resolution, bundle builder with hashes | same input ⇒ byte-identical bundle; overrides applied per env |
| M1-D5 | Validator with ≥ 60 coded rules across 6 layers (doc 09 §4) + report formats (text/JSON/JUnit) | rule catalogue each with positive+negative test |
| M1-D6 | Loader: stage → verify → activate → rollback; idempotent re-load; optimistic activation | failure at any step leaves active release unchanged |
| M1-D7 | Release manager + differ → `cfg_config_history` | diff of two releases correct to row level |
| M1-D8 | Snapshot builder/store/verifier (canonical JSON, sha256, append-only) | tamper ⇒ `SnapshotIntegrityError`; same config ⇒ same id |
| M1-D9 | `MetadataReader` ×3 + `MetadataService` façade | reader contract tests pass on all 3 impls |
| M1-D10 | CLI | golden-output tests |
| M1-D11 | Docs: module specs (`docs/architecture/modules/metadata-*.md`), ADR addenda, status/test evidence, release notes `0.1.0` | reviewed |
| M1-D12 | Metadata scale benchmark (500/2,500/5,000 objects) – **measurement only** | results recorded, no pass/fail claim |

## 3. Work breakdown (sequenced; indicative effort for 1–2 engineers, to be re-estimated)
| WP | Tasks | Depends | Est (eng-days) |
|---|---|---|---|
| WP0 Setup | repo scaffold, pyproject, ruff/mypy/pytest, pre-commit, import-linter, CI skeleton, test env (see §6) | – | 3 |
| WP1 Core minimal | error taxonomy, structured logging w/ redaction, naming, bootstrap YAML loader (+schema), registry interface | WP0 | 3 |
| WP2 DDL & migrations | TableSpec model, spec for ~55 tables, SQL generator, migrator, 0001 | WP1 | 7 |
| WP3 Pack model | YAML schemas, parser, ids, overrides, bundle + hashing | WP1 | 5 |
| WP4 Validator | rule engine, 6 layers, report formats, rule catalogue docs | WP3 | 8 |
| WP5 Release/Loader | staging writer, verification, activation pointer, rollback, idempotency, differ/history | WP2, WP3 | 6 |
| WP6 Snapshot | canonical JSON, builder, store, verifier | WP5 | 4 |
| WP7 Reader/Service | 3 readers, typed models, service façade, caching | WP5, WP6 | 5 |
| WP8 CLI & sample pack | CLI, synthetic sample pack, golden tests | WP4–7 | 3 |
| WP9 Hardening | scale benchmark, docs/specs, coverage, review, evidence | all | 4 |
| **Total** | | | **≈ 48 eng-days (≈ 5–6 calendar weeks @ 2 engineers)** — estimate, unvalidated |

## 4. Key design details to lock in M1 (per doc 09)
- Delta tables via `TableSpec` (columns, types, nullability, PK/BK, FK, partitioning/clustering, properties, comment) → SQL generator; one source of truth for DDL, JSON-Schema skeletons of entity docs, and typed dataclasses (generated or verified by test to prevent drift).
- Insert-only `cfg_*` with `pack_release_id`; views `v_cfg_*` filtered by `cfg_active_release`; one pointer table updated conditionally.
- Deterministic surrogate ids (`uuid5`) from business-key paths; sha256 content hashing over canonical JSON.
- Local-first: all logic testable without Databricks; Delta access behind a thin `TableStore` abstraction (Spark/Delta impl; in-memory impl for fast unit tests).

## 5. Test plan (evidence required before any M1 module is `COMPLETE`)
| Level | Coverage | Notes |
|---|---|---|
| Unit (pure Python) | parser, ids, override merge, canonical JSON/hash, every validator rule (positive + negative), differ, release state machine, CLI, errors | target ≥ 90% on these packages |
| Component (local Spark + Delta) | DDL apply/re-apply, migrations up/down, loader stage/activate/rollback, optimistic-activation conflict, appendOnly enforcement, view filtering, snapshot persist/verify, readers on Delta | `delta-spark` local |
| Reader contract | identical behaviour across Delta/Snapshot/InMemory | shared suite |
| Failure injection (metadata scope) | crash at every loader step (post-stage, mid-verify, pre-activate, post-activate) ⇒ active release is old-or-new, never mixed; re-run converges | previews doc 08 harness |
| Property-based | canonical JSON stability; id determinism; override merge associativity rules | hypothesis |
| Security | validator rejects literal secrets/PII-like values; loader principal permissions asserted in DDL grants text; no secrets in logs | |
| Benchmark | scale 500/2,500/5,000 objects | measurement |
| Databricks integration | apply DDL/migrations/load in a real DEV UC workspace | **blocked until a DEV workspace + SP is available**; M1 can reach `IMPLEMENTED`/local-complete, but the Databricks-specific items are tracked separately and not claimed |

**Definition of Done for M1 modules:** code reviewed; tests above green and logged in `TEST_EVIDENCE.md` (command, version, result); module spec written; no TODOs without ticket; limitations recorded; status updated truthfully.

## 6. Environment & tooling decisions
- Runtime target: **DBR LTS with Python 3.11/3.12 and Spark 3.5 or 4.0** (to confirm; pin to client-available DBR). The current sandbox has Python 3.13 and Java but **no PySpark/Delta installed**; local test env will use a pinned venv (Python 3.11/3.12) and `pyspark`+`delta-spark` versions matching the DBR choice. Network access to PyPI is available (via proxy). Databricks workspace is **not** available in this sandbox ⇒ Databricks-side tests need your DEV workspace.
- Tooling: ruff, mypy (strict for `metadata`), pytest, hypothesis, import-linter, pre-commit, `jsonschema`, `PyYAML`/`ruamel.yaml`, `pydantic` v2 (or dataclasses + jsonschema — decide in WP0; recommendation: dataclasses + jsonschema to keep wheel light on cluster).

## 7. Risks & mitigations (M1)
| Risk | Mitigation |
|---|---|
| Spark/Delta local setup/version mismatch vs DBR | pin versions per DBR LTS; CI matrix once decided |
| Over-large schema (≈55 tables) slows start | generate DDL from specs; implement only config-side logic for engines' tables (`DDL_ONLY`) |
| Validator rule explosion | rule catalogue with codes, priority tiers; start with must-have 60 |
| Insert-only model complicates "current" queries | views + reader abstraction; benchmark in D12 |
| Delta feature differences (appendOnly, constraints, CLUSTER BY) between local and Databricks | feature flags in DDL generator; integration tests when workspace available |
| Design drift from approved baseline | any deviation → change request per CLAUDE.md |

## 8. Open items needing your input before M1 starts
1. **Pack file format**: YAML (recommended) vs JSON — confirm YAML.
2. **Target DBR / Spark / Python** versions (recommend DBR LTS with Python 3.11, Spark 3.5 unless client mandates otherwise).
3. **Availability of a Databricks DEV workspace + SP** for integration tests (otherwise integration evidence deferred).
4. **Git platform** (Azure DevOps vs GitHub) for CI skeleton; both templates planned, one delivered in M1.
5. Confirm **change C3 (per-layer cursors)** and **C1 (release-versioned insert-only config)** from doc 07.
6. Naming: package `de_accelerator`, catalogs `<client>_<env>`, implemented schemas `meta/audit/dq/recon/gov/bronze/silver/gold` (a `docs` schema is a planned future capability, ADR-0018).
7. Confirm evidence rule: M1 modules may be marked COMPLETE for local tests only if Databricks items are explicitly listed as pending.

## 9. M1 exit criteria
All M1-D1…D12 accepted; evidence logged; known limitations updated; release `0.1.0` notes; baseline docs unchanged except approved addenda; go/no-go review for M2.
