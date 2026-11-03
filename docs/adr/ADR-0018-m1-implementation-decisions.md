# ADR-0018 — M1 implementation-level decisions and minor deviations from Rev 1

**Status: ACCEPTED. M1 closed as LOCAL_VERIFIED. Decisions 4 and 5 are implemented. Decision 6 (dedicated `docs` schema) is approved as a FUTURE capability and is deliberately NOT implemented.**

> **Revision note (M1 closure):** the original wording of decision 6 and the sections below said the `docs` schema code was a pending change request (CR-M1-01). The user then decided that the implemented M1 state stays as is — **8 schemas, 53 tables, 22 schema-only** — and that the `docs` schema (4 extra tables) is a future capability to be built with the document-processing milestone. CR-M1-01 is therefore withdrawn as an M1 item and re-filed as a future work item.

These are decisions taken while implementing M1 *within* the approved baseline. None changes an approved ADR; items marked ⚠ refine wording in docs 02/09 and were approved by the user (see §Approval record).

| # | Decision | Reason |
|---|---|---|
| 1 | Entity definitions (`schema/entities.py`) are the single source for pack JSON-Schemas, `cfg_*` DDL and typed models | Prevents drift between pack format, tables and readers |
| 2 | References between entities are by **code within the project**; surrogate ids are deterministic `uuid5(client|kind|project|key)` | Stable diffs/reloads; ids equal across environments |
| 3 | Release id = `uuid5(env, bundle_sha256)`; same bundle ⇒ same release (idempotent load) | Concurrency-safe, retry-safe loading |
| 4 | ⚠ Snapshot id = sha256 of canonical content **including the release id** | Guarantees every execution retains its release id; identical config in different releases gives different snapshots (no cross-release dedupe) |
| 5 | ⚠ `cfg_config_history` primary key includes `activation_seq` (new column) | A rollback re-activates an earlier release; without the sequence the rollback's changes collided with the first activation's rows (found by a test) |
| 6 | ⚠ **FUTURE (approved design, not implemented):** document runtime tables will live in a **dedicated `docs` schema** (`docs.doc_registry`, `docs.doc_page`, `docs.doc_extraction`, `docs.doc_classification`). Original documents will be stored in **ADLS Gen2** (raw, immutable, content-hash paths); document metadata and processing state in **Delta**. The configuration entity `cfg_doc_pipeline` is already implemented in `meta`. | Documents need their own governance/ACL boundary and lifecycle. Deferred to the document-processing milestone (V1) so M1/M2 stay focused. |
| 7 | `cfg_environment` rows are per project and per environment; a bundle contains only the target env's row | Env resolved at load; no runtime override logic |
| 8 | The pointer table is the single source of truth for release status; `reconcile()` derives/heals statuses and history | Crash safety with a single commit point |
| 9 | Loader re-validates every bundle (incl. deployed-schema compatibility) before staging | A bundle cannot bypass CI validation |
| 10 | CDC retention/recovery safeguards are **metadata + validator only** in M1 (`cdc_retention_check`, `max_replay_age`, `on_retention_gap`; rules 3026/3027) | Behaviour and crash-injection validation belong to M2 (not claimed) |
| 11 | Validation severities are configurable per client via options (`severity_overrides`, `disabled_rules`) but defaults are fixed in code | Policy flexibility without forking core |
| 12 | Table store counts for `insert_if_absent` on Delta are advisory; correctness never depends on them (first activation re-reads the pointer) | Delta merge metrics are not race-free |
| 13 | Runtime versions live in `config/runtime_matrix.yaml` (not hardcoded); profile `dbr_lts_py311_spark35` is **unverified** against a specific Databricks LTS | Per your instruction to validate before finalising |

## Approval record
| Decision | Approved | Note |
|---|---|---|
| 4 Snapshot id includes release id | Yes | Implemented and tested |
| 5 History key includes `activation_seq` | Yes | Implemented and tested (bug found by a rollback test) |
| 6 Dedicated `docs` schema; originals in ADLS Gen2, metadata/state in Delta | Yes (as a future capability) | **Not implemented by decision**; code stays at 8 schemas / 53 tables |

## Catalog schema structure
- **Implemented today (8 schemas):** `meta, audit, dq, recon, gov, bronze, silver, gold` under catalog `<client>_<env>`.
- **Planned (future, not implemented):** `docs` (document runtime tables). Layout after that milestone: `meta, audit, dq, recon, gov, docs, bronze, silver, gold`.

## Implemented vs schema-only vs future (as of this ADR)
| Item | State |
|---|---|
| `meta` schema: 24 `cfg_*` tables, release/pointer/event/history/migration/framework-version tables, `run_config_snapshot` (31 tables) | **Implemented and LOCAL_VERIFIED** (logic + tests) |
| `audit`, `dq`, `recon`, `gov` tables (22) | **Schema-only (`DDL_ONLY`)** — DDL exists and is tested only as DDL; no runtime logic |
| `docs` schema and its four runtime tables | **Planned / future capability — not implemented** (code has 8 schemas, 53 tables; no `docs.*` tables) |
| `bronze`, `silver`, `gold` schemas | **Approved naming only**; data tables are created per object by later milestones (M2/M3) |
| Document processing behaviour (landing, Azure DI extraction, classification, embeddings) | **Future** (V1/V2) |

## Future work item (withdrawn from M1)
When the document-processing milestone starts: add `docs` to `core/naming.SCHEMAS`, add the four `docs.*` tables (initially schema-only), add migration `0002` (schema v1 is already released as part of M1, so use an additive migration rather than editing `0001`), update the `schema/tables.py` module docstring, and extend tests. Not scheduled; no change to M1 or M2 code.
