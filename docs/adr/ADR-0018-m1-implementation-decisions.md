# ADR-0018 — M1 implementation-level decisions and minor deviations from Rev 1 (status: IMPLEMENTED, awaiting your acknowledgement)

These are decisions taken while implementing M1 *within* the approved baseline. None changes an approved ADR; items marked ⚠ refine wording in docs 02/09 and need your acknowledgement.

| # | Decision | Reason |
|---|---|---|
| 1 | Entity definitions (`schema/entities.py`) are the single source for pack JSON-Schemas, `cfg_*` DDL and typed models | Prevents drift between pack format, tables and readers |
| 2 | References between entities are by **code within the project**; surrogate ids are deterministic `uuid5(client|kind|project|key)` | Stable diffs/reloads; ids equal across environments |
| 3 | Release id = `uuid5(env, bundle_sha256)`; same bundle ⇒ same release (idempotent load) | Concurrency-safe, retry-safe loading |
| 4 | ⚠ Snapshot id = sha256 of canonical content **including the release id** | Guarantees every execution retains its release id; identical config in different releases gives different snapshots (no cross-release dedupe) |
| 5 | ⚠ `cfg_config_history` primary key includes `activation_seq` (new column) | A rollback re-activates an earlier release; without the sequence the rollback's changes collided with the first activation's rows (found by a test) |
| 6 | ⚠ Runtime/doc tables `doc_registry/page/extraction/classification` are **not** in schema v1 | The approved schema list (meta, audit, dq, recon, gov, bronze, silver, gold) has no docs schema; placement to be decided with the document module (V1) |
| 7 | `cfg_environment` rows are per project and per environment; a bundle contains only the target env's row | Env resolved at load; no runtime override logic |
| 8 | The pointer table is the single source of truth for release status; `reconcile()` derives/heals statuses and history | Crash safety with a single commit point |
| 9 | Loader re-validates every bundle (incl. deployed-schema compatibility) before staging | A bundle cannot bypass CI validation |
| 10 | CDC retention/recovery safeguards are **metadata + validator only** in M1 (`cdc_retention_check`, `max_replay_age`, `on_retention_gap`; rules 3026/3027) | Behaviour and crash-injection validation belong to M2 (not claimed) |
| 11 | Validation severities are configurable per client via options (`severity_overrides`, `disabled_rules`) but defaults are fixed in code | Policy flexibility without forking core |
| 12 | Table store counts for `insert_if_absent` on Delta are advisory; correctness never depends on them (first activation re-reads the pointer) | Delta merge metrics are not race-free |
| 13 | Runtime versions live in `config/runtime_matrix.yaml` (not hardcoded); profile `dbr_lts_py311_spark35` is **unverified** against a specific Databricks LTS | Per your instruction to validate before finalising |
