# Module spec — Metadata repository & management engine (M1)

Package: `de_accelerator.metadata` (+ `core`, `cli`). Design basis: docs 02, 09, 13; ADR-0002, 0007, 0008, 0016, 0018.
Status: see `docs/status/IMPLEMENTATION_STATUS.md` (implemented; **LOCAL_VERIFIED**, **DATABRICKS_INTEGRATION_PENDING**).

## 1. Responsibilities
Define the metadata schema (DDL + migrations), turn Git YAML packs into validated, hash-identified per-environment bundles, load them into Delta as immutable releases, manage the active-release pointer (activate / rollback / healing), keep an audit history, build immutable per-run configuration snapshots, and serve typed read access. It does **not** execute pipelines.

## 2. Components and interfaces
| Package | Key types | Notes |
|---|---|---|
| `schema/entities.py` | `EntityDef`, `Field`, `ENTITIES` (24) | Single source of truth → pack JSON-Schemas, `cfg_*` DDL, typed models |
| `schema/tablespec.py`, `tables.py` | `TableSpec`, `render_create`, `all_tables()` (53 tables) | 22 runtime/governance tables are `DDL_ONLY`; cfg tables are `delta.appendOnly` |
| `store/` | `TableStore` protocol; `InMemoryTableStore`, `SparkDeltaTableStore`, `build_local_spark` | Same contract tests on both; `insert_if_absent`, atomic `update_where` |
| `migrations/` | `Migrator`, `Migration`, `MIGRATIONS` | Ordered, idempotent, checksummed (pinned golden checksum), dry-run, guarded rollback |
| `packs/` | `parse_pack`, `build_bundle`, `PackBundle`, `entity_id`, `canonical_json` | Env overrides resolved here only; deterministic ids/hashes |
| `validation/` | `validate_pack`, `validate_bundle`, `ValidationReport`, 60 rules | Codes `MDV-nnnn`; catalogue in `validation-rules.md` |
| `release/` | `PackLoader`, `ReleaseManager`, `ChangeDiffer` | Stage→verify→activate; rollback; optimistic concurrency; crash healing |
| `snapshot/` | `SnapshotBuilder`, `SnapshotStore`, `Snapshot`, `SnapshotScope` | Content-hash id (includes release id); gzip; append-only; verified on load |
| `reader/` | `MetadataReader` + `Delta/Snapshot/InMemory` readers, generated models | One contract suite across the three |
| `service.py` | `MetadataService` | Read-only, UI-ready façade (no write methods) |
| `cli/main.py` | `de-accel` | validate-config, build-bundle, migrate, load-pack, activate, rollback, releases, status, snapshot, diff |

## 3. Data flow
`pack dir (Git)` → `parse` (JSON-Schema per file/item) → `apply_overrides(env)` → ids + `row_hash` → `PackBundle` (sha256) → `validate` (60 rules; any ERROR blocks) → `PackLoader.stage` (insert-only rows tagged `pack_release_id`, verified row-by-row by hash, status `STAGED`) → `ReleaseManager.activate` (single conditional pointer update on `activation_seq`) → `reconcile` (statuses + `cfg_config_history`) → `SnapshotBuilder.build(scope)` → `SnapshotStore.persist` → engines read `SnapshotMetadataReader`.

## 4. Invariants (all tested)
1. cfg rows are never updated/deleted (append-only; enforced by in-memory store and by Delta `appendOnly`).
2. A release becomes visible only via the active pointer; a half-staged release is invisible and cannot be activated (`STAGING`).
3. The pointer flip is the only commit point; everything after is idempotent and re-derived by `reconcile()` (crash sweep over every write operation of stage, activate, rollback).
4. Exactly one release is ACTIVE per env; concurrent activations → exactly one winner, the rest `ActivationConflictError`.
5. Same pack content ⇒ same bundle bytes/hash ⇒ same release id (`uuid5(env, bundle_sha256)`); reloading is a no-op.
6. Snapshot id = sha256(canonical content incl. release id); altered/corrupt stored content raises `SnapshotIntegrityError`.
7. No environment-override code outside `packs/` (static test); secrets only as `kv:<name>` (rules 4001–4003) and never echoed in reports (test).
8. A release is activatable only if `requires_core` is satisfied by the running framework and `metadata_schema_version` ≤ deployed schema.

## 5. Failure handling
| Failure | Behaviour |
|---|---|
| Invalid pack | Nothing loaded; CLI exit 1; report with codes |
| Crash during stage | Release stays `STAGING`; re-run completes missing rows (insert-if-absent) and verifies |
| Crash between pointer flip and status/history | `reconcile()` (run at start of every mutating call) repairs |
| Verification mismatch | Release marked `FAILED`, event logged, error raised; pointer untouched |
| Concurrent activation | Loser: `ActivationConflictError` (CLI exit 2); no state change |
| Snapshot tamper/corruption | `SnapshotIntegrityError` on load |
| Incompatible core/schema | `CompatibilityError` at activate/snapshot; `MDV-5xxx` at validate/stage |

## 6. Security
Secrets by reference only; secret-like literals and personal-identifier patterns (SSN/Aadhaar/PAN/card-like) are rejected in metadata (org policy); logging redacts secret patterns. Intended Unity Catalog grants (to be applied by IaC in M6): loader principal INSERT on `cfg_*` + UPDATE on `cfg_pack_release`/`cfg_active_release`; engine principal SELECT only; humans SELECT. **Not yet verified on UC** (`DATABRICKS_INTEGRATION_PENDING`).

## 7. Reuse across clients
Nothing client-specific in code; client identity enters only via pack manifest/bootstrap; `<client>_<env>` catalog derived by naming helper.

## 8. Test map
`tests/unit`: core, schema, store contract (memory), migrations, packs, validator (60 rule tests), release (incl. crash sweeps, concurrency), snapshot, reader contract, CLI. `tests/component` (local Delta): store contract, migrations, full release lifecycle, snapshot/readers/CLI on Delta.
Commands: `scripts/ci_test.sh` (all, with lint/type) · `pytest -m "not component"` (fast, ~30 s) · `pytest -m component` (local Spark/Delta, several minutes).
