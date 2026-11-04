# Module spec — transformation engine (M3: Bronze → Silver → Gold)

**Status: implemented and `LOCAL_VERIFIED` on branch `m3-design` (evidence: `docs/status/TEST_EVIDENCE.md`); Databricks/Unity Catalog validation pending.** Design: `docs/architecture/m3/00_M3_design.md`; decisions: `02_M3_decisions_and_estimate.md`; ADR-0020.

## Public surface
| Module | Responsibility |
|---|---|
| `core/transform_spec.py` | closed allowlists shared by validator and engines: cleansing ops, parameter types, DQ rule types, target types, order-term parser |
| `metadata/validation/rules_transform.py` | MDV-7xxx rules (active for packs with `metadata_schema_version >= 3`) |
| `audit/layer_cursor.py` | `LayerCursorRepo` — independent Silver/Gold cursors on `wm_layer_cursor` (conditional advance, idempotent) |
| `processing/common/` | `hashing` (SHA-256 content hash, 128-bit ids), `cleansing`, `columns` (typed `SilverPlan`), `rejects` (`dq.rej_<object>`), `names`, `errors` (ACC-7xxx), `unit` (retry wrapper + audit; temporary copy of M2 semantics, `M4-CONSOLIDATE-01`) |
| `processing/silver/` | `planner` (ordered receipted Bronze `live` batches), `builder` (map/cleanse/cast/key-check/DQ/dedupe), `strategies` (APPEND, SCD1, table management, content verification), `scd2`, `engine` |
| `processing/gold/`, `processing/runner.py`, `cli/transform.py` | Gold SQL/PySpark execution, dependency-aware runner, CLI (P4) |

## Contracts
* **Input**: Bronze tables written by M2 (`_batch_id/_batch_seq/_stream_key/_op_type/_op_seq/_row_hash/_ingest_ts`), Bronze receipts and manifests (`plan_json.mode`). M3 uses only the public M2 API (enforced by `tests/unit/test_m3_public_api.py`); no M2 file is modified.
* **Silver table**: declared columns + `_content_hash, _effective_ts, _src_batch_seq, _src_batch_id, _silver_batch_id, _silver_ts, _config_snapshot_id, _is_deleted, _deleted_ts`; SCD2 adds `_sk, _scd_hash, _valid_from, _valid_to, _is_current`.
* **Audit**: manifest stream `SILVER:live`; events `PLANNED`, `TARGET_COMMITTED` (counters), `CURSOR_ADVANCED`, `DONE`, `FAILED_*`, `ABANDONED`; receipt layer `SILVER`; `run_object`/`run_lineage` rows carry the real layer.
* **Recovery**: data-idempotent merge + content-level verification before the receipt; cursor advances only after the receipt (conditional update). Crash points `U_CP1..U_CP6`.
* **Rejects**: pointers `(src_batch_id, _row_hash, reason, column_name)` only — restricted pseudonymous data, not a privacy guarantee.

## Limits (see KNOWN_LIMITATIONS.md)
No delete-by-absence, no CDF, no data-skip workflow, unauthenticated `--actor`, ADD COLUMN-only schema evolution, local Delta evidence only.
