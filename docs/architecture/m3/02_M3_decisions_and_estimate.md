# M3 — Decision register D1–D12, scope and estimate

**Status: all decisions APPROVED by the user on 2026-11-04** (simplified design; supersedes the earlier D1–D10 proposal and the 59.5-day estimate).

## 1. Decision register
| ID | Decision | Notes |
|---|---|---|
| D1 | Silver consumes ordered, receipted Bronze `live` batches only | backfill/reprocess deferred (M4) |
| D2 | Closed cleansing allowlist: `trim, upper, lower, initcap, nullif, replace, default, map_values, parse_date, parse_timestamp` | no `expr`, regex, derived columns |
| D3 | Additive schema v3: `cfg_column.target_name`; `cfg_layer_mapping.effective_column, on_cast_error, column_mode`; `wm_layer_cursor.input_versions`; bootstrap `transform_root` | 53 tables unchanged; 0001/0002 frozen; touches M1 schema/validator/bootstrap files additively |
| D4 | `REJECT` default for cast errors (`FAIL` optional); reject rows are pointers `(src_batch_id, _row_hash)`; classified **restricted pseudonymous data**, not a privacy guarantee | HMAC reference = M4 option |
| D5 | SCD2 late arrivals go to the reject table (counted); no skip workflow; operators can only `replan` an uncommitted unit | actor name unauthenticated (as M2) |
| D6 | Gold SQL/PySpark = reviewed, checksum-verified files under `transform_root`; `inline_sql` rejected; typed bound parameters; tokenizer guard is defence in depth | trust boundary documented |
| D7 | Gold FULL (`OVERWRITE`) and KEY_MERGE (`MERGE`, upsert only); CDF designed, deferred (validator rejects) | |
| D8 | **No M1/M2 behaviour change; no M2 file edited.** Only the public M2 API (enforced by test). ≈40+25 lines of retry/runner logic temporarily duplicated; consolidation in M4 (`M4-CONSOLIDATE-01`) | approved governance exception |
| D9 | Minimal dependency-aware runner (levels, parallel within a level, HARD-skip) | planner/DAG = M4 |
| D10 | Basic reject tables and basic DQ (`NOT_NULL, VALID_VALUES, RANGE`; `LOG/QUARANTINE/FAIL_BATCH`); other DQ types rejected for v3 packs only | v2 packs keep loading |
| D11 | MERGE-type recovery = data-idempotent merge + content-level verification (change request to doc 08 / ADR-0009); Append keeps Delta txn idempotency | no commit tags, no session isolation |
| D12 | Deletes from Bronze `_op_type='D'` only; delete-by-absence deferred to M4 (specification in doc 00 §6); candidate-absent count reported only | JDBC source deletes do not reach Silver in M3 |

## 2. Must / Should / Defer
**MUST (M3):** per-layer cursors; Silver planner; cleansing allowlist + casting; business keys, dedupe; rejects + basic DQ; APPEND, SCD1, SCD2; `_op_type` deletes; Gold SQL/PySpark files; FULL + KEY_MERGE; dependency-aware runner; CLI (`transform`, `transform-status`, `transform-replan`); audit/retry/idempotency/crash recovery with content verification; M1/M2 backward compatibility.
**SHOULD (simple form):** Silver `ADD COLUMN` handling and explicit pass-through; Gold input-version recording and no-change skip; per-unit reconciliation counters; 6 core properties + split-equivalence test.
**DEFER (M4+):** delete-by-absence + connector attestation; operator approval workflow, data-skip; HMAC references; Gold CDF; advanced schema evolution; built-ins, `expr`, regex, derived columns; neutral `runtime/` consolidation; automated rebuild; DQ scoring, advanced reconciliation, planner/DAG; erasure automation; streaming; external validation.

## 3. Estimate (provisional): **43 person-days (range 38–48)**
Bottom-up 42.75 (rounded to 43 as the approved basis); development 21.5 / testing 21.25.
| Phase | Content | Days |
|---|---|---|
| P0 | design and documentation baseline | 1.5 |
| P1 | migration + compatibility (1.0), validator rules (1.25), `transform_root`/checksum/path safety (0.75), `LayerCursorRepo` (1.0), hashing + retry wrapper (1.0) | 5.0 |
| P2 | planner (1.75), cleansing/casting (2.0), rejects/dedupe/DQ (2.5), APPEND/SCD1/deletes (2.5), Silver engine incl. verification (3.5), schema/pass-through (1.0) | 13.25 |
| P3 | SCD2 incl. chain verification (3.0), reference model + properties (2.0), absence-candidate report (0.25) | 5.25 |
| P4 | SQL loader + trust controls (2.25), PySpark loader (1.0), FULL/KEY_MERGE (1.5), version recording/no-change (1.0), Gold engine incl. verification (2.5), runner + CLI (2.25) | 10.5 |
| P5 | crash matrix 36 + negatives (3.0), concurrency (1.0), PostgreSQL end-to-end (1.25), docs/closure (2.0) | 7.25 |
Elapsed ≈ 53 working days for one engineer at 80 % productive time. Assumptions: local Spark/Delta/PostgreSQL as in M2; no Databricks/Unity Catalog/Azure DevOps validation; no scope change. Excluded from the estimate: everything under DEFER, external validation, scale benchmark. Estimates are unmeasured (±25 %).
