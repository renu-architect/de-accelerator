# M3 — Decision Register (D1–D10) and Effort Breakdown

**Status: PROPOSED. None of D1–D10 is approved.** Each needs your explicit answer (approve / amend / reject). Design: `00_M3_design.md`; tests: `01_M3_test_plan_and_estimate.md`.
"Streaming" column: does the decision constrain or help a future streaming mode (not implemented in M3).

## 1. Decisions

### D1 — Silver reads only the Bronze `live` stream, in batch order
- **Proposed:** Silver consumes receipted Bronze batches of stream `live`, strictly in `batch_seq` order; backfill/reprocess/resync batches are applied by a separate explicit command (later).
- **Why required:** M2 left open how Bronze batches of different types are consumed (gap G9). Mixing them silently would corrupt SCD2 history.
- **Alternatives:** (a) consume all streams interleaved by ingest time; (b) per-stream cursors from day one.
- **Impact M1/M2:** none on code. Uses M2 receipts and `_batch_seq`/`_stream_key` as already written.
- **Recommendation:** approve (simplest correct rule; (b) can be added without breaking it).
- **Streaming:** helpful — a streaming source would simply be another stream key.

### D2 — Cleansing vocabulary as `cleanse_rules: {ops: [...]}` (declarative, no inline code)
- **Proposed:** ordered ops (`trim`, `upper`, `lower`, `nullif`, `replace`, `regex_replace`, `default`, `map_values`, `parse_date`, `parse_timestamp`, `expr`).
- **Why:** business rules must be configuration, reviewable in Git, and fit the existing M1 JSON-object column (a bare list is rejected by the validator).
- **Alternatives:** free SQL expression per column only (flexible but unreviewable and harder to validate); Python UDF files (slow, harder to test).
- **Impact:** M1 none (field exists). New validator rules only.
- **Recommendation:** approve. `expr` remains the escape hatch.
- **Streaming:** neutral — ops compile to Spark column expressions that work in streaming too.

### D3 — Additive metadata schema v3 (migration 0003)
- **Proposed:** `cfg_column.target_name/expression`; `cfg_layer_mapping.effective_column/on_cast_error/late_arrival_policy/incremental_mode/max_batches_per_unit`; `wm_layer_cursor.input_versions/unit_seq`; bootstrap `transform_root`.
- **Why:** these cannot be expressed today (rename/derived columns, SCD2 ordering, error policy, Gold increments).
- **Alternatives:** stuff them into existing JSON params columns (no schema change, but untyped, unvalidated, hard to query).
- **Impact:** M1: additive migration, 0001/0002 pinned and unchanged; old packs stay valid (all new fields optional with defaults). M2: none.
- **Recommendation:** approve.
- **Streaming:** neutral.

### D4 — Default `on_cast_error = REJECT`
- **Proposed:** a non-null value that fails its cast sends the row to `dq.rej_<object>` with a reason; per-mapping override `NULL` or `FAIL`.
- **Why:** one bad value must not stop a load or silently become NULL.
- **Alternatives:** default `FAIL` (safest, blocks pipelines); default `NULL` (silent data loss).
- **Impact:** M1/M2 none. Creates a minimal reject table that M4 DQ later builds on.
- **Recommendation:** approve; clients wanting strictness set `FAIL`.
- **Streaming:** helpful — side-output of rejects works per micro-batch.

### D5 — SCD2 late arrivals fail by default; no history restatement in M3
- **Proposed:** a version older than the target's newest `_valid_from` for the key → unit fails with a DATA error (`FAIL`, default) or row rejected (`REJECT`). Re-deriving history is out of scope.
- **Why:** restating history is the hardest SCD2 case; handling it wrongly is worse than refusing it.
- **Alternatives:** implement restatement now (+≈5–8 d, high risk); silently ignore late rows.
- **Impact:** none on M1/M2.
- **Recommendation:** approve; revisit when a client needs it.
- **Streaming:** limiting — streaming sources reorder more often; restatement would likely be needed then (explicit future item).

### D6 — Gold transforms: SQL files, PySpark files (checksum-verified), small built-in set; no inline Python
- **Proposed:** SQL with generated placeholders and bound parameters; PySpark `transform(inputs, params, ctx)` files loaded only if SHA-256 equals `cfg_transformation.checksum`; built-ins `select/rename/filter/aggregate/surrogate_key/current_rows`.
- **Why:** meets "reusable PySpark and SQL engine" while keeping runtime code reviewed and tamper-evident.
- **Alternatives:** SQL only (simpler, cannot express complex logic); inline Python in YAML (no review/test discipline).
- **Impact:** M1: uses existing `cfg_transformation` + checksum rules 2010/2011; adds `transform_root` setting. M2 none.
- **Recommendation:** approve.
- **Streaming:** neutral — the same functions apply to a micro-batch DataFrame.

### D7 — Gold increments via Delta change feed; `FULL` is the default
- **Proposed:** `incremental_mode`: `FULL` (default), `KEY_MERGE`, `CDF`. CDF consumes changed rows of the driver input; cursor stores consumed Delta versions.
- **Why:** FULL does not scale to large facts; Delta change feed was proven on MERGE-maintained tables in the spike.
- **Alternatives:** FULL only (smallest, may be enough for MVP); time-based watermark columns (needs a column on every input, misses deletes).
- **Impact:** requires change data feed on Silver tables (M3 creates them so). Bronze/M2 none. Limit: changes in a non-driver input do not re-derive existing rows.
- **Recommendation:** approve the design but **build FULL and KEY_MERGE first; CDF is the first item cut** in the reduced scope (§2).
- **Streaming:** helpful — change feed is the natural bridge to streaming reads.

### D8 — Extract shared M2 helpers instead of duplicating
- **Proposed:** move retry/attempt/result/best-effort code (`ingestion/execution.py`) and Delta commit tag/verify (`processing/common/delta_commit.py`) out of the ingestion engine / Bronze writer; behaviour unchanged.
- **Why:** governance rule "never duplicate existing functionality"; otherwise two diverging copies of the recovery protocol.
- **Alternatives:** copy into M3 (fast, but duplicates the most delicate code); build Silver on top of the ingestion engine class (wrong coupling).
- **Impact:** **touches M2 code** (the only M3 decision that does). Guard: all 398 M1/M2 tests must pass unchanged, done first in M3.0; if any M2 behaviour must change, I stop and ask.
- **Recommendation:** approve. If you prefer zero M2 contact, choose "copy" and accept ≈+2 d and duplication debt.
- **Streaming:** helpful — one shared protocol for all modes.

### D9 — Minimal topological runner now; planner/DAG deferred to M4
- **Proposed:** order by `cfg_pipeline_dependency` + declared inputs in levels; parallel within a level; HARD dependent of failed object is skipped.
- **Why:** Gold needs Silver first; a full planner is a separate concern.
- **Alternatives:** per-layer commands only, ordering left to Workflows (no code, but no skip logic); full planner now (scope creep).
- **Impact:** M1 reads existing dependency table; M2 runner mechanics reused.
- **Recommendation:** approve.
- **Streaming:** neutral.

### D10 — Minimal reject tables now; DQ engine in M4
- **Proposed:** `dq.rej_<object>` (append, idempotent, reason code, source batch) written by Silver only.
- **Why:** D4 needs somewhere to put rejects.
- **Alternatives:** log counts only (rows lost); full DQ engine now (scope creep).
- **Impact:** uses the existing `dq` schema; no M1/M2 change.
- **Recommendation:** approve.
- **Streaming:** helpful.

## 2. Effort estimate — what the numbers are

| Milestone | Days | Essential content |
|---|---|---|
| M3.0 Foundations | 6 | migration 0003 (1), MDV-7xxx rules + tests (2), `transform_root` + checksum loader (0.5), refactor G5 with full M2 regression (2), commit-tag helper + isolated session (0.5) |
| M3.1 Silver core | 12 | planner/units (2), cleansing + casting + rejects (3), key check + dedupe + hashes (1.5), APPEND + SCD1 (2), engine/cursor/lease/recovery (2), CLI + Delta-audit tests (1.5) |
| M3.2 SCD2 + deletes | 8.5 | SCD2 algorithm + late-arrival (3.5), reference model + property tests (2), soft/hard deletes (1), delete-by-absence (2) |
| M3.3 Gold | 9 | SQL loader + placeholders/params (2), PySpark file loader (1), built-ins (2.5), FULL/KEY_MERGE (1), CDF (3 incl. cursor versions) — runner levels folded in (−0.5) |
| M3.4 Hardening | 7 | crash matrix Silver+Gold (3), parallel + PostgreSQL end-to-end (2), docs/status/closure review (2) |
| **Total** | **42.5** | |

The sub-items are my estimates for one engineer, ±25 %; they are not measured.

**Reduced option (≈36 days) removes 6.5 days:** CDF mode (−3), built-ins beyond `select/filter/aggregate` (−1.5), delete-by-absence (−2). Nothing else is cut; the crash matrix and property tests stay.

### Minimum viable M3 (my proposal)
**Essential** — Silver: planner/units, cleansing + casting, business keys + dedupe, APPEND, SCD1, SCD2 (with late-arrival FAIL), soft/hard deletes from `_op_type`, rejects table, independent cursors, recovery/crash matrix, CLI. Gold: SQL + PySpark file transforms, `FULL` and `KEY_MERGE`, dependency-level runner. Foundations (v3 migration, validator rules, G5 refactor). Tests: unit, property (SCD1/2), Delta-audit component, crash matrix, parallel, PostgreSQL end-to-end.

**Optional enhancements (deferrable without breaking the design):** CDF incremental Gold; built-ins beyond three; delete-by-absence; `expr`/`regex_replace`-style extras beyond a core op set; SCD2 history restatement (not planned); streaming (not planned; hooks only).

Because delete-by-absence is what makes FULL/SNAPSHOT sources delete-aware, deferring it means that, until it is built, SQL-source deletes are not reflected in Silver (documented limitation, as in M2).

## 3. Not in M3 (unchanged)
DQ rules/scoring (M4), orchestration planner (M4), reconciliation, semantic layer, documents, streaming, Databricks/Unity Catalog and real SQL Server validation (pending, same as M2), any runtime LLM.
