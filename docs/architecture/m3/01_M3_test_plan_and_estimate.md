# M3 — Test strategy and acceptance criteria

**Status: APPROVED (2026-11-04).** Companion to `00_M3_design.md`; estimate and scope in `02_M3_decisions_and_estimate.md`.
Rule from the M2 closure: **engine tests run on the Delta-backed audit store from the first test.** No test is removed or weakened to reach a pass.

## 1. Test tiers
| Tier | Scope |
|---|---|
| Unit | cleansing ops (one test each + order), casting/`on_cast_error`, dedupe ordering, key checks, hashing (typed, null-safe, timezone-independent), DQ rule types, SQL guard (tokenizer: DML/DDL/`;`/comments/strings), path safety, checksum gate, placeholder/parameter resolution, `LayerCursorRepo` semantics, MDV-7xxx (one negative test per rule), migration 0003 |
| Reference model | pure-Python SCD1/SCD2/delete model + hypothesis: (1) replay of any unit equals applying it once; (2) same logical change delivered in different batches adds no version; (3) tracked hash unchanged with advanced effective date creates no version and updates untracked columns; (4) A→B→A gives three versions; (5) per key: non-overlapping intervals and exactly one current non-deleted version; (6) crash-replay equals no crash. Plus a unit-split equivalence property for streams without cross-batch effective-time inversions, and same-effective-time ordering |
| Component (local Delta, Delta audit store) | Silver APPEND/SCD1/SCD2/deletes, rejects, DQ, schema handling, independent cursors, Gold FULL/KEY_MERGE/version recording/no-change, dependency runner |
| Verification-negative | after the merge the target is deliberately corrupted (row dropped, hash altered, SCD2 chain broken); verification must fail, unit `FAILED`, cursor unmoved |
| Crash matrix | 6 crash points × 6 strategies (Silver APPEND, SCD1, SCD2, delete; Gold FULL, KEY_MERGE) = 36 cases; each asserts final data equals a no-crash run (content comparison), exactly one receipt, cursor advanced once; plus a double-replay of the merge |
| Concurrency | 4-way parallel Silver on Delta audit store; mixed Silver+Gold run; same-object double start (lease) |
| Integration (real PostgreSQL) | PostgreSQL → Bronze → Silver → Gold from metadata only: initial load, then update + new row + changed attribute (SCD2 history) |
| Backward compatibility | v2 pack loads on v3 core; v3 pack refused by a v2 deployment (`check_compat`); `acme_pack` and `sqlserver_demo_pack` still validate; migrations 0001/0002 golden checksums; M3 public-M2-API import test; unknown informational events do not break `EventLog.state`/`open_batch`; all 398 M1/M2 tests unchanged |
| Privacy | canary test: seeded synthetic key values appear in no reject, audit, event or reconciliation payload |
| SQL trust | tampered file, tampered checksum, path traversal, symlink escape, DML/DDL, `;`, injection through a parameter value, undeclared placeholder, `inline_sql` rejected |
| Static | ruff, `mypy --strict`, coverage ≥ 90 % for new code |
Not testable here (stay `DATABRICKS_INTEGRATION_PENDING`): Databricks/Unity Catalog, Key Vault, Azure DevOps, real SQL Server, scale, UC grants for restricted tables.

## 2. Acceptance criteria (M3 closes as `LOCAL_VERIFIED` only when all hold)
1. A metadata-only PostgreSQL → Bronze → Silver → Gold run works with no code change.
2. SCD1, SCD2 and `_op_type` deletes pass the reference-model suite; replay of any unit is a data no-op.
3. Silver and Gold use independent cursors; a forced Silver failure leaves Bronze data and watermark untouched.
4. The 36-case crash matrix passes on the Delta audit store (identical data, one receipt, one cursor advance); verification-negative tests fail the unit as required.
5. Rejects and audit tables hold no keys or payload (canary test) and per-unit counters reconcile.
6. Gold SQL (files only), PySpark (checksum-verified), FULL and KEY_MERGE work; the SQL-trust tests pass.
7. Basic DQ rule types work; other types are rejected for v3 packs while v2 packs and `acme_pack` still load.
8. The public-API import test passes, no M1/M2 behaviour changes, and all 398 existing tests pass unchanged.
9. Documentation, status files and the central overview state `LOCAL_VERIFIED` only and list the limitations; new-code coverage ≥ 90 %; ruff and strict mypy clean.
