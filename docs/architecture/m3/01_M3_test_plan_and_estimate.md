# M3 — Test Strategy, Acceptance Criteria, Milestones and Estimates

**Status: PROPOSED — awaiting your approval. No M3 code exists.** Companion to `00_M3_design.md`.

## 1. Test strategy
Lessons from the M2 closure review are built in: **engine tests run on the Delta-backed audit store from the first test**, not in-memory only.

| Tier | Scope | Tooling |
|---|---|---|
| Unit | cleansing ops, casting/`on_cast_error`, dedupe ordering, key checks, surrogate keys, SCD1/SCD2 statement generation, placeholder resolution, checksum gate, MDV-7xxx rules (one negative test each), migration 0003 (golden checksum, 0001/0002 pins unchanged) | pytest on local Spark |
| Property / reference model | SCD1 and SCD2 against a pure-Python reference implementation over random change streams, including replays and re-ordering within allowed bounds; invariant: one current row per key, no overlapping validity intervals, replay = no-op | hypothesis |
| Component (local Delta) | Silver unit end-to-end (APPEND/SCD1/SCD2, soft/hard delete, delete-by-absence), Gold FULL/KEY_MERGE/CDF, rejects table, independent cursors, Silver failure leaves Bronze untouched, HARD-dependent skip | pytest, Delta audit store |
| Crash matrix | the nine crash points of design §6, each for Silver and Gold, re-run to completion with identical final data and exactly one receipt | existing `FaultInjector` |
| Concurrency | 4-way parallel Silver units and mixed Silver/Gold run on Delta; same-object double start (lease); isolated-session tag test | threads, `spark.newSession()` |
| Integration (real PostgreSQL) | PostgreSQL → Bronze → Silver → Gold with a second extract containing updates, a new row and a deleted row; assert SCD2 history and Gold result | existing fixture |
| Regression | all 398 M1/M2 tests must pass unchanged after the behaviour-preserving refactor (G5) | CI |
| Static | ruff, `mypy --strict`, import-linter dependency rule | CI |
| Not testable here | Databricks / Unity Catalog, real SQL Server, scale — remain `DATABRICKS_INTEGRATION_PENDING` | — |

## 2. Acceptance criteria (M3 can close as `LOCAL_VERIFIED` when all hold)
1. AC1 A pack-defined table flows PostgreSQL → Bronze → Silver → Gold with no code changes, using only metadata.
2. AC2 SCD1 and SCD2 pass reference-model property tests; SCD2 replay of any unit is a data no-op.
3. AC3 Silver and Gold use independent cursors; a forced Silver failure does not alter Bronze data/watermark and does not trigger re-extraction.
4. AC4 Crash matrix (9 points × Silver/Gold) passes on the Delta audit store: one receipt, identical data after recovery.
5. AC5 Cast errors and null business keys land in `dq.rej_<object>` according to policy; `FAIL` policy fails the unit with a DATA error.
6. AC6 Deletes: soft, hard and delete-by-absence for complete-extract units.
7. AC7 Gold SQL, PySpark (checksum-verified) and built-in transforms; FULL and CDF incremental modes produce identical results on the same inputs.
8. AC8 Parallel runs (4-way) complete with only absorbed Delta conflicts and no duplicate data.
9. AC9 All M1/M2 tests still pass; ruff and strict mypy clean; coverage of new code ≥ 90 %.
10. AC10 Docs, `IMPLEMENTATION_STATUS`, `KNOWN_LIMITATIONS`, `TEST_EVIDENCE` updated; status stated honestly (no production-readiness claim).

## 3. Milestones and effort (working days, one engineer; estimates, not commitments)
| Milestone | Content | Days |
|---|---|---|
| M3.0 Foundations | schema v3 migration 0003, MDV-7xxx rules, bootstrap `transform_root`, extraction of shared execution/commit helpers (G5) with M2 regression, `delta_commit` helper | 6 |
| M3.1 Silver core | planner, cleansing/casting/key check/rejects, dedupe, APPEND + SCD1, engine, cursor, CLI, Delta-audit tests | 12 |
| M3.2 SCD2 + deletes | SCD2 algorithm, late-arrival policy, soft/hard/absence deletes, property tests | 8.5 |
| M3.3 Gold | SQL/PySpark/built-ins, FULL/KEY_MERGE/CDF, dependency-level runner | 9 |
| M3.4 Hardening | crash matrix, parallel, PostgreSQL end-to-end, docs/status, closure review | 7 |
| **Total** | | **42.5** |
MVP cut (defer CDF mode, built-ins beyond `select/filter/aggregate`, delete-by-absence): ≈ 36 days.

## 4. Risks
| Risk | Mitigation |
|---|---|
| SCD2 correctness subtleties | reference model + property tests before engine work |
| MERGE is not de-duplicated by Delta on replay | tag lookup first + data-level idempotency (spike S1) |
| Refactor of M2 engine regresses ingestion | behaviour-preserving, 398 tests as net, done first |
| Gold CDF limit (non-driver input changes) | documented; `FULL` available; validator warns |
| Delta behaviour differs on Databricks runtime | stays `DATABRICKS_INTEGRATION_PENDING`; re-verify spike on runtime |
| Commit conflict volume at scale | bounded retry exists; scale untested |

## 5. Decisions requested
Full register with rationale, alternatives, M1/M2 impact, recommendation and streaming effect: `02_M3_decisions_and_estimate.md`. **None is approved yet.** The same file explains the 42.5-day and ≈36-day estimates and defines the minimum viable scope.
