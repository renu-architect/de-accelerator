# M2 — Implementation Breakdown, Dependencies, Test Strategy, Estimate and Milestones

Estimates are engineer-days for one engineer including tests and module specs; **unvalidated** (M1 plan was 48 d; use the same calibration discipline). Complexity: S ≤1.5 d · M 2–3 d · L 4–6 d · XL >6 d.

## 1. Work packages
| WP | Deliverable | Cx | Days | Depends on | Scope |
|---|---|---|---|---|---|
| **A1** | **CR-M2-01 (additive, M1 touch):** migration `0002` (`read_options`, `cdc_op_map`, audit-table columns, file-tracking key), entity update, validator `ConnectorValidator` hook + rules MDV-6001…6003; M1 tests extended, checksum re-pinned | M | 4 | approval D1 | MVP |
| A2 | `core/secrets.py` (SecretProvider, Databricks + env impls), connector registry/entry points, `ConnectorContext`, `Capabilities`, `Clock`, error mapping helpers | M | 3 | – | MVP |
| A3 | Audit repositories: manifest, events, receipts, lease, watermark (on `TableStore`), state fold, conditional commits | L | 6 | A1 | MVP |
| A4 | Bronze writer: audit columns, schema policy (FAIL/ADD/WIDEN/RESCUE), table creation, txn idempotent write, `Observation` counts, receipt verification, `run_schema_snapshot` | L | 6 | A3 | MVP |
| A5 | `IngestionEngine` + strategies (FULL, INCREMENTAL, SNAPSHOT), planner, recovery, retry/backoff, `run_attempt/run_error/run_object/run_lineage` logging | XL | 7 | A2, A3, A4 | MVP |
| A6 | `MockConnector`, `core/faults.py` injector, Bronze crash-matrix harness (CP01–CP15 × load types) | L | 6 | A5 (parallel with A5 tail) | MVP |
| A7 | `IngestionRunner` (thread pool, per-source caps), CLI (`ingest, batch-status, abandon-batch, rewind-watermark, reprocess`), Workflows job template + docs | M | 3 | A5 | MVP |
| B1 | JDBC base: query/dialect framework, partitioned reads, bounds, type mapping, counts | M | 4 | A2, A5 | MVP |
| B2 | SQL Server + Azure SQL: auth (secret / UC service credential / SP), `rowversion` safe bound, timestamp bound | M | 3 | B1 | MVP |
| B3 | PostgreSQL dialect | S | 2 | B1 | MVP |
| C1 | File connector: listing, tracking, selection, frozen list, `max_files_per_batch` | M | 3 | A5 | MVP |
| C2 | CSV / JSON / Parquet readers, schema modes, corrupt handling, rescue | M | 3 | C1 | MVP |
| B4 | CDC: generic change feed + deletes + `cdc_op_map` + retention gap policies (FAIL/FULL_RESYNC/ALERT) | L | 4 | B1, A5 | MVP |
| B5 | SQL Server native CDC (LSN range, min-LSN check, op mapping) | M | 2 | B4, B2 | MVP |
| D1 | REST: client (auth, retry, rate limit), 5 paginators, landing + `_progress/_SUCCESS`, sealed event, recovery | XL | 7 | A5 | MVP |
| E1 | Discovery framework + JDBC discovery/profiling | M | 3 | B1 | MVP |
| E2 | File schema inference + REST sample schema | M | 3 | C2, D1 | MVP |
| E3 | Suggestion engine (keys, strategy, watermark, PII, DQ), pack writer, self-validation with M1 validator, `discover` CLI | M | 3 | E1 | MVP |
| F1 | Integration tier: Docker SQL Server/PostgreSQL scenarios, REST server fixtures, subprocess SIGKILL subset | L | 4 | B*, D1 | MVP (execution pending Docker) |
| F2 | Module specs, ADR addenda, evidence, coverage gate, review fixes, perf smoke (1 M rows local, measurement only) | M | 3 | all | MVP |
| X1 | Backfill chunking (`max_window`, `max_batches_per_run`) | S | 1.5 | A5 | optional |
| X2 | Per-source circuit breaker | S | 1.5 | A7 | optional |
| X3 | SQL Server Change Tracking connector variant | M | 3 | B5 | optional / later |
| X4 | Scale smoke at 10 M rows + 500-object plan-time measurement | M | 2 | all | optional |
| | **MVP total** (A 35 + B 15 + C 6 + D 7 + E 9 + F 7) | | **79** | | |
| | **Optional** | | **8** | | |
Check: milestone rows below sum to the same 79 d (35 + 9 + 6 + 6 + 7 + 9 + 7).

## 2. Dependency graph and order
```
A1 ─┬─▶ A3 ─▶ A4 ─┐
A2 ─┴────────────┴─▶ A5 ─▶ A6 (harness) ─▶ A7 ─┬─▶ B1 ─▶ B2, B3, B4 ─▶ B5 ─▶ E1 ─▶ E3
                              (mock end-to-end) ├─▶ C1 ─▶ C2 ───────────────▶ E2 ─┘
                                                └─▶ D1 ────────────────────────────▶ F1 ─▶ F2
```
**Priority order (aligned with "real source → Bronze first"):** M2.1 engine on mock → M2.2 SQL Server/Azure SQL/PostgreSQL full+incremental+snapshot → M2.3 files → M2.4 CDC → M2.5 REST → M2.6 discovery → M2.7 hardening. Parallelisable with two engineers: {B*, C*} after A5; {D1} after A5; {E*} after B1/C2.

## 3. Milestones, exit criteria, estimate
| Milestone | Work packages | Days | Outcome | Exit criteria (all must hold) |
|---|---|---|---|---|
| **M2.1 Core engine on mock** | A1–A7 | 35 | End-to-end Bronze ingestion from a mock source: manifest, receipts, cursor, recovery, retry, runner, CLI | Crash matrix green for FULL/INCREMENTAL/SNAPSHOT on local Delta; I1–I7 hold; 0 skipped crash points |
| **M2.2 Relational sources** | B1–B3 | 9 | SQL Server, Azure SQL, PostgreSQL full/incremental/snapshot into Bronze | Contract tests green on mock-JDBC (SQLite-free local dialect tests) + Docker integration (pending evidence) |
| **M2.3 Files** | C1–C2 | 6 | CSV/JSON/Parquet file-incremental and snapshot | File crash matrix incl. re-delivery; corrupt-record policy tests |
| **M2.4 CDC** | B4–B5 | 6 | Change feed + SQL Server CDC with deletes and retention-gap policies | CDC crash matrix incl. deletes and 3 gap policies |
| **M2.5 REST** | D1 | 7 | Paginated, authenticated API ingestion with landing | API crash matrix incl. mid-pagination, sealed event |
| **M2.6 Discovery** | E1–E3 | 9 | `de-accel discover` → validated draft packs | Generated packs pass M1 validator; no sample values emitted (tested) |
| **M2.7 Hardening** | F1–F2 | 7 | Integration tier, specs, evidence | Coverage ≥ 90 % on new code; ruff/mypy strict clean; evidence logged; known limitations updated |
| Total MVP | | **79** | | |
Calendar guide (not a commitment): ~16 weeks for one engineer (79 d); ~9–10 weeks for two working in parallel after M2.1. **First real-source Bronze load** (SQL Server incremental) is reachable at the end of M2.2 ≈ day 44 (≈ week 5–6 with two engineers). Statuses follow the approved vocabulary: each milestone ends `LOCAL_VERIFIED` with `DATABRICKS_INTEGRATION_PENDING`; nothing is `PRODUCTION_READY`.

## 4. Test strategy
| Tier | Scope | Tooling / location | Runs here? |
|---|---|---|---|
| Unit | query/dialect generation, window and safe-bound computation, file selection, pagination parsing, op mapping, state fold, retry/backoff math, error mapping, secret resolution, schema-policy matrix | pytest, fake clock, in-memory `TableStore` | yes |
| Contract | one shared suite per interface: capabilities truthful, `plan` deterministic & secret-free, `extract` stays inside plan, errors correctly classed — run against Mock, JDBC (dialect-level), File, REST | `tests/contract/` | yes (Docker-backed JDBC variants no) |
| Component | Bronze writer on **local Delta**: idempotent replay, receipt verification, schema evolution matrix, audit columns, column mapping; engine end-to-end on Mock/File/REST-fixture | pytest `-m component` | yes |
| **Crash-recovery** | Doc 04 §8 matrix: 6 load types × CP01–CP15 × ≥3 batches (≈ 270 scenarios) + double-fault + concurrency + random sequences; subprocess SIGKILL subset | `core/faults.py` | yes |
| Integration (Docker) | real SQL Server (incl. CDC, rowversion) and PostgreSQL schemas, REST mock server, large-file reads | Testcontainers, `-m integration` | **no (no Docker daemon here)** → pending |
| Databricks | JDBC drivers on the chosen DBR, UC external locations/volumes, service-credential token for Azure SQL, secret scopes, Workflows job (`max_concurrent_runs`, repair run), Delta behaviours (txn idempotency, type widening) on the real runtime | `-m databricks` | **no (no workspace)** → `DATABRICKS_INTEGRATION_PENDING` |
| Security | secrets never in manifests/events/logs/exceptions (canary secret test), env provider blocked in PROD, generated SQL injection tests (hostile identifiers rejected), discovery emits no values | pytest | yes |
| Performance (measurement only) | 1 M-row local ingest, plan time for 500 objects | script, results recorded, no pass/fail claim | yes |
Definition of done per work package: tests green and recorded in `TEST_EVIDENCE.md`; module spec updated; `IMPLEMENTATION_STATUS.md` updated with LOCAL_VERIFIED / DATABRICKS_INTEGRATION_PENDING; no baseline deviation.

## 5. Risks
| Risk | Mitigation |
|---|---|
| Databricks-only behaviours unverified (JDBC drivers, service credential, type widening, txn idempotency on the runtime) | explicit pending checks with fallbacks; early Databricks smoke test the day a workspace exists |
| No Docker here → real-DB bugs found late | integration tier runs on dev machine/CI; dialect unit tests + contract tests reduce risk; schedule an early SQL Server smoke on a real instance |
| Source diversity (CDC variants, API behaviours) | narrow MVP scope (D4); connector contract tests; later connectors are additive |
| Watermark edge cases (timestamp ties, in-flight transactions, clock skew) | safe upper bounds + lookback + property tests; documented residual limits |
| Large initial loads / listing cost | `max_window`, partitioned reads, `max_files_per_batch`; measurements in X4 |
| M1 touch (CR-M2-01) | additive/expand-only migration; M1 tests must stay green; re-pin checksum with approval |
| Effort estimate uncertainty (79 d) | milestone gates; re-estimate after M2.1 |

## 6. Approval gate
Nothing in this set is implemented. Implementation starts only after your explicit approval of: **CR-M2-01 (D1)** and decisions **D2–D10** in ADR-0019 (or your amendments). Suggested first step after approval: M2.1 (A1→A7).
