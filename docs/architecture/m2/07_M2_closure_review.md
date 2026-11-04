# M2 Validation and Closure Review (with resolution)

Reviewed commit: `e0e3c27` on branch `m2-ingestion` (clean tree, equals remote). Date: 2026-11-04. Scope: M2.1 (core ingestion engine) and the M2.2 JDBC slice.
Method: code review against `docs/architecture/m2/00–06` and ADR-0019; full automated test run; read-only probes (`review_probes/`) for the paths the test suite does not exercise.

## 0. Resolution (post-fix, 2026-11-04) — read this first

Scope approved by the user: the five critical fixes (A1–A5), five essential items (B1–B5), documentation and this report. Everything else (B6–B12, discovery, file connectors, REST, CDC and deletes, advanced orchestration) is **deferred**. M2 closure covers the implemented ingestion foundation, the JDBC connector and Bronze Delta ingestion (M2.1 + the implemented JDBC portion of M2.2).

### Final results
| Check | Result |
|---|---|
| `pytest -q tests --cov=de_accelerator` | **398 passed, 0 failed, 0 skipped** (20 min 33 s) |
| Coverage | **95 %** (4,022 statements, 208 missed) |
| `ruff check src tests` | All checks passed |
| `mypy --strict` | No issues in 72 source files |
| Test growth | 335 → 398 (+63) since the review; every fix has a regression test |

### Findings → resolution
| ID | Fix | Regression tests |
|---|---|---|
| **A1** Event log failed on Delta (naive vs aware timestamps; 50 junk rows) | Delta timestamps are read as epoch microseconds and rebuilt as timezone-aware UTC (independent of the process time zone); event append verifies its own insert by a nonce in the payload, never by timestamp equality | `test_event_append_writes_exactly_one_row_per_call_even_with_naive_timestamps`, `test_full_load_with_delta_audit_writes_exactly_the_protocol_events` (exactly 6 event rows, was 50 junk + error), `test_delta_timestamps_are_utc_aware_whatever_the_process_timezone` (Kolkata / Los Angeles / UTC) |
| **A2** Lease failed for another owner on Delta | Same timestamp normalisation; lease comparison defensive (`_utc`) | `test_lease_busy_and_takeover_with_naive_timestamps`, `test_lease_busy_and_takeover_with_delta_audit` |
| **A3** Delta commit conflicts treated as fatal | `SparkDeltaTableStore` detects Delta concurrency exceptions and retries every write with bounded jittered backoff; persistent conflicts raise `StoreConflictError` (TRANSIENT, so the engine's retry policy applies) | `test_conflict_detection`, `test_retry_*` (3), `test_four_way_parallel_ingestion_with_delta_audit` (**38 conflict retries absorbed, all objects DONE, two rounds**) |
| **A4** Failure path could abort the whole run | Failure/result audit writes are best-effort (logged, never raised); runner catches unexpected exceptions per object | `test_failure_path_survives_audit_outage…`, `test_success_path_survives_result_logging_outage`, `test_runner_isolates_an_object_whose_audit_logging_also_fails`, `test_runner_safety_net_contains_unexpected_exceptions` |
| **A5** No test covered the Delta audit path | New module runs the engine on the Delta audit store: full/incremental cycles, exact event log, lease busy/takeover, time-zone independence, crash recovery at 3 points (CP04/CP06/CP08) without loss or duplicates, 4-way parallel run, CLI | `tests/component/test_ingestion_on_delta_audit.py` (10 tests, 10 min) |
| **B1** Validator accepted unimplemented settings | Rejected, not ignored: CDC; SERVICE_PRINCIPAL / MANAGED_IDENTITY; all JDBC `read_options`; `isolation_level`; `cdc_op_map`; new rule **MDV-6004** for `partition_cols`, `write_mode` ≠ APPEND, `backfill_from/to` (65 rules) | 11 new validator tests |
| **B2** `late_arrival_window` could skip in-flight transactions | Rejected for JDBC; documented alternatives (lookback; rowversion uses SQL Server's own safe bound). Demo pack updated | `test_unimplemented_settings_are_rejected_not_ignored[late_arrival_window (B2)]` |
| **B3** Observed vs committed rows not checked | Batch fails with `IntegrityError` (no receipt, no watermark) if rows read ≠ rows in the Delta commit | `test_bronze_write_integrity_mismatch_blocks_the_commit`, `test_matching_row_counts_pass_the_integrity_check` |
| **B4** New CLI commands untested | `ingest`, `batch-status`, `abandon-batch` tested (exit codes, JSON, subset, failure, recovery) incl. on the Delta audit store; `reprocess` labelled **experimental** | `tests/component/test_cli_ingestion.py` (10) + 1 on Delta |
| **B5** EnvSecretProvider PROD guard untested | Tested, plus Key Vault reference handling, redaction and the Databricks provider with a fake `dbutils` | `tests/unit/test_secrets.py` (14); `core/secrets.py` coverage 50 % → 96 % |

M1 code touched by the fixes (all necessary, all additive in behaviour): `metadata/store/spark.py` (UTC-aware timestamps from epoch microseconds, conflict retry, `unix_micros` projection), `core/errors.py` (`StoreConflictError`), `metadata/validation/rules_connectors.py` (rule 6004). M1 behaviour change to note: timestamps read from a Delta store are now timezone-aware (they were naive).

### Acceptance criteria
| # | Criterion | Result |
|---|---|---|
| 1 | M1 backward-compatible | **Met** — all M1 tests pass; migration `0001` checksum unchanged (pinned); v1→v2 upgrade tested on real Delta. Two behaviour notes above. |
| 2 | Approved defects fixed with regression tests | **Met** — table above |
| 3 | Delta-backed audit and recovery tests pass | **Met** — 10/10 incl. crash recovery at 3 points and idempotent re-run |
| 4 | Four-way parallel ingestion passes | **Met** — 2 rounds × 4 objects, all DONE, 38 conflict retries absorbed |
| 5 | CLI ingestion and batch-status tests pass | **Met** — in-memory (10) and on Delta (1) |
| 6 | Bronze write integrity verified | **Met** — commit verification (history + count), observed-vs-commit check, idempotent replay tests |
| 7 | Docs distinguish local validation from real SQL Server / Databricks | **Met** — quickstart, module spec, status, limitations; every unverified step marked PENDING |
| 8 | No unsupported feature reported as implemented | **Met** — validator rejects what is not implemented; docs list not-implemented items; reprocess marked experimental |

### Still NOT verified (unchanged by these fixes)
Real SQL Server / Azure SQL execution · any Databricks workspace (Key Vault scope, Unity Catalog, drivers, job template, Delta behaviour on the runtime) · Azure DevOps pipeline · scale/performance · production-sized Delta conflict behaviour (each batch makes ~25 small audit commits). Nothing is `PRODUCTION_READY`.

### Final recommendation
**M2 can be closed as `LOCAL_VERIFIED` for the defined scope, with SQL Server and Databricks validation explicitly PENDING, and M3 may start once you approve closure.** Mandatory pre-M3 items (A1–A5, B1–B5) are done and tested. Items deferred by your decision (B6–B12) are tracked in `docs/status/KNOWN_LIMITATIONS.md`; none blocks M3. Recommended soon after (not blocking): run the SQL Server container test on a Docker-capable machine and a first Databricks smoke run, because both can still reveal environment-specific issues.

---
# Original review (as found, before the fixes)

## 1. Original recommendation (before fixes — superseded by §0)

**NOT READY to close M2. Do not start M3 yet.** The implementation is solid where it was tested, but the review found that **the engine does not work on the storage it is designed to use in production (Delta-backed audit tables)**, and that the whole test suite hid this by using an in-memory audit store. Five critical findings (C1–C5, about 4–5 engineer-days to fix) must be resolved first. Real SQL Server and Databricks validation remain separate, external, pending items that I cannot perform from this environment.

| Question | Answer |
|---|---|
| Can the SQL Server-to-Bronze demo run today? | **No.** Not on SQL Server (none available) and, independently of that, not on any Delta-backed audit store — including the local one — until C1–C3 are fixed. The CLI `ingest` command uses the Delta store, so it is blocked too. |
| Is the engine logic itself sound? | Yes where tested: 335 tests pass, including a 37-test crash-recovery matrix and real-PostgreSQL integration, **all with the audit tables held in memory**. |
| Can we mark anything `PRODUCTION_READY`? | No. Nothing is. |

## 2. Test results (independent re-run on the pushed commit)

| Check | Result |
|---|---|
| `pytest -q tests --cov=de_accelerator` | **335 passed, 0 failed, 0 skipped** (532 s) |
| Coverage | **93 %** (3,947 statements, 259 missed) |
| `ruff check src tests` | All checks passed |
| `mypy --strict` | No issues in 72 source files |
| M1 backward compatibility | All M1 tests (182 at closure) pass; only a handful of assertions were deliberately updated for schema v2 (migration statement counts, schema-version strings, one validator case); migration `0001` checksum is pinned by test and unchanged; M1 example pack still validates; a v1 database upgrades in place on real Delta (data preserved) |
| Behavioural changes to M1 users | (a) `validate-config --env all` now means "the environments the pack declares" (was always DEV/QA/PROD); (b) `load-pack` now refuses a database below schema v2 with "run migrate"; (c) validator has 4 more rules (64) |

Coverage hot-spots that matter (from the report): `cli/main.py` 78 % — **lines 282–321, i.e. all four new CLI commands, have zero test coverage**; `core/secrets.py` 50 % — the PROD guard of `EnvSecretProvider` and the Databricks provider are untested; `ingestion/windows.py` 78 % — the integer-watermark branch is untested; `connectors/jdbc/connector.py` 87 %.

### Retry, recovery, idempotency, duplicate handling — what is actually verified
| Behaviour | Tests | Audit store used | Verdict |
|---|---|---|---|
| Crash at 10 protocol points × FULL/INCREMENTAL/SNAPSHOT, recovery by a fresh engine; no loss/duplicates; source not re-read when only the watermark was outstanding | 30 + 3 double-fault + 4 targeted | **in-memory** | Logic verified; **not verified on Delta** |
| Transient retry, exhausted retries stay resumable, fatal needs `--restart`, abandon, lease busy/takeover, concurrent runners | 12 engine end-to-end tests | **in-memory** | Same caveat |
| Idempotent Bronze append (`txnAppId/txnVersion` replay and stale version skipped), commit verification, schema evolution | 16 + 29 | Bronze on **real local Delta** | **Verified** |
| Same flow against a real database | 6 PostgreSQL tests | audit **in-memory**, Bronze on real Delta | Source side verified; audit-on-Delta not |
| Repositories on Delta (manifest, receipt, watermark, run log) | probe | **real Delta** | PASS |
| Repositories on Delta (event log, lease for another owner) | probe | **real Delta** | **FAIL (C1, C2)** |
| Parallel objects with Delta audit tables | probe | **real Delta** | **FAIL (C3)** |

## 3. Findings

Severity: **Critical** = blocks closure (the product cannot work as designed); **High** = must be fixed or explicitly accepted before relying on it; **Medium/Low** = technical debt. Evidence is a probe result (P), a coverage fact (C), or code inspection (I — reasoned from the code, not executed).

### Critical
| ID | Finding | Evidence | Impact |
|---|---|---|---|
| **C1** | `EventLog.append` fails on Delta: Delta returns *naive* timestamps, the code compares them to *timezone-aware* ones (`==` is silently `False`), so it never recognises its own insert, retries 50 times and **writes 50 junk event rows**, then raises `RuntimeError`. | P: `probe_repos_on_delta.py` (50 rows written, FAIL) | Every engine run on Delta fails at the first event. Corrupts the audit log. |
| **C2** | `LeaseRepo.acquire` raises `TypeError` (naive vs aware) whenever another owner holds or has held the lease — the busy and crash-takeover path. | P | Crash recovery and overlapping-run protection are broken on Delta. |
| **C3** | Parallel objects write the same audit tables concurrently; Delta raises `ConcurrentAppendException`, which the engine classifies as a fatal LOGIC error instead of retrying. | P: `probe_delta_audit.py` (concurrency 4) | Default concurrency (4) cannot work on Delta; objects fail spuriously. |
| **C4** | The failure-handling path itself writes audit rows (`_record_failure`, `_finish`); if that raises, the exception escapes `ingest_object` and **aborts the whole run**, losing other objects' results. | P (`RuntimeError` aborted the runner at concurrency 1) | One audit-store error defeats failure isolation, a core requirement. |
| **C5** | No test runs the engine, crash matrix or runner against the Delta-backed audit store; all 335 tests passed while C1–C4 existed. | C / P | The "LOCAL_VERIFIED" claim for the engine is only valid for the in-memory store. |

### High
| ID | Finding | Evidence |
|---|---|---|
| H1 | `late_arrival_window` is applied against the **framework's UTC clock**, not the source's clock, and source timestamps are treated as UTC labels. If the source's local time is behind UTC, the hold-back is ineffective and in-flight transactions can be **skipped (data loss)**. `source_timezone` is accepted by the validator but never used. | I (`incremental_window`, `_wm_text`) |
| H2 | Options accepted by the validator but **not implemented** (silently ignored): `source_timezone`, `max_window`, `max_batches_per_run`, `change_table`, `capture_instance`, `isolation_level`. `CDC` is accepted as a load type by the validator but fails at runtime ("not implemented"). CI says "valid", runtime says otherwise. | P: grep shows 0 uses |
| H3 | Authentication gaps: `SERVICE_PRINCIPAL` is sent to SQL Server as a plain SQL login (no Entra authentication setting) so it cannot work; the `MANAGED_IDENTITY` token call (`dbutils.credentials…`) is a best-effort guess that was never run. Both pass validation. | I; C (`secrets.py`, `_access_token` uncovered) |
| H4 | The four new CLI commands have no tests. Probe shows the happy paths work, but `reprocess` uses a **constant run id** (`reprocess-<actor>`) for every run, creates **no `run_pipeline` row** (orphan `run_object` rows), and `--reason` is printed but **not stored** (not audited as designed). | C (lines 282–321 = 0 %); P: `probe_cli.py` |
| H5 | Databricks job template is incomplete: no `data_security_mode` (required for Unity Catalog), does not deploy the bootstrap YAML or the pack, and has no task for `migrate`/`load-pack`, so the 5-step workflow cannot be executed from the template alone; `/Workspace/...` bootstrap path assumed readable. | I; not run |
| H6 | Design promised "mismatch between observed rows and commit rows fails the batch" and a source-count check. Only commit-vs-counted-rows is checked; `connector.count()` exists but is never called. M2.2's "reconciliation" item is therefore not delivered. | I |
| H7 | `partition_column` is assumed integer (`int()` on min/max): a date/uuid column raises an unclassified error. `max_connections` caps concurrent *objects*, not real DB connections (each partitioned read opens `num_partitions` connections). | I |

### Medium / Low (technical debt)
Lease renewal result is ignored and there is no heartbeat (a >30 min run can lose its lease unnoticed; correctness still protected by idempotent writes); lease TTL hard-coded to 30 min; `EnvSecretProvider` PROD guard untested; integer/rowversion watermark paths untested beyond SQL strings; connector capability flags overstated (`discovery=True` and `count_pushdown=True` without callers/`discover()`); the promised shared connector contract-test suite was not written; `open_batch` logic is convoluted and `ManifestRepo.create`'s second return value is unused; `verify()` loads the whole Delta history each batch (cost grows with commits); connectors are cached per run and never `close()`d; physical table comments of activated audit tables still say `DDL_ONLY`; Delta type widening unavailable locally (documented fallback); crash points implemented 10 vs 15 designed; Bronze `cast_to_table` is case-sensitive; spark redaction conf set best-effort.

### Placeholders / hardcoded assumptions found
No `TODO`/`FIXME`. `NotImplementedError` only in the abstract dialect base. Hard-coded: lease TTL 30 min; stream name `live`; layer `BRONZE`; default ports 1433/5432; UTC assumption for source timestamps; `15.4.x` runtime and `Standard_DS3_v2` in the template (variables with defaults); watermark types limited to timestamp/integer/rowversion. No client-specific values in core.

## 4. SQL Server integration — exactly what is verified

**Not tested against a real SQL Server or Azure SQL instance.** There is no SQL Server in this environment (no listener on 1433, no Docker daemon).

| Tested | How | Does it prove SQL Server works? |
|---|---|---|
| Generated T-SQL (bracket quoting, `CAST(... AS datetime2(6))` literals, `rowversion` as `CAST(n AS binary(8))`, `MIN_ACTIVE_ROWVERSION()` bound, TLS-only JDBC URL, hostile-identifier rejection, exact incremental query text) | 29 unit tests of the strings | No — proves the SQL text, not that SQL Server accepts or returns what we assume |
| The JDBC read/plan/extract machinery (partitioned reads, window, watermark, retries, auth failure, connection refused, secret canary) | **Real PostgreSQL 16 server** through the same `JdbcConnector` | Partly — same code path, different database and driver |
| Not tested at all | SQL Server driver availability on the runtime, `datetime2`/`rowversion`/`decimal` type mapping, `encrypt`/certificate behaviour, login/permission errors as SQL Server words them, Entra/managed-identity auth, CDC | — |

## 5. Databricks integration — exactly what is verified

**Nothing has been run in a Databricks workspace.** All Delta behaviour was verified on local open-source Delta 3.2 / Spark 3.5.9 only.

| Component | Local evidence | Databricks / Unity Catalog evidence |
|---|---|---|
| Bronze Delta write (append, `txnAppId/txnVersion`, `userMetadata`, column mapping, schema evolution) | Verified on local Delta | **None** — UC three-level names, table properties (`minReaderVersion/minWriterVersion`), type widening, `Observation` on shared/Spark Connect clusters unverified |
| Audit tables on Delta | Partly verified; **defective (C1–C3)** | None |
| Key Vault secret scope (`DatabricksSecretProvider`) | None (0 % covered path) | None |
| Job template `deployment/databricks/databricks.yml` | Reviewed only; never `bundle validate`d | None; incomplete (H5) |
| Wheel | Builds (109 KB), exposes `de-accel` console script | Not installed on a cluster |
| Unity Catalog grants, catalog/schema creation, external locations | n/a | None |
| JDBC drivers on the runtime, managed-identity token | n/a | None |

## 6. Closure matrix

| Area | Status |
|---|---|
| Connector interface, registry, entry-point loader, mock connector | **Implemented and locally tested** (no shared contract-test suite yet) |
| JDBC connector — PostgreSQL | **Integration tested** (real PostgreSQL 16 server; audit store in-memory) |
| JDBC connector — SQL Server / Azure SQL | **Implemented; unit-tested SQL generation only. Pending external validation** (no instance) |
| SERVICE_PRINCIPAL / MANAGED_IDENTITY auth | **Not functional / unverified** (H3) |
| Batch protocol (manifest, events, receipts, watermark, lease, run log) | In-memory store: **implemented and locally tested**. Delta store: **defective (C1–C3), not tested** |
| Bronze writer (audit columns, idempotent append, verification, schema evolution) | **Implemented and locally tested on real local Delta** |
| Ingestion engine — retry, recovery, duplicate prevention | **Implemented and locally tested (in-memory audit store)**; Delta path **blocked** |
| Runner / parallel ingestion | Logic tested sequentially; parallel on Delta **fails (C3, C4)** |
| CLI `ingest`, `batch-status`, `abandon-batch`, `reprocess` | **Implemented; untested** (happy paths confirmed by probe); defects H4 |
| Schema v2 migration, loader check, validator hook/rules (M1 touch) | **Implemented and locally tested** (incl. in-place v1→v2 on real Delta) |
| Demo pack, quickstart, job template | **Files exist; not executable end-to-end** (C1–C3, H5) |
| Schema discovery / metadata auto-generation | **Not implemented** |
| File connectors (CSV/JSON/Parquet), REST, CDC & deletes, late-arrival beyond lookback, backfill chunking, reconciliation | **Not implemented** |
| Real SQL Server execution; Databricks execution (UC, Key Vault, job, drivers); Azure DevOps pipeline; scale/performance | **Pending external environment validation** |
| Production readiness | **None** |

## 7. Proposed fix plan (needs your approval — no implementation changes were made in this review)

| Step | Items | Effort (est.) |
|---|---|---|
| **A — Critical (blocks closure)** | C1 + C2: normalise all timestamps to timezone-aware UTC at the store boundary (one place, `SparkDeltaTableStore.select`) and add a regression test; C3: classify Delta concurrent-commit errors as TRANSIENT and retry audit writes with backoff inside the repositories; C4: guard `_record_failure/_finish` so audit failures cannot abort the run; C5: run the engine happy path, a crash-matrix subset and a 4-way parallel run against the Delta audit store as regular component tests (convert the probes) | ≈ 4–5 d |
| **B — High** | H2 + H3: make the validator reject unsupported options/load types/auth modes until implemented (smallest honest fix); H1: take "now" from the source clock and document/implement the timezone assumption; H4: CLI tests, unique run id + `run_pipeline` row + audited `--reason` for reprocess; H6: enforce observed-vs-commit rows and record the source count; H5: complete the job template (UC access mode, migrate/load-pack tasks, bootstrap/pack deployment); H7: clear errors for non-integer partition columns | ≈ 6–7 d |
| **C — External validation (owner: you / environment)** | Real SQL Server run (a container on a Docker-capable machine is enough) and a Databricks workspace run, following `docs/guides/QUICKSTART_SQLSERVER_TO_BRONZE.md` | depends on environment |

Closure criteria I propose: Step A complete and green on Delta; Step B items either fixed or explicitly accepted as limitations; Step C recorded as pending (it does not block M3 design work, but M2 stays `DATABRICKS_INTEGRATION_PENDING` / SQL Server pending until it is done).

## 8. Conformance to the approved M2 design (summary)

| Design element (docs/architecture/m2) | As built |
|---|---|
| Connector interface, plan/extract split, registry | Built as specified; contract tests missing |
| Manifest immutability, deterministic windows, receipts, conditional watermark | Built and tested (in-memory); Delta defects C1–C3 |
| Empty plans create no batch | Built; deliberate refinement of doc 04 (recorded in the status docs) |
| Lease + idempotent writes for duplicate prevention | Built; lease renewal result ignored |
| Crash matrix (15 points designed) | 10 points implemented, all load types FULL/INCREMENTAL/SNAPSHOT; in-memory audit store |
| Schema evolution matrix | Built; type widening unverifiable locally |
| Secrets via Key Vault / managed identity | Key Vault reference model built; Databricks provider untested; managed identity unverified |
| Workflows job template | Drafted; incomplete and unvalidated |
| Discovery/auto-generation, files, REST, CDC | Not part of the implemented slice |
