# Module spec — M2 ingestion engine (source → Bronze)

Design: `docs/architecture/m2/` and ADR-0019. **Closure review and resolution: `docs/architecture/m2/07_M2_closure_review.md` (2026-11-04).**
Statuses use: *Implemented and locally tested* · *Integration tested* · *Not implemented* · *Pending external environment validation*.

## 1. Implemented capabilities (commit e0e3c27)
| Package | Content |
|---|---|
| `connectors/base.py` | `Connector` protocol, `Capabilities`, plan/extract types, registry + entry-point loader, static-validator adapter |
| `connectors/mock` | deterministic mock source with fault hooks and read counters (tests/demos) |
| `connectors/jdbc` | SQL Server, Azure SQL, PostgreSQL: FULL / INCREMENTAL / SNAPSHOT, partitioned reads, safe upper bounds (timestamp, integer, SQL Server rowversion), typed-literal SQL, whitelisted identifiers, TLS-only URLs, Key Vault credential references |
| `ingestion/windows.py`, `engine.py`, `runner.py` | window arithmetic; recovery-first single-object engine with retry; thread-pool runner with per-source caps |
| `audit/` | manifest, event log + state fold, receipts, conditional watermark, lease, run log repositories |
| `processing/bronze` | audit columns, schema-evolution policy, transactional idempotent writer, commit verification |
| `core/` | `SecretProvider` (Key Vault scope / env for local), `Clock`, `FaultInjector`, ingestion error classes, py4j wire-log silencing |
| `cli` | `ingest`, `batch-status`, `abandon-batch`, `reprocess` |
| M1 additive changes | schema v2 migration (`read_options`, `cdc_op_map`, audit columns), loader schema check, connector validation hook + rules MDV-6001/6002/6003/6004/6010 (65 rules in total), `--env all` fix, migrator re-apply fix; **closure fixes:** Spark store returns timezone-aware UTC timestamps (rebuilt from epoch microseconds) and retries Delta commit conflicts with bounded jittered backoff (`StoreConflictError`, TRANSIENT) |

## 2. Tested capabilities (what the passing tests prove — 398 tests, 95% coverage)
| Capability | Evidence | Qualification |
|---|---|---|
| Bronze append: raw preservation, audit columns, idempotent replay (same/stale `txnVersion` skipped), commit verification, schema evolution | 16 tests on **real local Delta** + 29 pure schema-policy tests | open-source Delta 3.2 only |
| Observed rows must equal the Delta commit's rows or the batch is not committed (B3) | 2 tests | local |
| Retry, recovery, duplicate prevention, lease, abandon, in-flight snapshot pinning | 12 engine tests + 37-test crash matrix (10 points × FULL/INCREMENTAL/SNAPSHOT) | in-memory audit store |
| **The same engine on the Delta-backed audit store** (A5): full and incremental cycles, exact event log, lease busy/takeover, timestamps independent of process time zone, crash recovery at 3 points without loss/duplicates, **4-way parallel run (two rounds, 38 Delta commit-conflict retries absorbed)**, CLI `ingest` + `batch-status` | 10 tests, 10 min | **local Delta only** |
| Failure isolation when audit logging fails (A4): result still reported, run continues, runner safety net | 4 tests | local |
| CLI `ingest`, `batch-status`, `abandon-batch` | 10 tests + 1 on Delta | `reprocess` is **experimental / untested** |
| Real relational source via JDBC → Bronze | 6 tests on a **real PostgreSQL 16 server** (full, incremental with updates and lookback, partitions, crash recovery, wrong password = PLATFORM not retried, connection refused = TRANSIENT retried, secret canary) | audit tables in memory |
| SQL Server / Azure SQL SQL generation and validation | 29 unit tests of exact SQL strings + 11 tests of accepted/rejected configuration | **never executed on SQL Server** |
| Secret handling incl. the PROD guard of the env provider (B5) | 14 tests | Databricks provider tested with a fake `dbutils` only |
| M1 compatibility and in-place v1→v2 upgrade | full M1 suite + upgrade test on real Delta | — |

## 3. Pending integration validations
Real SQL Server / Azure SQL execution · Databricks (Key Vault secret scope, Unity Catalog, JDBC drivers on the runtime, Delta behaviours on the runtime, job template, wheel install, service credential) · Azure DevOps pipeline · scale/performance. Delta-backed audit behaviour is verified on **local** Delta only; production-sized Delta conflict behaviour is unmeasured (each batch performs ~25 small audit commits — maintenance/compaction is a later concern).

## 4. Supported configuration surface and known limitations
**Supported (validator accepts exactly this):** JDBC sources `sqlserver | azuresql | postgresql` with `auth_mode: SECRET`; load types `FULL`, `SNAPSHOT`, `INCREMENTAL` (timestamp, integer, SQL Server rowversion watermarks; `lookback_window`; `partition_column` on an integer column + `num_partitions`); source options `fetchsize, max_connections, encrypt, trust_server_certificate, query_timeout_sec, login_timeout_sec`; schema evolution `FAIL | ADD_COLUMNS | ALLOW_WIDENING | RESCUE`; Bronze append-only history.
**Rejected by the validator (not implemented):** `CDC`; `auth_mode` `SERVICE_PRINCIPAL` / `MANAGED_IDENTITY`; all `read_options` keys; source option `isolation_level`; `cdc_op_map`; `late_arrival_window` (use `lookback_window` or a rowversion watermark); `partition_cols`, `write_mode` other than APPEND, `backfill_from/to` (use `reprocess`).
**Not implemented:** discovery/metadata auto-generation; CSV/JSON/Parquet connectors; REST; CDC and deletes; backfill chunking; source-count reconciliation; connector contract-test suite; per-source circuit breaker; a way to add JDBC drivers to a *local* CLI Spark session (drivers must come from the Databricks runtime, or tests).
**Deferred defects/debt (tracked, accepted for M2 closure):** timestamp watermarks are treated as UTC labels (source time zone and source clock not modelled; mitigated by the validator rejecting `late_arrival_window`); `reprocess` uses a constant run id, writes no `run_pipeline` row and does not audit `--reason`; Databricks job template incomplete (no UC access mode, no `migrate`/`load-pack` tasks); non-integer partition columns give an unclassified error and `max_connections` caps objects, not DB connections; lease renewal result ignored and no heartbeat (30-min TTL); `verify()` scans table history; connectors never closed; physical comments of activated audit tables still say `DDL_ONLY`; 10 of 15 designed crash points.
INCREMENTAL cannot detect source deletes (by design). A crashed (not failed) runner holds its lease until the TTL expires.

## 5. Protocol (as built)
plan (frozen window) → manifest (insert-once) → lease → extract → Bronze append with `txnAppId/txnVersion` + commit metadata → verify (history + count) → receipt → conditional watermark → events/lineage → DONE. Empty plans create no batch. A handled failure releases the lease; a crash leaves it to expire. Open batches resume with the snapshot they were planned with.
