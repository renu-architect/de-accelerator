# Module spec — M2 ingestion engine (source → Bronze)

Design: `docs/architecture/m2/` and ADR-0019. Status: see `docs/status/IMPLEMENTATION_STATUS.md`.

## Implemented so far (M2.1 + M2.2 slice)
| Package | Content | State |
|---|---|---|
| `connectors/base.py` | `Connector` protocol, `Capabilities`, plan/extract data types, registry + entry-point loader, validator adapter | implemented |
| `connectors/mock` | deterministic mock source with fault hooks and read counters | implemented (test/demo) |
| `connectors/jdbc` | SQL Server, Azure SQL, PostgreSQL: FULL / INCREMENTAL / SNAPSHOT, partitioned reads, safe upper bounds, typed-literal SQL, TLS-only URLs, Key Vault credentials | implemented; PostgreSQL verified on a real server; SQL Server/Azure SQL **unit-tested only** |
| `ingestion/windows.py`, `engine.py`, `runner.py` | window arithmetic, recovery-first single-object engine with retry, thread-pool runner | implemented |
| `audit/` | manifest, event log + state fold, receipts, conditional watermark, lease, run log repositories | implemented |
| `processing/bronze` | audit columns, schema-evolution policy, transactional idempotent writer, commit verification | implemented |
| `core/` | `SecretProvider` (Key Vault scope / env for local), `Clock`, `FaultInjector`, ingestion error classes | implemented |
| `cli` | `ingest`, `batch-status`, `abandon-batch`, `reprocess` | implemented |
| M1 additive changes | schema v2 migration (`read_options`, `cdc_op_map`, audit columns), connector validation hook + rules MDV-6001/6002/6003/6010 | implemented |

## Not implemented yet (planned, in order)
CDC + deletes + retention-gap policies · file connectors (CSV/JSON/Parquet) + file tracking · REST · discovery/auto-generation ·
SQL Server native CDC · Azure SQL managed-identity token path (Databricks-only) · parallel-scale/perf measurement · Databricks execution.

## Protocol (as built)
plan (frozen window) → manifest (insert-once) → lease → extract → Bronze append with `txnAppId/txnVersion` + commit metadata → verify
(history + count) → receipt → conditional watermark → events/lineage → DONE. Empty plans create no batch. A handled failure releases the lease;
a crash leaves it to expire (TTL 30 min). Open batches are resumed with the snapshot they were planned with.

## Invariants tested (crash matrix, 10 crash points × FULL/INCREMENTAL/SNAPSHOT)
No loss/duplicates; one DONE per batch; cursor never ahead of the receipt; recovery resumes the same batch; the source is not re-read when
only the watermark commit was outstanding; later batches keep contiguous windows.
