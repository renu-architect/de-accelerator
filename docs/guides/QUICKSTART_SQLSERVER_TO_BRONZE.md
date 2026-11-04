# Quickstart — SQL Server table → Bronze Delta table

> **STATUS (M2 closure, 2026-11-04) — read before using.**
> The ingestion engine, the JDBC connector and the CLI are implemented and **verified locally**: mock source and a **real PostgreSQL server** through the same JDBC code, with the audit tables on **real local Delta** (including crash recovery and a four-way parallel run).
> **Not yet verified: a real SQL Server / Azure SQL instance, and any Databricks workspace.** The SQL Server SQL generation is unit-tested only. Every step marked **[PENDING]** below has never been executed in the target environment — expect to find environment-specific issues (driver, TLS, types, Unity Catalog, secret scope).
> Nothing here is production-ready.

## Prerequisites
| Need | Detail |
|---|---|
| SQL Server | reachable from the cluster; a **read-only** login; the source table has a `datetime`/`datetime2` column that is updated on every change (watermark), or a `rowversion` column |
| Databricks workspace (Unity Catalog) | catalog `<client>_<env>` with schemas `meta, audit, dq, recon, gov, bronze, silver, gold`; a UC-compatible cluster access mode; a service principal for the job |
| Key Vault-backed secret scope | name = `secret_scope` in the bootstrap file; secrets named in the pack's `kv:` references (demo: `erp-host`, `erp-db`, `erp-user`, `erp-pwd`) |
| Runtime | Databricks LTS runtime with Python 3.11 / Spark 3.5 (`config/runtime_matrix.yaml`, **unverified**); the SQL Server JDBC driver must exist on the runtime (**unverified**) |
| Local tools | Python 3.11, `uv`, Databricks CLI |

## Configuration — the only thing a developer writes
`metadata/examples/sqlserver_demo_pack/` is a complete example: one source (`erp_sql`, Key Vault references only), one object (`orders`), one incremental load strategy (watermark `ModifiedDate`, 1 h lookback, 8 partitions on `OrderID`), one Bronze mapping, one execution. More tables = more items in `objects.yaml`, `load_strategies.yaml` and `layer_mappings.yaml`; no per-table code.

**What the validator accepts (everything else is rejected, not ignored):** `source_type` sqlserver | azuresql | postgresql; `auth_mode: SECRET`; `load_type` FULL | SNAPSHOT | INCREMENTAL; watermark types timestamp | integer | rowversion (SQL Server); `lookback_window`; `partition_column` (integer column) + `num_partitions`; source options `fetchsize, max_connections, encrypt, trust_server_certificate, query_timeout_sec, login_timeout_sec`.
**Rejected until implemented:** CDC; service-principal and managed-identity auth; all `read_options`; `late_arrival_window` (use `lookback_window`, or a `rowversion` watermark which uses SQL Server's own safe upper bound); `partition_cols`; `write_mode` other than APPEND; `backfill_from/to` (use `reprocess`, experimental).

Bootstrap file `bootstrap/dev.yaml` (not committed; one per environment):
```yaml
client_code: acme            # lower case; catalog becomes acme_dev
environment: DEV
flavor: DATABRICKS           # LOCAL only for tests/laptop (also needs warehouse_dir and DEACCEL_SECRET_<NAME> env vars)
secret_scope: kv-acme-dev    # must match the Key Vault-backed secret scope
```

## The five steps
| # | Step | Command | Evidence today |
|---|---|---|---|
| 1 | Configure | edit the pack YAML | — |
| 2 | Validate | `de-accel validate-config metadata/examples/sqlserver_demo_pack` | **works** (65 rules, tested) |
| 3a | Create metadata schema (once per environment) | `de-accel --bootstrap bootstrap/dev.yaml migrate` | migrator tested on real local Delta; **[PENDING]** on Databricks |
| 3b | Load + activate the release | `de-accel --bootstrap bootstrap/dev.yaml load-pack metadata/examples/sqlserver_demo_pack --activate --approved-by <name>` (`rollback` flips back) | loader/release manager tested on real local Delta; **[PENDING]** on Databricks |
| 4 | Run ingestion | `de-accel --bootstrap bootstrap/dev.yaml ingest --execution daily_orders` | engine + CLI tested end-to-end (mock source, real PostgreSQL, Delta audit store, 4-way parallel); **[PENDING]** against SQL Server and on Databricks |
| 5 | Monitor | `de-accel --bootstrap bootstrap/dev.yaml batch-status --object orders` (or `--json`); tables `audit.run_pipeline`, `run_object`, `run_batch_manifest`, `run_batch_event`, `run_target_receipt`, `wm_watermark`, `run_error`; data in `<catalog>.bronze.erp_orders` | tested (CLI + Delta) |
Steps 3a, 3b and 4 need Spark, so on Databricks they must run on a cluster (one-off job tasks or a notebook calling the same CLI entry point). The bundle template `deployment/databricks/databricks.yml` currently contains **only step 4** and is **incomplete and never validated** (no Unity Catalog access mode, no migrate/load-pack tasks, bootstrap/pack not deployed).

**Local CLI limitation:** with `flavor: LOCAL` the CLI starts a plain local Spark session and has no setting for extra JDBC driver jars, so a *local* CLI run against a real database is not possible; on Databricks the driver must come from the runtime. Local verification is done through the pytest suites below.

## Databricks deployment (target) **[PENDING]**
```bash
uv build --wheel                              # produces dist/de_accelerator-*.whl (builds today)
databricks bundle validate -t dev --var client=acme --var environment=DEV --var service_principal=<app-id>
databricks bundle deploy   -t dev ...         # template is incomplete — see above
databricks bundle run ingest_daily_orders -t dev
```

## What you can run today (no SQL Server, no Databricks)
```bash
uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -e ".[spark,dev]"
.venv/bin/python -m pytest tests/component/test_ingestion_engine.py            # mock source -> Bronze, retries, recovery
.venv/bin/python -m pytest tests/component/test_ingestion_crash_matrix.py      # 37 crash/recovery scenarios
.venv/bin/python -m pytest tests/component/test_ingestion_on_delta_audit.py    # engine on the Delta audit store incl. 4-way parallel (~10 min)
.venv/bin/python -m pytest tests/integration                                   # REAL PostgreSQL -> Bronze (needs the PostgreSQL 16 server binaries and root)
```
To try a real SQL Server yourself: run `mcr.microsoft.com/mssql/server` in Docker, create an `Orders` table with a `ModifiedDate datetime2` column, and write a pytest in the style of `tests/integration/test_postgres_to_bronze.py` with `source_type: sqlserver` and the SQL Server JDBC driver added to the Spark session (`extra_packages=["com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11"]`). **Not attempted here** (no Docker daemon); expect issues the PostgreSQL run cannot reveal (types, TLS, driver, `datetime2` precision).

## What is verified
| Claim | Evidence |
|---|---|
| YAML → validate → release → snapshot → engine → Bronze Delta, with crash recovery, retries and no duplicates | local tests; **audit tables on real local Delta** (A5) and in memory (matrix) |
| Four objects ingested in parallel with Delta audit tables | local test (38 Delta commit conflicts absorbed by retry) |
| Real database via JDBC → Bronze (full, incremental with updates and lookback, partitions, auth failure, connection refused) | **real PostgreSQL 16** |
| SQL Server T-SQL generation and configuration validation | unit tests of exact strings |
| Real SQL Server / Azure SQL | **PENDING** |
| Databricks (Key Vault scope, Unity Catalog, job, drivers, service credential) | **PENDING** |
