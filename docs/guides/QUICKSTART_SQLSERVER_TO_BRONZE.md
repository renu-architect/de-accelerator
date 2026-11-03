# Quickstart — SQL Server table → Bronze Delta table

Status: the engine, the SQL Server dialect and the CLI are implemented and tested **locally** (mock source, real local Delta, and a real
PostgreSQL server through the same JDBC connector). **Not yet run against a real SQL Server or on Databricks** — see "What is verified".

## The developer workflow (5 steps)
1. **Configure** the table in YAML (`metadata/examples/sqlserver_demo_pack/` — one source, one object, one load strategy, one Bronze mapping).
   Secrets are Key Vault references (`kv:erp-pwd`), never values. Adding tables = adding items to `objects.yaml` / `load_strategies.yaml` /
   `layer_mappings.yaml`; no per-table code or script.
2. **Validate:** `de-accel validate-config metadata/examples/sqlserver_demo_pack` (64 rules: references, load-strategy consistency, secrets,
   SQL-identifier safety, connector options).
3. **Activate:** `de-accel --bootstrap bootstrap/dev.yaml migrate` (once), then `de-accel --bootstrap bootstrap/dev.yaml load-pack <pack> --activate`
   (stages an immutable release and flips the active pointer; `rollback` flips it back).
4. **Run:** `de-accel --bootstrap bootstrap/dev.yaml ingest --execution daily_orders` (locally or as the Databricks job in `deployment/databricks/databricks.yml`).
5. **Monitor:** `de-accel --bootstrap bootstrap/dev.yaml batch-status --object orders` and the audit tables
   (`audit.run_pipeline`, `run_object`, `run_batch_manifest`, `run_batch_event`, `run_target_receipt`, `wm_watermark`, `run_error`).
   Records are in `<catalog>.bronze.erp_orders` with the standard `_batch_id`, `_execution_id`, `_ingest_ts`, `_op_type`, … audit columns.

## Databricks prerequisites (one-time, per environment)
- Key Vault-backed secret scope named as `environment.secret_scope` in the pack (secrets: `erp-host`, `erp-db`, `erp-user`, `erp-pwd`).
- Unity Catalog catalog `<client>_<env>` with schemas `meta, audit, dq, recon, gov, bronze, silver, gold`; the job's service principal needs
  `USE/CREATE/MODIFY` on them (never write access to `meta.cfg_*` except via `load-pack`).
- Network path from the cluster to SQL Server; a read-only database user.
- Build and deploy: `uv build --wheel` then `databricks bundle deploy` (see `deployment/databricks/databricks.yml`; bootstrap YAML example below).
```yaml
# bootstrap/dev.yaml
client_code: acme
environment: DEV
flavor: DATABRICKS        # LOCAL for laptop runs (needs warehouse_dir)
secret_scope: kv-acme-dev
```

## Run it locally without any database (smoke test)
`pytest tests/component/test_ingestion_engine.py` (mock source, real local Delta) or, with the local PostgreSQL server available in the sandbox,
`pytest tests/integration` (real PostgreSQL → Bronze through the JDBC connector).

## What is verified (and what is not)
| Claim | Evidence |
|---|---|
| YAML → validate → release → snapshot → engine → Bronze Delta, incl. crash recovery and no duplicates | local tests (mock + real local Delta) |
| Real relational source via JDBC → Bronze (full, incremental with UPDATEs/lookback, partitioned reads, auth failure, connection refused) | **real PostgreSQL 16 server**, local |
| SQL Server T-SQL generation (quoting, typed literals, rowversion bound, TLS URL) | unit tests of exact SQL strings |
| Execution against a **real SQL Server / Azure SQL** | **PENDING** (no SQL Server in the sandbox; Docker unavailable) |
| Execution on **Databricks** (Key Vault scope, Unity Catalog, job, driver availability, managed identity) | **PENDING** (no workspace) |
