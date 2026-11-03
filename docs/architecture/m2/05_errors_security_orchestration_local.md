# M2 — Error Handling and Retry, Security, Workflows Orchestration, Local Development

## 1. Error classification (reuses M1 `AcceleratorError.error_class`)
| Class | Typical causes | Retried? | Batch outcome |
|---|---|---|---|
| TRANSIENT | network reset, JDBC connection loss, HTTP 429/500/502/503/504, Delta `ConcurrentAppendException`/commit conflict, cluster/executor loss | yes (policy) | `FAILED_RETRYABLE` → resumed |
| DATA | schema change under `FAIL`, incompatible type, corrupt ratio above `max_corrupt_pct`, reserved column name, `max_pages` exceeded, unsupported source type | no | `FAILED_FATAL` |
| CONFIG | missing/invalid metadata, unknown connector, missing secret, bad `read_options`, capability not supported for load type | no | `FAILED_FATAL` (usually caught earlier by CI validation) |
| PLATFORM | 401/403, login failed, UC permission denied, Key Vault access denied | no | `FAILED_FATAL` + page on-call |
| LOGIC | unexpected exception | no | `FAILED_FATAL` |
Exception mapping lives in each connector (e.g., JDBC `SQLTransientConnectionException`/SQL state classes → TRANSIENT; auth SQL states → PLATFORM; HTTP status → class). Unknown exceptions default to LOGIC (never silently retried).

## 2. Retry strategy
- Policy from M1 `cfg_retry_policy` via `cfg_execution.retry_policy`: `max_attempts`, `backoff_sec`, `backoff_mult`, `retryable_error_classes` (default `[TRANSIENT]`). Jitter ±20 %.
- **A retry is a recovery, not a restart:** each attempt first runs `recover()` (doc 04 §6), so work already durable (landed pages, committed Bronze rows, receipts) is never repeated. Attempts are recorded in `run_attempt` (attempt no., error, timing, reason).
- Exhausted retries → `FAILED_FATAL`-equivalent for this run; the batch stays open and is resumed by the next run or by `ingest --restart`. Other objects continue (failure isolation: nothing is shared except the per-source connection cap).
- Per-file poison handling: a file failing `max_attempts_per_file` (default 3) across runs is marked `QUARANTINED` in `run_file_tracking`, skipped with an alert; **source files are never moved or modified**.
- Optional (not MVP): per-source circuit breaker — after K consecutive TRANSIENT failures across objects, pause the source for `circuit_open_sec`.
- Timeouts: `cfg_execution.timeout_min` bounds the run; connectors set per-request/query timeouts (`query_timeout_sec`, REST `timeout_sec`).
- Reprocessing / manual levers (audited, CLI): `ingest --restart <batch>`, `abandon-batch <batch> --reason`, `reprocess …`, `rewind-watermark … --reason` (admin only).

## 3. Security (Key Vault, Managed Identity, least privilege)
- **Secrets:** metadata holds only `kv:<name>` references (M1 rules MDV-4001–4003). `SecretProvider.get(name)` resolves with `scope = environment.secret_scope` using `dbutils.secrets.get` (Key Vault-backed scope) on Databricks. Resolved values live in memory only: never written to manifests, events, logs (redaction already in M1 `core.logging`), landing files or exceptions.
- **Local development provider:** `EnvSecretProvider` reads `DEACCEL_SECRET_<NAME>`; it **refuses to initialise when `environment == PROD`** and logs a warning in QA.
- **Managed identity / no static keys wherever possible:** ADLS (landing, raw sources, Delta storage) through Unity Catalog external locations / volumes backed by the workspace Access Connector (managed identity) — `auth_mode: MANAGED_IDENTITY`, no storage keys. Azure SQL via **UC service credential** (managed identity) to obtain an Entra access token (`connection.service_credential`), fallback `SERVICE_PRINCIPAL` (client id/secret/tenant in Key Vault). SQL Server/PostgreSQL: user/password from Key Vault, TLS enforced (`encrypt=true`, `trust_server_certificate=false` by default).
- **Identities and grants (applied by IaC in M6, verified in Databricks integration tests):** job runs as a service principal (`run_as`) with: `SELECT` on the snapshot/metadata views it needs, `INSERT/UPDATE` only on the `audit` tables it owns (`wm_watermark` update, manifest insert), `CREATE TABLE`/`MODIFY` on `bronze`, read on the source external locations; **no** write access to `meta.cfg_*`. Source credentials are read-only database users.
- **Data protection:** `personal_data` objects keep Bronze under `gov` policy (retention/erasure, doc 11); landing and quarantine locations inherit ADLS encryption and ACLs; discovery/profiling never emits sample values; logs and events contain counts/ids only.
- **Network:** private endpoints / VNet injection are infrastructure (M6); the connector assumes the network path exists and reports PLATFORM errors otherwise.
- **Supply chain:** JDBC drivers must be present in the cluster runtime (SQL Server and PostgreSQL drivers are expected on Databricks runtimes — *verification pending*); no drivers are downloaded at runtime.

## 4. Databricks Workflows orchestration (M2 slice)
**M2 delivers:** (a) `de-accel ingest --execution <code> [--objects a,b] [--restart]` as a python-wheel task entry point; (b) a **job template** (Databricks Asset Bundle YAML, shipped as a template file, rendered per client) — design shown below; (c) run id propagation: `run_id = DATABRICKS_JOB_RUN_ID` when present, else a generated uuid.
```yaml
# deployment/workflows/ingest_job.template.yml  (design; NOT implemented)
resources:
  jobs:
    ingest_${execution_code}:
      name: ${client}-${env}-ingest-${execution_code}
      max_concurrent_runs: 1                    # no overlapping runs of the same execution
      schedule: {quartz_cron_expression: "${cron}", timezone_id: "${tz}"}
      tags: {client: ${client}, project: ${project}, pipeline: ${execution_code}}   # cost tracking
      tasks:
        - task_key: ingest
          python_wheel_task: {package_name: de_accelerator, entry_point: de-accel,
                              parameters: ["--bootstrap", "${bootstrap_path}", "ingest", "--execution", "${execution_code}"]}
          job_cluster_key: ingest_cluster
          max_retries: 0                        # retries are owned by the framework (recovery-aware)
          timeout_seconds: ${timeout_s}
      job_clusters: [{job_cluster_key: ingest_cluster, new_cluster: {spark_version: "${dbr}", node_type_id: "${node}", autoscale: {min_workers: 1, max_workers: ${max_workers}}}}]
      run_as: {service_principal_name: ${sp}}
```
- **Why one task + thread pool in M2:** simplest thing that runs real loads; objects run concurrently (`cfg_execution.concurrency`) with per-source caps; recovery makes "Repair run" safe because a re-run resumes open batches. 
- **M4 evolution (not M2):** planner builds the DAG from `cfg_pipeline_dependency`; `for_each` fans out `ingest_object`; `OrchestrationProvider` abstraction. `ingest_object()` is already the unit of work, so M2 code is reused unchanged.
- Scheduling, parameters, alerts on failure and cluster profile are job settings rendered from `cfg_execution`; nothing client-specific in core.

## 5. Local development and mock testing
| Need | Approach |
|---|---|
| Run without Databricks | `flavor: LOCAL` bootstrap (M1) → local Spark + Delta; Bronze at `bronze.<table>` in the local warehouse |
| Source without infrastructure | `MockConnector` (`source_type: mock`, already in the M1 enum): programmable tables, watermark data, CDC feeds with ops, file drops, schema drift, injected failures, **read counters** (for invariant I5) |
| Files | temp directories with CSV/JSON/Parquet generated by fixtures; file re-delivery scenarios |
| REST | in-process HTTP server fixture (pagination styles, 429/5xx, auth token expiry, mid-pagination crash) |
| SQL Server / PostgreSQL | Testcontainers/Docker tier (`integration` marker) with seeded schemas incl. CDC enablement; **not runnable in this sandbox** (no Docker daemon) → runs on a dev machine/CI agent; tracked as pending evidence |
| Secrets | `EnvSecretProvider` (blocked in PROD) |
| Time | injectable `Clock` for deterministic watermark/lookback tests |
One-command flows: `pytest -m "not component and not integration"` (fast) · `pytest -m component` (local Spark/Delta, crash matrix) · `pytest -m integration` (Docker) · `pytest -m databricks` (pending, workspace needed).
