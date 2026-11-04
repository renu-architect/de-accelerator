# M2 — Metadata Configuration and YAML Examples

All examples are in `examples/ingest_examples_pack/` (a complete pack for client `acme`, synthetic). **Validation evidence:** with the two *proposed* fields (`read_options`, `cdc_op_map`, CR-M2-01) removed, the pack passes the existing M1 validator for DEV, QA and PROD with 0 errors and 0 warnings (the first draft was rejected by rule MDV-2005 — key column not declared — which is exactly the safety net the rules provide). The two proposed fields cannot be validated until CR-M2-01 is approved and implemented.

> **Closure note (2026-11-04):** the examples below are **design targets**. The validator currently **rejects** CDC, `SERVICE_PRINCIPAL`/`MANAGED_IDENTITY` auth, every `read_options` key, `cdc_op_map`, `late_arrival_window`, `partition_cols`, `write_mode` ≠ APPEND and `backfill_from/to` for JDBC sources, because none of them is implemented yet. Only SECRET-auth JDBC sources with FULL/SNAPSHOT/INCREMENTAL loads (and the file/REST sources, for which no connector exists yet) are accepted today. The runnable examples are `metadata/examples/sqlserver_demo_pack/` and `metadata/examples/acme_pack/`.

## 1. Which metadata drives what
| Need | Where (M1 entity.field) | Status |
|---|---|---|
| Source system, auth, secrets | `source.source_type, connector_type, auth_mode, connection{kv: refs}, options` | exists |
| What to read | `object.schema_name, object_name, object_kind, business_keys, partition_cols, target_bronze, enabled_layers, personal_data` | exists |
| Which columns / types | `column.*` (optional; discovery can generate) | exists |
| How to load | `load_strategy.load_type, watermark_column/type, lookback_window, late_arrival_window, partition_column, num_partitions, schema_evolution_mode, is_complete_extract, page_config, backfill_from/to` | exists |
| CDC safeguards | `load_strategy.cdc_op_column, cdc_seq_column, cdc_retention_check, max_replay_age, on_retention_gap` | exists (validated by rules 3002/3026/3027) |
| Connector-specific read settings (path/glob/format, endpoint/records path, change table, limits) | `load_strategy.read_options` | **new (CR-M2-01)** |
| CDC op code mapping | `load_strategy.cdc_op_map` | **new (CR-M2-01)** |
| Concurrency, retry, schedule | `execution.concurrency, retry_policy, schedule_*, timeout_min`; `retry_policy.*` | exists |
| Secret scope / catalog | `environment.secret_scope, catalog_name` | exists |
| Bronze target | `layer_mapping(layer=BRONZE).target_table` (must be `bronze.<table>`) | exists |

## 2. Reference: `read_options` keys by connector (proposed)
| Connector | Keys |
|---|---|
| JDBC | `change_table` (CDC feed), `capture_instance` (SQL Server CDC), `query_timeout_sec`, `max_window` (e.g. `P7D`, splits large initial loads into sequential batches), `max_batches_per_run`, `source_timezone` |
| File | `path`, `glob`, `format`, `format_options{}`, `schema_mode` (`STRING_ALL|INFER|DECLARED`), `max_files_per_batch`, `max_corrupt_pct` |
| REST | `endpoint`, `method`, `headers{kv refs only}`, `query{}`, `records_path`, `incremental_param`, `incremental_format`, `until_param`, `max_window` |
Unknown keys are rejected by the connector's `validate_config` (MDV-6xxx) so typos fail in CI.

## 3. Examples (7 source types)
### Sources
Key Vault references only; managed identity for ADLS and Azure SQL; OAuth2 secrets for REST.
```yaml
# sources/sources.yaml
kind: source
items:
  # 1) SQL Server over JDBC, user/password from Key Vault references (never literals)
  - code: erp_sql
    source_type: sqlserver
    connector_type: jdbc
    auth_mode: SECRET
    connection: {host: "kv:erp-host", port: "1433", database: "kv:erp-db", user: "kv:erp-user", password: "kv:erp-pwd"}
    options: {fetchsize: 10000, max_connections: 4, encrypt: true}
  # 2) Azure SQL using a Unity Catalog service credential backed by a managed identity (no password anywhere)
  - code: crm_azsql
    source_type: azuresql
    connector_type: jdbc
    auth_mode: MANAGED_IDENTITY
    connection: {host: "kv:crm-host", database: "kv:crm-db", service_credential: "kv:crm-svc-cred-name"}
    options: {fetchsize: 10000, max_connections: 4}
  # 3) PostgreSQL
  - code: web_pg
    source_type: postgresql
    connector_type: jdbc
    auth_mode: SECRET
    connection: {host: "kv:web-host", port: "5432", database: "kv:web-db", user: "kv:web-user", password: "kv:web-pwd"}
    options: {fetchsize: 20000, max_connections: 2}
  # 4-6) File sources in ADLS Gen2 (access via Unity Catalog external location / managed identity: no keys)
  - {code: land_csv,     source_type: csv,     connector_type: file, auth_mode: MANAGED_IDENTITY, connection: {root: "abfss://landing@acmedev.dfs.core.windows.net/csv"}}
  - {code: land_json,    source_type: json,    connector_type: file, auth_mode: MANAGED_IDENTITY, connection: {root: "abfss://landing@acmedev.dfs.core.windows.net/json"}}
  - {code: land_parquet, source_type: parquet, connector_type: file, auth_mode: MANAGED_IDENTITY, connection: {root: "abfss://landing@acmedev.dfs.core.windows.net/parquet"}}
  # 7) REST API with OAuth2 client credentials from Key Vault
  - code: shop_api
    source_type: rest
    connector_type: rest
    auth_mode: SERVICE_PRINCIPAL
    connection: {base_url: "kv:shop-api-url", token_url: "kv:shop-token-url", client_id: "kv:shop-client-id", client_secret: "kv:shop-client-secret"}
    options: {timeout_sec: 30, max_requests_per_sec: 5}
```
### Objects

```yaml
# objects/objects.yaml
kind: object
items:
  - {code: orders,        source: erp_sql,   schema_name: dbo,    object_name: Orders,      business_keys: [OrderID],    enabled_layers: [BRONZE], target_bronze: bronze.erp_orders}
  - {code: customers,     source: crm_azsql, schema_name: dbo,    object_name: Customers,   business_keys: [CustomerID], enabled_layers: [BRONZE], target_bronze: bronze.crm_customers, personal_data: true, sensitivity: confidential}
  - {code: sessions,      source: web_pg,    schema_name: public, object_name: web_sessions, business_keys: [session_id], enabled_layers: [BRONZE], target_bronze: bronze.web_sessions}
  - {code: products_csv,  source: land_csv,  object_name: products,   object_kind: file, business_keys: [sku],        enabled_layers: [BRONZE], target_bronze: bronze.csv_products}
  - {code: events_json,   source: land_json, object_name: events,     object_kind: file, business_keys: [event_id],   enabled_layers: [BRONZE], target_bronze: bronze.json_events}
  - {code: prices_parquet, source: land_parquet, object_name: prices, object_kind: file, business_keys: [sku],        enabled_layers: [BRONZE], target_bronze: bronze.parquet_prices}
  - {code: shop_orders,   source: shop_api,  object_name: orders,     object_kind: api,  business_keys: [id],         enabled_layers: [BRONZE], target_bronze: bronze.shop_orders}
```
### Load strategies
SQL Server incremental · Azure SQL full · PostgreSQL CDC feed · CSV/JSON file-incremental · Parquet snapshot · REST cursor pagination.
```yaml
# load/load_strategies.yaml
kind: load_strategy
items:
  # INCREMENTAL on a timestamp with 2h re-read overlap and 5 min safety lag against in-flight commits
  - object: orders
    load_type: INCREMENTAL
    watermark_column: ModifiedDate
    watermark_type: timestamp
    lookback_window: PT2H
    late_arrival_window: PT5M
    partition_column: OrderID
    num_partitions: 8
    schema_evolution_mode: ADD_COLUMNS
    # --- proposed by M2 (CR-M2-01, additive): connector-specific read options ---
    read_options: {}
  # FULL load, complete extract
  - {object: customers, load_type: FULL, is_complete_extract: true, schema_evolution_mode: ADD_COLUMNS}
  # CDC from a change-feed table (op + sequence columns); deletes arrive as op 'D'
  - object: sessions
    load_type: CDC
    cdc_op_column: op_code
    cdc_seq_column: change_seq
    max_replay_age: P7D
    on_retention_gap: FULL_RESYNC
    cdc_retention_check: true
    # --- proposed by M2: map source op codes to I/U/D ---
    cdc_op_map: {I: [insert, c], U: [update, u], D: [delete, d]}
    read_options: {change_table: public.web_sessions_changes}
  # FILE_INCR: new/changed CSV files since last batch
  - object: products_csv
    load_type: FILE_INCR
    lookback_window: PT1H
    schema_evolution_mode: ADD_COLUMNS
    read_options:
      path: products/
      glob: "*.csv"
      format: csv
      format_options: {header: true, delimiter: ",", encoding: UTF-8, multiLine: false}
      max_files_per_batch: 500
      max_corrupt_pct: 0.1
  - object: events_json
    load_type: FILE_INCR
    schema_evolution_mode: RESCUE
    read_options: {path: events/, glob: "*.json", format: json, format_options: {multiLine: false}, max_files_per_batch: 1000}
  # SNAPSHOT of parquet files: every run lands a full copy
  - object: prices_parquet
    load_type: SNAPSHOT
    is_complete_extract: true
    read_options: {path: prices/, glob: "*.parquet", format: parquet}
  # API with cursor pagination and an incremental 'since' parameter
  - object: shop_orders
    load_type: API
    watermark_column: updated_at
    watermark_type: timestamp
    lookback_window: PT1H
    page_config: {pagination: cursor, page_size: 200, cursor_param: cursor, next_cursor_path: "$.meta.next", max_pages: 5000}
    read_options:
      endpoint: /v2/orders
      method: GET
      records_path: "$.data"
      incremental_param: updated_since
      incremental_format: iso8601
```
### Bronze layer mappings

```yaml
# load/layer_mappings.yaml
kind: layer_mapping
items:
  - {object: orders,         layer: BRONZE, target_table: bronze.erp_orders,      merge_type: APPEND}
  - {object: customers,      layer: BRONZE, target_table: bronze.crm_customers,   merge_type: APPEND}
  - {object: sessions,       layer: BRONZE, target_table: bronze.web_sessions,    merge_type: APPEND}
  - {object: products_csv,   layer: BRONZE, target_table: bronze.csv_products,    merge_type: APPEND}
  - {object: events_json,    layer: BRONZE, target_table: bronze.json_events,     merge_type: APPEND}
  - {object: prices_parquet, layer: BRONZE, target_table: bronze.parquet_prices,  merge_type: APPEND}
  - {object: shop_orders,    layer: BRONZE, target_table: bronze.shop_orders,     merge_type: APPEND}
```
### Execution

```yaml
# orchestration/executions.yaml
kind: execution
items:
  - code: ingest_all
    scope: {objects: [orders, customers, sessions, products_csv, events_json, prices_parquet, shop_orders]}
    schedule_cron: "0 0 * * * ?"
    schedule_timezone: UTC
    concurrency: 4
    retry_policy: standard
    timeout_min: 180
```

### SQL Server native CDC variant (instead of the generic feed)
```yaml
- object: orders
  load_type: CDC
  cdc_retention_check: true
  max_replay_age: P3D
  on_retention_gap: FULL_RESYNC
  read_options: {capture_instance: dbo_Orders}     # op/seq columns are implied by native CDC
  cdc_op_map: {I: [2], U: [4], D: [1]}              # __$operation values; 3 (before image) ignored
```
### Environment overrides (resolved at pack load — no runtime logic)
```yaml
# overrides/PROD.yaml
overrides:
  - {kind: source, project: ingest, key: {code: erp_sql}, set: {connection: {host: "kv:erp-host-prod", database: "kv:erp-db-prod"}, options: {max_connections: 8}}}
  - {kind: load_strategy, project: ingest, key: {object: orders}, set: {num_partitions: 32}}
```
