# M2 — Connector Interface Specifications and Plugin Architecture

Specifications only (signatures are contracts, not implementation). Python 3.10+, typed; all inputs are immutable M1 typed models (`SourceConfig`, `ObjectConfig`, `LoadStrategyConfig`, `ColumnConfig`, `EnvironmentConfig`).

## 1. Plugin architecture
- **Registry key = `source_type`** (M1 enum: `sqlserver, azuresql, postgresql, csv, json, parquet, rest, mock`, …). A connector class declares the `source_type`s it serves (one JDBC class serves three).
- **Discovery:** Python entry-point group `de_accelerator.connectors` (built-ins registered in the wheel; client extensions in the client deployment repo register the same way). `core.registry.Registry` (M1) is reused; duplicate keys fail at startup.
- **Extension rule (unchanged, ADR-0008):** an extension must implement this interface, pass the **connector contract test suite** (`tests/contract/connector_contract.py`), and never patch core.
- **Lifecycle:** one `Connector` instance per (source, run); constructed with a `ConnectorContext`; thread-safe for concurrent `plan`/`extract` of different objects of the same source, bounded by `max_connections`.

```
ConnectorContext:   source: SourceConfig; env: EnvironmentConfig; secrets: SecretProvider;
                    spark: SparkSession; run_id: str; logger; clock: Clock (UTC, injectable for tests);
                    landing_root: str | None            # ADLS path for connectors that must land raw files (REST)
Capabilities:       full, incremental, cdc, snapshot, file_incremental, pagination,
                    discovery, profiling, count_pushdown, partitioned_read   # booleans; contract tests verify they are truthful
```

## 2. The interface
```
class Connector(Protocol):
    source_types: ClassVar[tuple[str, ...]]
    capabilities: Capabilities

    # --- configuration & connectivity -------------------------------------------------
    def validate_config(self, source: SourceConfig, objects: Sequence[ObjectBinding]) -> list[Finding]:
        """Static checks used by CI (hook into M1 validator, codes MDV-6xxx). No network, no secrets."""
    def test_connection(self) -> TestResult:
        """Authenticate and run a trivial read-only probe. Never logs secrets."""

    # --- discovery (read-only; used by `de-accel discover`) ---------------------------
    def discover(self, scope: DiscoveryScope) -> list[DiscoveredObject]
    def get_schema(self, obj: ObjectBinding) -> SourceSchema          # columns, native types, nullability, PK if known
    def profile(self, obj: ObjectBinding, sample: ProfileSpec) -> list[ColumnProfile]   # aggregate stats only

    # --- extraction (two-phase; the phases are what make replay deterministic) --------
    def plan(self, req: PlanRequest) -> ExtractPlan
        """CHEAP and side-effect free. Resolves everything that must be frozen in the manifest:
           the exact window (wm_from, wm_to), file list, CDC LSN range, partition bounds, query hash.
           Same inputs + same source state => same plan. May query the source (e.g. MAX(watermark), file listing)."""
    def extract(self, plan: ExtractPlan, ctx: ExtractContext) -> ExtractResult
        """Executes exactly the frozen plan. Returns a (lazy) Spark DataFrame of RAW source rows plus
           per-row source metadata columns (_source_file, _source_file_mod_ts, _op_type, _op_seq).
           Must be repeatable: running it twice yields the same row set barring concurrent source change."""
    def count(self, plan: ExtractPlan) -> int | None            # pushdown count for the window (verification); None if unsupported
    def close(self) -> None
```
### Data types
```
PlanRequest:   obj: ObjectBinding; strategy: LoadStrategyConfig; stream_key: str; batch_type: LIVE|BACKFILL|REPROCESS|RESYNC;
               committed: WatermarkState | None   # (value, seq, epoch) — the last COMMITTED source cursor; None = first run
               window_override: (from, to) | None # backfill / reprocess
ExtractPlan:   wm_from: str | None; wm_to: str | None; lookback: str | None;
               file_list: list[FileRef(path, size, mtime, etag?)] | None;
               plan_json: dict      # connector-specific frozen facts, secret-free (SQL hash, partition bounds, capture instance, API params)
               plan_hash: str       # sha256(canonical_json(plan)) — goes into manifest_hash
               empty: bool          # nothing to read (wm_from == wm_to, no new files) => batch completes with 0 rows
ExtractResult: df: DataFrame; source_schema: SourceSchema; stats: dict   # expected_rows (if pushdown), pages, files
ObjectBinding: source: SourceConfig; obj: ObjectConfig; columns: list[ColumnConfig]; strategy: LoadStrategyConfig
```
### Contract rules every connector must satisfy (enforced by the shared contract tests)
1. `plan()` is deterministic and side-effect free; its output contains **no secrets**.
2. `extract(plan)` never reads outside the plan window/file list (property test: seeded source with rows outside the window).
3. Declared capabilities are truthful (a connector claiming `cdc` must pass the CDC scenario suite; otherwise the engine refuses the load type with a CONFIG error).
4. Errors are raised as `AcceleratorError` subclasses with a correct `error_class` (TRANSIENT/DATA/CONFIG/PLATFORM) — the retry engine relies on it.
5. No writes to the source, ever. Credentials come only from `ctx.secrets`.
6. `discover/get_schema/profile` never return data values (only names, types, aggregate statistics; min/max suppressed for PII-suspect columns).

## 3. Built-in connectors (MVP)
| Connector | Serves `source_type` | Load types | Notes |
|---|---|---|---|
| `JdbcConnector` (+ dialects) | `sqlserver`, `azuresql`, `postgresql` | FULL, INCREMENTAL, SNAPSHOT, CDC (feed; SQL Server native) | Spark JDBC with partitioned reads; dialect classes own quoting, type mapping, bounds queries, CDC queries |
| `FileConnector` | `csv`, `json`, `parquet` | FILE_INCR, SNAPSHOT, FULL | Listing via Hadoop FS API on `abfss://`/Volumes/local paths; reads with Spark readers |
| `RestConnector` | `rest` | API (incremental via `since` param), FULL | Driver-side HTTP client with auth, paging, retry, rate limit; lands pages to ADLS before Bronze |
| `MockConnector` | `mock` | all | Deterministic in-memory/temp-file source with fault hooks; used by unit/crash tests and local demos |

### 3.1 JDBC specifics
- **Connection:** from `source.connection` keys (`host, port, database, user, password` | `service_credential`), all `kv:` references resolved at runtime via `SecretProvider`. Options (`source.options`): `fetchsize`, `max_connections`, `encrypt`, `trust_server_certificate` (default false), `query_timeout_sec`, `isolation_level` (default `READ_COMMITTED`; `NOLOCK` hints are **never** generated unless explicitly configured).
- **Query generation:** only from metadata; identifiers must match `^[A-Za-z_][A-Za-z0-9_ ]*$` and are quoted by the dialect (`[x]`, `"x"`); values are typed literals produced by the dialect (never string-concatenated user text). Window: `wm > :lo AND wm <= :hi` where `lo = wm_from − lookback`. Column list from `cfg_column` when declared, else `*`.
- **Partitioned read:** `partition_column`/`num_partitions` (M1 fields) with bounds from a `MIN/MAX` query **restricted to the window** and frozen in `plan_json`; skew mitigation is a tuning concern, not correctness.
- **Watermark values** are carried as the *source's own string representation* (not Spark-converted) to avoid precision loss (e.g. `datetime2(7)` vs Spark microseconds).
- **SQL Server safe upper bound:** for `rowversion` watermarks `wm_to = MIN_ACTIVE_ROWVERSION() − 1` (never `MAX`), so uncommitted transactions with lower rowversions cannot be skipped. For timestamp watermarks `wm_to = min(MAX(col), source_now − late_arrival_window)`.
- **SQL Server native CDC:** `wm` = LSN (hex). `plan` computes `hi = sys.fn_cdc_get_max_lsn()` and **checks retention** (`sys.fn_cdc_get_min_lsn(capture_instance) ≤ committed LSN`, else gap policy); `extract` reads `cdc.fn_cdc_get_all_changes_<instance>(lo, hi, 'all update old')`, maps `__$operation` 1→D, 2→I, 4→U (type 3 "before image" dropped by default), `_op_seq = __$start_lsn || __$seqval`.
- **Generic change feed (any JDBC table or landed files):** `cdc_op_column` + `cdc_seq_column` + `cdc_op_map` + `read_options.change_table`; reads `seq > lo AND seq <= hi`; `hi = MAX(seq)` frozen at plan time.
- **Type mapping:** Spark JDBC defaults + `customSchema` for exact `decimal(p,s)`; `rowversion/binary` kept as binary; unsupported types fail loudly (DATA error) rather than silently stringifying.
- **Azure SQL / managed identity:** `auth_mode: MANAGED_IDENTITY` → token via a **Unity Catalog service credential** (`connection.service_credential`) passed as access token; `SERVICE_PRINCIPAL` fallback (client id/secret/tenant from `kv:` refs). *Databricks-only; pending verification (G14).*

### 3.2 File specifics (CSV / JSON / Parquet)
- `plan` lists `connection.root / read_options.path` matching `glob`, orders by `(mtime, path)`, excludes files already `DONE` in `run_file_tracking` for the same `(path, mtime, size)`, applies `lookback` only as a *listing optimisation* (tracking is the correctness mechanism), caps at `max_files_per_batch`, and freezes `file_list` (path, size, mtime, etag when available).
- `extract` reads exactly the frozen list; `_source_file` and `_source_file_mod_ts` come from Spark `_metadata`. Never modifies, moves or deletes source files.
- **CSV:** default `schema_mode: STRING_ALL` (raw preservation: leading zeros, no parse loss); options: `header, delimiter, quote, escape, encoding, multiLine, nullValue`. **JSON:** `INFER` default; nested structures preserved; `multiLine` option. **Parquet:** native schema. `DECLARED` mode uses `cfg_column.source_type`.
- **Corrupt records:** `PERMISSIVE` + corrupt column (CSV/JSON) or `_rescued_data`; batch fails when corrupt ratio > `max_corrupt_pct` (default 0 for Parquet, 0.1% CSV/JSON), counted in the same write pass (Spark `Observation`).
- **Why not Auto Loader:** its checkpoint is a second source of truth that cannot be reconciled with our manifest/receipt protocol (D5). Trade-off: listing cost on very large directories → mitigated by date-partitioned paths + mtime pre-filter; Auto Loader remains an optional later optimisation.

### 3.3 REST specifics
- **Auth:** `bearer` (kv token), `api_key` (header/query), `oauth2_client_credentials` (token_url/client_id/client_secret/scope from `kv:` refs, token cached until 60 s before expiry, refreshed on 401 once).
- **Pagination types** (`page_config.pagination`): `offset` (offset+limit params), `page` (page number+size), `cursor` (`cursor_param`, `next_cursor_path`), `link_header` (RFC 5988 `rel="next"`), `next_url` (absolute URL in body path). `page_size`, `max_pages` (hard stop, fails batch if exceeded rather than silently truncating).
- **Incremental:** `read_options.incremental_param` receives `wm_from − lookback` formatted per `incremental_format` (`iso8601`, `epoch_s`, `epoch_ms`); optional `until_param` receives the frozen `wm_to` (run start − `late_arrival_window`) when the API supports it.
- **Landing-first extraction:** every page response body is written verbatim to `landing_root/<source>/<object>/<batch_id>/page_<n>.json` (+ `_progress.json` after each page; `_SUCCESS` with page count, record count and sha256 list when complete). Bronze then reads the landed files — so Bronze never depends on re-calling the API, and the landed pages are the preserved raw source (D6). Resume after crash continues from `_progress.json`; if `_SUCCESS` exists the fetch is skipped entirely.
- **Sealed extraction:** after `_SUCCESS`, the engine appends an `EXTRACTION_SEALED` event (checksum, record count, `wm_to_observed = max(record[watermark_column])`) — this is the immutable extraction fact used by Bronze commit and the watermark (refinement G7).
- **Reliability:** retries on 429/5xx with `Retry-After`, exponential backoff + jitter, request timeout, `max_requests_per_sec` token bucket; 4xx other than 401/429 → DATA/CONFIG error (not retried).
- `records_path`: restricted JSONPath subset (`$.a.b`, `$.a[*]`) — no arbitrary expression evaluation.

## 4. Discovery and metadata auto-generation (`de-accel discover`)
Inputs: `--source <code> [--schema s] [--include pattern] --out <pack dir>`. Process: `test_connection` → `discover` → `get_schema` → (optional) `profile` on a bounded sample → **draft pack** written to a *new directory* as YAML. Never writes Delta, never overwrites an existing pack.
| Suggestion | Heuristic |
|---|---|
| Objects/columns | catalog metadata (`INFORMATION_SCHEMA`) / file schema inference / REST sample response |
| Keys | declared PK/unique constraints; else candidate columns named `id`/`*_id` flagged `# REVIEW` |
| Load strategy | INCREMENTAL if a `rowversion`/`modified*/updated*/last_*` datetime column exists; CDC-capable table → CDC suggestion comment; files → FILE_INCR; else FULL |
| Watermark column/type | best candidate by name + type |
| PII | column-name patterns (email, phone, name, address, dob, ssn-like names…) → `is_pii: true`, `classification: confidential`, `personal_data: true` on the object, `# REVIEW` |
| DQ | `NOT_NULL` on keys, `UNIQUE` on declared PK, `FRESHNESS` on watermark |
| Profiling | null %, distinct count estimate, min/max **only for non-PII numeric/date columns**; no sample values anywhere (org policy: no personal data in metadata) |
Every generated file starts with `# AUTO-GENERATED DRAFT — review before merge`; the generator runs the M1 validator on its own output and reports findings. Human review in a Git PR remains mandatory (ADR-0002); AI-assisted refinement is optional and never automatic.
