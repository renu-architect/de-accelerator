# ADR-0019 — M2 source-to-Bronze ingestion: decisions requiring approval

**Status: PROPOSED (design review pending). Nothing is implemented.** Detail: `docs/architecture/m2/`. Each decision lists the alternative rejected and why.

| # | Decision | Alternatives considered | Rationale / consequence |
|---|---|---|---|
| D1 | **CR-M2-01:** additive migration `0002` — `cfg_load_strategy.read_options json`, `cfg_load_strategy.cdc_op_map json`; additive audit-table columns; file-tracking key `(object, path, mtime, size)`; `ConnectorValidator` hook + rules MDV-6001–6003 | Stuff options into existing json fields (`page_config`, `tags`, `cfg_source.options`) | Honest, typed place for connector settings; expand-only so existing packs/tests stay valid; touches M1 minimally. Requires re-pinning the migration checksum (approved by you) |
| D2 | Bronze preserves source names and native types (CSV default all-string), Delta column mapping `name`, `_`-prefixed audit columns, source columns may not start with `_` | Normalise names/types in Bronze | Raw preservation principle; cleansing belongs to Silver |
| D3 | M2 ships a minimal `IngestionRunner` (thread pool) + single-task Workflows job template; planner/DAG/`for_each`/provider in M4 | Build the planner now | Fastest path to real loads; `ingest_object()` is reused by M4 unchanged |
| D4 | CDC MVP = generic change feed (op+seq columns, incl. deletes) + SQL Server native CDC. Later: SQL Server Change Tracking, PostgreSQL logical replication/Debezium capture (Debezium *output files* can already be ingested as files) | Support all CDC mechanisms now | Keeps scope realistic; the normalised `I/U/D + seq` shape makes later adapters additive |
| D5 | File discovery by listing + manifest + file tracking; not Auto Loader | Auto Loader | Auto Loader's checkpoint is a second source of truth incompatible with the manifest/receipt protocol |
| D6 | REST: landing-first (verbatim pages in ADLS) + immutable `EXTRACTION_SEALED` event | In-memory pagination straight into Bronze | Durable replay without re-calling the API; pages are the preserved raw source |
| D7 | Azure SQL managed identity via Unity Catalog service credential (token), service-principal fallback | Static SQL logins only | No passwords in Azure; **Databricks-only, verification pending** |
| D8 | Bronze is append-only history; overlap duplicates (lookback, repeated snapshots) are kept; Silver dedupes | Dedupe in Bronze | Raw history and replay simplicity; avoids costly merges at ingest |
| D9 | CDC deletes are Bronze rows with `_op_type='D'`; no physical deletes in Bronze (erasure is a separate governed process) | Delete in Bronze | Preserves audit trail; Silver applies delete semantics |
| D10 | CI/dev agent with Docker for SQL Server/PostgreSQL integration tests; Databricks tests when a workspace exists | Mock-only | Real-source confidence; both tiers explicitly tracked as pending evidence |

## Refinements of doc 08 (no change to approved ADRs)
`batch_seq = max(seq)+1`; REST window sealed after landing; receipt may be verified by stats-skipped count; source cursor `epoch` for CDC resync.

## Evidence gathered during design
Local feasibility spike (Delta 3.2, throwaway script, not in repo): replaying an append with the same `txnAppId/txnVersion` is skipped; a stale lower version is skipped; skipped writes create no commit; history exposes `userMetadata` and `numOutputRows`; column-mapping tables accept names with spaces/dots. A complete example pack for 7 source types passes the M1 validator (DEV/QA/PROD, 0 errors/warnings) once the two proposed fields are removed. **Not yet verified:** any Databricks behaviour, any real SQL Server/PostgreSQL behaviour (no Docker daemon in the design sandbox).
