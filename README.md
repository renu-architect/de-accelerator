# DE Accelerator — core repository

Enterprise metadata-driven data engineering accelerator (Azure Databricks / Delta / Unity Catalog). One core codebase, one deployment repo and security boundary per client (not multi-tenant). Binding rules: `CLAUDE.md`.

## Project status (central overview)
Only `docs/status/IMPLEMENTATION_STATUS.md` states what is implemented. Evidence: `docs/status/TEST_EVIDENCE.md`. **No module is `PRODUCTION_READY`.**

| Milestone | Scope | Status |
|---|---|---|
| M1 | Metadata engine: YAML packs, validator, release-versioned Delta metadata, immutable snapshots, CLI | **CLOSED — `LOCAL_VERIFIED`** |
| M2 | Ingestion foundation: connector interface, JDBC connector (SQL Server, Azure SQL, PostgreSQL), batch protocol, Bronze Delta ingestion, runner, CLI | **CLOSED — `LOCAL_VERIFIED`** (398 tests, 95 % coverage, ruff + strict mypy clean, commit `51270e6`, branch `m2-ingestion`) |
| M3 | Bronze → Silver → Gold transformation (SCD1/SCD2, cleansing, dedupe, incremental, recovery) | **IN PROGRESS** on branch `m3-design` — design approved 2026-11-04 (`docs/architecture/m3/`, ADR-0020); not yet verified |
| M4+ | DQ/reconciliation, orchestration planner, semantic layer, documents | not started |

### What "closed as LOCAL_VERIFIED" means for M1/M2
Verified by automated tests on a developer/sandbox machine with local Spark + Delta and a real local PostgreSQL server. It does **not** mean production-ready.

**Pending external validation:** real SQL Server / Azure SQL execution · Databricks and Unity Catalog (Key Vault secret scope, JDBC drivers, runtime Delta behaviour, job template) · Azure DevOps pipeline execution · scale and performance.

**Future capabilities (not implemented):** schema discovery, file connectors (CSV/JSON/Parquet), REST, CDC, source deletes, service-principal / managed-identity auth, backfill chunking. Details: `docs/status/KNOWN_LIMITATIONS.md`.

## Local development
```bash
uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -e ".[spark,dev]"
.venv/bin/python -m pytest -m "not component"      # fast unit tests
.venv/bin/python -m pytest -m component            # local Spark + Delta tests (slow)
scripts/ci_test.sh                                 # lint + mypy + all tests (what CI runs)
```

## Try it on the synthetic example packs
```bash
de-accel validate-config metadata/examples/acme_pack
de-accel build-bundle metadata/examples/acme_pack --out /tmp/bundles
de-accel --bootstrap bootstrap.yaml migrate
de-accel --bootstrap bootstrap.yaml load-pack /tmp/bundles/bundle_DEV.json --activate --approved-by <name>
de-accel --bootstrap bootstrap.yaml ingest --execution <name>      # source -> Bronze (M2)
```
SQL Server → Bronze walkthrough (needs a reachable source): `docs/guides/QUICKSTART_SQLSERVER_TO_BRONZE.md`. All sample data is synthetic.

## Documentation map
Architecture `docs/architecture/` (M2 in `m2/`, M3 design in `m3/`) · decisions `docs/adr/` · module specs `docs/architecture/modules/` · status `docs/status/`.
