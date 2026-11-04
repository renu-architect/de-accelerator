# M2 closure-review probes (evidence, not product code)
Read-only scripts used during the M2 closure review (2026-11-04). They import the repo but change nothing in it.
Run from the repo root with the project venv, e.g. `.venv/bin/python docs/architecture/m2/review_probes/probe_repos_on_delta.py` (needs local Spark; ~2–4 min each).
| Script | What it demonstrates | Result on commit e0e3c27 |
|---|---|---|
| `probe_repos_on_delta.py` | each audit repository against the REAL Delta store | EventLog.append **FAIL** (writes 50 rows, then RuntimeError); LeaseRepo.acquire for another owner **FAIL** (naive/aware datetime TypeError); manifest/receipt/watermark/run-log PASS |
| `probe_delta_audit.py` | runner with Delta-backed audit tables, 4 objects | concurrency 1: **FAIL** (event_seq RuntimeError aborts the run); concurrency 4: Delta `ConcurrentAppendException` classified as a fatal LOGIC error |
| `probe_cli.py` | `ingest`, `batch-status`, `reprocess`, `abandon-batch` (in-memory audit store) | happy paths work; reprocess reuses a constant run id and creates no `run_pipeline` row |
**Historical (resolved 2026-11-04):** the defects shown above were fixed and the probes were converted into regular regression tests (`tests/component/test_ingestion_on_delta_audit.py`, `tests/unit/test_audit_delta_semantics.py`, `tests/component/test_cli_ingestion.py`). The scripts are kept only as evidence of what the closure review found.
