#!/usr/bin/env bash
# Portable CI entrypoint (used by Azure DevOps; also runnable locally / on other CI systems).
set -euo pipefail
PY="${PYTHON:-python}"
$PY -m ruff check src tests
$PY -m mypy
$PY -m pytest -m "not databricks" --cov=de_accelerator --cov-report=term-missing --junitxml=test-results.xml "$@"
