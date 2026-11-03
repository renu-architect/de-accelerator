#!/usr/bin/env python3
"""Run pytest for a module and append a truthful evidence entry to docs/status/TEST_EVIDENCE.md.

usage: record_evidence.py "<module>" "<pytest args...>"
"""
import datetime
import re
import subprocess
import sys
import sysconfig  # noqa: F401

module, args = sys.argv[1], sys.argv[2:]
cmd = [sys.executable, "-m", "pytest", "-q", *args]
res = subprocess.run(cmd, capture_output=True, text=True)
tail = (res.stdout.strip().splitlines() or [""])[-1]
py = f"Python {sys.version.split()[0]}"
try:
    import pyspark
    import delta  # noqa: F401
    env = f"{py}, pyspark {pyspark.__version__}, delta-spark (local)"
except Exception:
    env = py
status = "PASS" if res.returncode == 0 else "FAIL"
entry = (f"\n### {module} — {status} — {datetime.date.today()}\n- Command: `{' '.join(['pytest', '-q', *args])}`\n"
         f"- Environment: {env}; local only (no Databricks)\n- Result: `{tail}`\n")
open("docs/status/TEST_EVIDENCE.md", "a").write(entry)
print(entry)
sys.exit(res.returncode)
