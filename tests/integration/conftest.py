"""A REAL PostgreSQL server for integration tests (skipped when the binaries/driver are unavailable)."""

from __future__ import annotations

import os
import shutil
import subprocess
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest

PG_BIN = Path("/usr/lib/postgresql/16/bin")


@dataclass
class Pg:
    host: str
    port: int
    db: str
    user: str
    password: str
    sock: str

    def sql(self, statement: str) -> None:
        subprocess.run(["runuser", "-u", "postgres", "--", str(PG_BIN / "psql"), "-h", self.sock, "-p", str(self.port), "-d", self.db,
                        "-v", "ON_ERROR_STOP=1", "-q", "-c", statement], check=True, capture_output=True)  # fmt: skip


@pytest.fixture(scope="session")
def pg(spark):
    if not (PG_BIN / "initdb").exists() or os.geteuid() != 0:
        pytest.skip("PostgreSQL server binaries not available (or not root to switch to the postgres user)")
    if not getattr(spark, "pg_driver_available", False):
        pytest.skip("PostgreSQL JDBC driver could not be resolved")
    root = Path(f"/tmp/deaccel_pg_{uuid.uuid4().hex[:8]}")
    root.mkdir(mode=0o755)
    shutil.chown(root, "postgres", "postgres")
    port, pw = 55000 + os.getpid() % 900, "S3cretPw-for-tests-only"
    pwfile = root / "pw"
    pwfile.write_text(pw)
    shutil.chown(pwfile, "postgres", "postgres")
    run = lambda *a: subprocess.run(["runuser", "-u", "postgres", "--", *a], check=True, capture_output=True)  # noqa: E731
    run(str(PG_BIN / "initdb"), "-D", f"{root}/data", "-U", "postgres", "--auth-local=trust", "--auth-host=scram-sha-256", f"--pwfile={pwfile}")
    (root / "sock").mkdir()
    shutil.chown(root / "sock", "postgres", "postgres")
    run(str(PG_BIN / "pg_ctl"), "-D", f"{root}/data", "-o", f"-p {port} -k {root}/sock -c listen_addresses=127.0.0.1", "-l", f"{root}/log", "-w", "start")
    info = Pg("127.0.0.1", port, "erp", "acc_reader", "Reader-pw-9f3a", f"{root}/sock")
    subprocess.run(["runuser", "-u", "postgres", "--", str(PG_BIN / "createdb"), "-h", info.sock, "-p", str(port), "erp"], check=True, capture_output=True)
    info.sql(f"CREATE ROLE acc_reader LOGIN PASSWORD '{info.password}'")
    info.sql("GRANT CONNECT ON DATABASE erp TO acc_reader")
    yield info
    subprocess.run(["runuser", "-u", "postgres", "--", str(PG_BIN / "pg_ctl"), "-D", f"{root}/data", "-m", "immediate", "stop"], capture_output=True)
    shutil.rmtree(root, ignore_errors=True)
