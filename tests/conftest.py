from __future__ import annotations

import pytest

from de_accelerator.metadata.store.memory import InMemoryTableStore


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from de_accelerator.metadata.store.spark import build_local_spark

    wh = tmp_path_factory.mktemp("warehouse")
    try:  # PostgreSQL JDBC driver for the integration tier (downloaded once); fall back to a plain session if unavailable
        s = build_local_spark(str(wh), extra_packages=["org.postgresql:postgresql:42.7.3"])
        s.pg_driver_available = True  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover
        s = build_local_spark(str(wh))
        s.pg_driver_available = False  # type: ignore[attr-defined]
    yield s
    s.stop()


@pytest.fixture(params=["memory", pytest.param("delta", marks=pytest.mark.component)])
def store(request):
    """Same contract tests run against the in-memory store and a real local Delta store."""
    if request.param == "memory":
        yield InMemoryTableStore()
        return
    from de_accelerator.metadata.store.spark import SparkDeltaTableStore

    spark = request.getfixturevalue("spark")
    yield SparkDeltaTableStore(spark, flavor="LOCAL")
    # tables are dropped per-test by the tests themselves (see `fresh_tables`)
