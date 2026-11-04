"""A4 + B3: audit-store trouble on the failure path must not abort a run; written rows must equal the Delta commit."""

import pytest

from de_accelerator.core.errors import DataError, StoreError
from de_accelerator.ingestion.engine import ObjectResult
from de_accelerator.processing.bronze.writer import Verification
from tests.ingest_util import DDL, build_world, rows

pytestmark = pytest.mark.component


class FailingTables:
    """Delegates to a real store but raises on WRITES to the listed tables (simulates an audit-store outage for logging tables)."""

    def __init__(self, inner, tables):
        self._inner, self._tables = inner, set(tables)
        self.attempted: list[str] = []

    def __getattr__(self, name):
        attr = getattr(self._inner, name)
        if name not in ("insert", "insert_if_absent", "update_where"):
            return attr

        def guarded(table, *a, **kw):
            if table in self._tables:
                self.attempted.append(table)
                raise StoreError(f"simulated audit outage writing {table}")
            return attr(table, *a, **kw)

        return guarded


@pytest.fixture
def mk(spark, tmp_path):
    made = []

    def make(objs, tables, tables_to_fail=()):
        w = build_world(tmp_path, spark, objs, tables, wrap=(lambda s: FailingTables(s, tables_to_fail)) if tables_to_fail else None)
        made.append(w)
        return w

    yield make
    for o in ("a", "b", "c", "orders"):
        spark.sql(f"DROP TABLE IF EXISTS bronze.{o}")


def test_failure_path_survives_audit_outage_and_still_returns_a_failed_result(mk):
    w = mk([{"code": "orders", "table": "orders", "load_type": "FULL"}], {"orders": (DDL, rows(9))}, ("run_error", "run_object", "run_attempt"))
    w.source.fail_extract = [DataError("source data is bad")]
    res = w.ingest("orders")  # before the fix this raised StoreError out of ingest_object
    assert res.status == "FAILED" and res.error_class == "DATA" and "source data is bad" in res.message
    assert set(w.store.attempted) >= {"run_error", "run_object"}  # the engine did try to log, and tolerated the failure
    bid = w.engine.manifests.list("orders", "live")[0].batch_id
    assert w.engine.events.state(bid) == "FAILED_FATAL"  # the protocol event (not best-effort logging) was still recorded


def test_success_path_survives_result_logging_outage(mk):
    w = mk([{"code": "orders", "table": "orders", "load_type": "FULL"}], {"orders": (DDL, rows(9, 10))}, ("run_object", "run_attempt"))
    res = w.ingest("orders")
    assert res.status == "DONE" and res.rows_written == 2 and w.bronze("orders").count() == 2  # committed data is never reported as failed
    assert w.engine.watermarks.get("orders", "live").committed_seq == 1


def test_runner_isolates_an_object_whose_audit_logging_also_fails(mk):
    w = mk([{"code": c, "table": c, "load_type": "FULL"} for c in "abc"], {c: (DDL, rows(9, 10)) for c in "abc"}, ("run_error", "run_object", "run_attempt"))
    w.source.fail_extract = [DataError("a is broken")]
    s = w.runner.run(execution="run_all", concurrency=1)  # must not raise
    assert s.status == "PARTIAL" and s.by_status() == {"FAILED": 1, "DONE": 2}
    assert sum(w.spark.table(f"bronze.{c}").count() for c in "abc" if w.spark.catalog.tableExists(f"bronze.{c}")) == 4


def test_runner_safety_net_contains_unexpected_exceptions(mk):
    w = mk([{"code": c, "table": c, "load_type": "FULL"} for c in "abc"], {c: (DDL, rows(9)) for c in "abc"})
    real = w.engine.ingest_object

    def exploding(reader, ref, project, code, run_id, **kw):
        if code == "b":
            raise RuntimeError("unexpected bug in object b")
        return real(reader, ref, project, code, run_id, **kw)

    w.engine.ingest_object = exploding
    s = w.runner.run(execution="run_all", concurrency=1)
    by = {r.object_code: r for r in s.results}
    assert by["b"].status == "FAILED" and by["b"].error_class == "LOGIC" and "unexpected bug" in by["b"].message
    assert by["a"].status == "DONE" and by["c"].status == "DONE" and s.status == "PARTIAL"


def test_bronze_write_integrity_mismatch_blocks_the_commit(mk):
    """B3: if the rows read differ from the rows the Delta commit holds, no receipt and no watermark may be written."""
    w = mk([{"code": "orders", "table": "orders", "load_type": "FULL"}], {"orders": (DDL, rows(9, 10, 11))})
    orig = w.engine.writer.verify

    def tampered(*a, **kw):
        v = orig(*a, **kw)
        return None if v is None else Verification(v.delta_version, (v.rows_in_commit or 0) + 1, v.rows_counted, v.commit_tag)

    w.engine.writer.verify = tampered
    res = w.ingest("orders")
    assert res.status == "FAILED" and res.error_class == "DATA" and "integrity" in res.message.lower()
    assert w.engine.watermarks.get("orders", "live") is None  # cursor did not advance
    assert w.store.count("run_target_receipt") == 0  # no receipt for an unverifiable write


def test_matching_row_counts_pass_the_integrity_check(mk):
    w = mk([{"code": "orders", "table": "orders", "load_type": "FULL"}], {"orders": (DDL, rows(9, 10, 11))})
    res = w.ingest("orders")
    assert isinstance(res, ObjectResult) and res.status == "DONE" and res.rows_read == res.rows_written == 3
