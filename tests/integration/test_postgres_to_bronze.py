"""PostgreSQL -> M1 metadata -> M2 engine -> Bronze Delta, against a REAL PostgreSQL server through the real JDBC connector."""

import json

import pytest

import de_accelerator.connectors.builtin  # noqa: F401  (registers connectors)
from de_accelerator.core.faults import FaultInjector, InjectedCrash
from tests.ingest_util import build_world

pytestmark = [pytest.mark.component, pytest.mark.integration]

SECRETS = {"pg-host": "127.0.0.1", "pg-db": "erp", "pg-user": "acc_reader", "pg-pwd": "Reader-pw-9f3a"}


def source_cfg(pg):
    return {"code": "erp", "source_type": "postgresql", "connector_type": "jdbc", "auth_mode": "SECRET",
            "connection": {"host": "kv:pg-host", "port": str(pg.port), "database": "kv:pg-db", "user": "kv:pg-user", "password": "kv:pg-pwd"},
            "options": {"encrypt": False, "fetchsize": 1000, "max_connections": 2}}  # fmt: skip


@pytest.fixture
def world(pg, spark, tmp_path):
    pg.sql("DROP TABLE IF EXISTS public.orders; DROP TABLE IF EXISTS public.customers")
    pg.sql("CREATE TABLE public.orders (id bigint PRIMARY KEY, amount numeric(12,2), customer text, active boolean, modified_at timestamp)")
    pg.sql("CREATE TABLE public.customers (id bigint PRIMARY KEY, name text)")
    pg.sql("GRANT SELECT ON public.orders, public.customers TO acc_reader")
    pg.sql("INSERT INTO public.orders VALUES (1, 10.50, 'ann', true, '2026-11-03 09:00:00'), (2, 20.00, NULL, false, '2026-11-03 10:00:00'),"
           " (3, 30.25, 'cy', true, '2026-11-03 11:00:00')")  # fmt: skip
    pg.sql("INSERT INTO public.customers SELECT g, 'c' || g FROM generate_series(1, 50) g")
    objs = [{"code": "orders", "table": "orders", "schema": "public", "load_type": "INCREMENTAL", "watermark_column": "modified_at",
             "lookback": "PT1H", "partition_column": "id", "num_partitions": 3},
            {"code": "customers", "table": "customers", "schema": "public", "load_type": "FULL", "partition_column": "id", "num_partitions": 4}]  # fmt: skip
    w = build_world(tmp_path, spark, objs, {}, source=source_cfg(pg), secrets=dict(SECRETS))  # own copy: tests mutate it
    yield w
    spark.sql("DROP TABLE IF EXISTS bronze.orders")
    spark.sql("DROP TABLE IF EXISTS bronze.customers")


def test_incremental_then_full_from_real_postgres(world, pg):
    w = world
    r1 = w.ingest("orders")
    assert r1.status == "DONE" and r1.rows_written == 3
    df = w.bronze("orders")
    row = df.where("id = 1").collect()[0]
    assert str(row["amount"]) == "10.50" and row["customer"] == "ann" and row["active"] is True  # types preserved natively
    assert df.where("id = 2").collect()[0]["customer"] is None and row["_source_system"] == "erp" and row["_op_type"] == "C"
    m = w.engine.manifests.list("orders", "live")[0]
    assert m.plan_json["partition"] == {"column": "id", "lo": 1, "hi": 3, "n": 3} and m.plan_json["mode"] == "incremental"  # frozen in manifest
    assert w.engine.watermarks.get("orders", "live").committed_value == "2026-11-03T11:00:00.000000Z"
    assert w.ingest("orders").status == "NO_DATA"  # nothing newer: no batch

    pg.sql("INSERT INTO public.orders VALUES (4, 40.00, 'di', true, '2026-11-03 11:30:00')")
    pg.sql("UPDATE public.orders SET amount = 99.99, modified_at = '2026-11-03 11:45:00' WHERE id = 1")  # an UPDATE is seen via the watermark
    r2 = w.ingest("orders")
    assert r2.status == "DONE" and r2.rows_written == 3  # id 4, updated id 1, and id 3 from the 1h lookback overlap
    assert {(r["id"], str(r["amount"])) for r in w.bronze("orders").where("_batch_seq = 2").collect()} == {(1, "99.99"), (3, "30.25"), (4, "40.00")}
    assert w.bronze("orders").where("id = 1").count() == 2  # history kept: old and new version, Silver will pick the latest

    rc = w.ingest("customers")
    assert rc.status == "DONE" and rc.rows_written == 50 and w.bronze("customers").count() == 50
    assert {r["_op_type"] for r in w.bronze("customers").collect()} == {"S"}
    assert w.engine.manifests.list("customers", "live")[0].plan_json["partition"]["n"] == 4


def test_crash_after_commit_recovers_without_duplicates_on_real_source(world):
    w = world
    w.new_engine(FaultInjector({"CP06_before_receipt": 1}))  # Delta commit done, receipt/cursor not yet written
    with pytest.raises(InjectedCrash):
        w.ingest("orders")
    assert w.bronze("orders").count() == 3 and w.engine.watermarks.get("orders", "live") is None
    w.clock.advance(minutes=31)
    w.new_engine()
    res = w.ingest("orders")
    assert res.status == "DONE" and res.batch_seq == 1 and w.bronze("orders").count() == 3
    assert w.engine.watermarks.get("orders", "live").committed_seq == 1


def test_secrets_never_leak_into_audit_tables_or_logs(world, caplog):
    w = world
    with caplog.at_level("DEBUG"):
        w.ingest("orders")
    blob = json.dumps({t: w.store.select(t) for t in ("run_batch_manifest", "run_batch_event", "run_error", "run_object", "run_attempt")}, default=str)
    blob += caplog.text
    for secret in ("Reader-pw-9f3a",):
        assert secret not in blob


def test_wrong_password_is_a_platform_error_not_retried(world, pg):
    w = world
    w.secrets["pg-pwd"] = "definitely-wrong"
    w.new_engine()
    res = w.ingest("orders")
    assert res.status == "FAILED" and res.error_class == "PLATFORM" and res.attempts == 1  # auth errors are not retried
    assert "definitely-wrong" not in (res.message or "") and w.engine.watermarks.get("orders", "live") is None
    assert w.store.count("run_error") == 1


def test_unreachable_database_is_transient_and_retried(world):
    w = world
    src = w.store.select("cfg_source")[0]
    assert src["source_type"] == "postgresql"
    for r in w.store._data["cfg_source"]:  # point the connector at a closed port => connection refused
        c = json.loads(r["connection"])
        c["port"] = "1"
        r["connection"] = json.dumps(c)
    w.new_engine()
    res = w.ingest("orders")
    assert res.status == "FAILED" and res.error_class == "TRANSIENT" and res.attempts == 3  # retried per the retry policy, then left resumable


def test_test_connection_and_schema_from_real_database(world):
    from de_accelerator.connectors.base import ConnectorContext
    from de_accelerator.connectors.jdbc.connector import JdbcConnector
    from de_accelerator.core.secrets import StaticSecretProvider
    from de_accelerator.metadata.reader import SnapshotMetadataReader

    w = world
    ref, reader = w.snapshot()
    src, obj = reader.source("ingest", "erp"), reader.object("ingest", "orders")
    from de_accelerator.connectors.base import ObjectBinding

    c = JdbcConnector(ConnectorContext(src, reader.environment("ingest"), StaticSecretProvider(SECRETS), w.spark, "r", w.clock))
    assert c.test_connection().ok
    schema = c.get_schema(ObjectBinding(src, obj, (), reader.load_strategy("ingest", "orders")))
    assert [x.name for x in schema.columns] == ["id", "amount", "customer", "active", "modified_at"]
    assert isinstance(reader, SnapshotMetadataReader)
