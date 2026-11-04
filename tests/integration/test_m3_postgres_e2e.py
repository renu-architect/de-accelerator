"""PostgreSQL -> M2 ingestion -> Bronze -> M3 Silver (SCD2 + SCD1) -> Gold (KEY_MERGE + FULL), from metadata only, on real Delta (audit
tables on Delta too) and a REAL PostgreSQL server through the real JDBC connector. Acceptance criterion 1."""

import pytest

import de_accelerator.connectors.builtin  # noqa: F401  (registers connectors)
from de_accelerator.audit.layer_cursor import LayerCursorRepo
from de_accelerator.core.secrets import StaticSecretProvider
from de_accelerator.ingestion.engine import EngineConfig, IngestionEngine
from de_accelerator.ingestion.runner import IngestionRunner
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.schema.tables import all_tables
from de_accelerator.metadata.store.spark import SparkDeltaTableStore
from tests.m3util import build_world, silver_obj

pytestmark = [pytest.mark.component, pytest.mark.integration]

SECRETS = {"pg-host": "127.0.0.1", "pg-db": "erp", "pg-user": "acc_reader", "pg-pwd": "Reader-pw-9f3a"}
CUST_COLS = [
    {"name": "id", "ordinal": 1, "target_type": "bigint", "is_bk": True, "is_nullable": False},
    {"name": "name", "ordinal": 2, "target_type": "string", "cleanse_rules": {"ops": [{"op": "trim"}]}},
    {"name": "email", "ordinal": 3, "target_type": "string", "cleanse_rules": {"ops": [{"op": "trim"}, {"op": "lower"}]}},
    {"name": "country", "ordinal": 4, "target_type": "string",
     "cleanse_rules": {"ops": [{"op": "trim"}, {"op": "upper"}, {"op": "map_values", "map": {"UK": "GB"}}]}},
    {"name": "modified_at", "ordinal": 5, "target_type": "timestamp"},
]  # fmt: skip
ORD_COLS = [
    {"name": "id", "ordinal": 1, "target_type": "bigint", "is_bk": True, "is_nullable": False},
    {"name": "customer_id", "ordinal": 2, "target_type": "bigint"},
    {"name": "amount", "ordinal": 3, "target_type": "decimal(12,2)"},
    {"name": "modified_at", "ordinal": 4, "target_type": "timestamp"},
]
FACT_SQL = ("SELECT o.id, o.amount, c.country FROM {{ input.orders }} o LEFT JOIN "
            "(SELECT * FROM {{ input.customers }} WHERE _is_current AND NOT _is_deleted) c ON c.id = o.customer_id WHERE NOT o._is_deleted")  # fmt: skip
DIM_SQL = "SELECT id, name, email, country FROM {{ input.customers }} WHERE _is_current AND NOT _is_deleted"
GOLD = [
    {"code": "fact_orders", "merge": "MERGE", "keys": ["id"], "dep_type": "HARD", "steps": [{"type": "SQL", "text": FACT_SQL, "inputs": ["orders", "customers"], "params": {}}]},
    {"code": "dim_customer", "merge": "OVERWRITE", "keys": ["id"], "dep_type": "HARD", "steps": [{"type": "SQL", "text": DIM_SQL, "inputs": ["customers"], "params": {}}]},
]  # fmt: skip


@pytest.fixture(scope="module")
def delta_audit(spark):
    s = SparkDeltaTableStore(spark, flavor="LOCAL")
    for t in reversed(all_tables()):
        s.drop_table(t)
    Migrator(s, MIGRATIONS).apply()
    yield s
    for t in reversed(all_tables()):
        s.drop_table(t)


@pytest.fixture(scope="module")
def world(pg, spark, delta_audit, tmp_path_factory):
    pg.sql("DROP TABLE IF EXISTS public.m3_orders; DROP TABLE IF EXISTS public.m3_customers")
    pg.sql("CREATE TABLE public.m3_customers (id bigint PRIMARY KEY, name text, email text, country text, modified_at timestamp)")
    pg.sql(
        "CREATE TABLE public.m3_orders (id bigint PRIMARY KEY, customer_id bigint, amount numeric(12,2), modified_at timestamp)"
    )
    pg.sql("GRANT SELECT ON public.m3_customers, public.m3_orders TO acc_reader")
    pg.sql("INSERT INTO public.m3_customers VALUES (1, ' Ann ', 'ANN@X.COM', 'uk', '2026-11-03 09:00:00'), (2, 'Bob', 'bob@x.com', 'fr', '2026-11-03 09:00:00'),"
           " (3, 'Cy', 'cy@x.com', 'de', '2026-11-03 09:00:00')")  # fmt: skip
    pg.sql(
        "INSERT INTO public.m3_orders VALUES (10, 1, 10.50, '2026-11-03 10:00:00'), (11, 2, 20.00, '2026-11-03 10:00:00'), (12, 3, 30.25, '2026-11-03 10:00:00')"
    )
    src = {"code": "erp", "source_type": "postgresql", "connector_type": "jdbc", "auth_mode": "SECRET",
           "connection": {"host": "kv:pg-host", "port": str(pg.port), "database": "kv:pg-db", "user": "kv:pg-user", "password": "kv:pg-pwd"},
           "options": {"encrypt": False, "fetchsize": 1000, "max_connections": 2}}  # fmt: skip
    objs = [
        silver_obj(
            "customers",
            schema="public",
            table="m3_customers",
            columns=CUST_COLS,
            merge="SCD2",
            effective="modified_at",
            scd_columns=["email", "country"],
            load={"load_type": "FULL", "is_complete_extract": True},
            business_keys=["id"],
        ),  # fmt: skip
        silver_obj(
            "orders",
            schema="public",
            table="m3_orders",
            columns=ORD_COLS,
            merge="SCD1",
            effective="modified_at",
            dedupe_order="modified_at desc",
            load={
                "load_type": "INCREMENTAL",
                "watermark_column": "modified_at",
                "watermark_type": "timestamp",
                "lookback_window": "PT1H",
            },
            business_keys=["id"],
        ),  # fmt: skip
    ]
    w = build_world(tmp_path_factory.mktemp("pge2e"), spark, objs, store=delta_audit, gold=GOLD, source=src)
    ing_cfg = EngineConfig(env="DEV", catalog=None, flavor="LOCAL", sleep=lambda s: None)
    ing = IngestionEngine(delta_audit, spark, StaticSecretProvider(dict(SECRETS)), ing_cfg, w.clock)
    w.ingestion = IngestionRunner(ing, delta_audit, "DEV")  # type: ignore[attr-defined]
    yield w
    for c in ("customers", "orders"):
        for t in (f"bronze.{c}", f"silver.{c}", f"dq.rej_{c}"):
            spark.sql(f"DROP TABLE IF EXISTS {t}")
    for g in ("fact_orders", "dim_customer"):
        spark.sql(f"DROP TABLE IF EXISTS gold.{g}")


def statuses(summary):
    return {(r.layer, r.object_code): r.status for r in summary.results}


def test_postgres_to_bronze_silver_gold_from_metadata_only(world, pg):
    w = world
    s1 = w.ingestion.run(objects=("customers", "orders"))
    assert s1.status == "SUCCEEDED"
    t1 = w.runner.run(objects=("customers", "orders", "fact_orders", "dim_customer"), concurrency=2)
    assert t1.status == "SUCCEEDED", [(r.object_code, r.status, r.message) for r in t1.results]
    assert statuses(t1) == {
        ("SILVER", "customers"): "DONE",
        ("SILVER", "orders"): "DONE",
        ("GOLD", "fact_orders"): "DONE",
        ("GOLD", "dim_customer"): "DONE",
    }
    cust = {x["id"]: x for x in w.table("silver.customers")}
    assert (
        cust[1]["name"] == "Ann" and cust[1]["email"] == "ann@x.com" and cust[1]["country"] == "GB"
    )  # trim / lower / map UK->GB
    assert all(x["_is_current"] and x["_valid_to"] is None for x in cust.values())
    assert {x["id"]: x["country"] for x in w.table("gold.fact_orders")} == {10: "GB", 11: "FR", 12: "DE"}
    assert sorted(x["id"] for x in w.table("gold.dim_customer")) == [1, 2, 3]

    # source changes: customer 1 moves country (new SCD2 version), customer 4 is new, order 11 is corrected, order 13 is new
    pg.sql("UPDATE public.m3_customers SET country = 'us', modified_at = '2026-11-04 08:00:00' WHERE id = 1")
    pg.sql("INSERT INTO public.m3_customers VALUES (4, 'Di', 'di@x.com', 'es', '2026-11-04 08:30:00')")
    pg.sql("UPDATE public.m3_orders SET amount = 25.00, modified_at = '2026-11-04 09:00:00' WHERE id = 11")
    pg.sql("INSERT INTO public.m3_orders VALUES (13, 4, 40.00, '2026-11-04 09:30:00')")
    s2 = w.ingestion.run(objects=("customers", "orders"))
    assert s2.status == "SUCCEEDED"
    t2 = w.runner.run(objects=("customers", "orders", "fact_orders", "dim_customer"), concurrency=2)
    assert statuses(t2) == {
        ("SILVER", "customers"): "DONE",
        ("SILVER", "orders"): "DONE",
        ("GOLD", "fact_orders"): "DONE",
        ("GOLD", "dim_customer"): "DONE",
    }
    versions = sorted((x["_valid_from"], x["country"], x["_is_current"]) for x in w.table("silver.customers") if x["id"] == 1)
    assert [(v[1], v[2]) for v in versions] == [("GB", False), ("US", True)]  # SCD2 history
    for i in (2, 3):  # the FULL reload re-delivered unchanged customers: duplicates of a logical change create no new version
        assert len([x for x in w.table("silver.customers") if x["id"] == i]) == 1
    orders = {x["id"]: x["amount"] for x in w.table("silver.orders")}
    assert str(orders[11]) == "25.00" and 13 in orders and len(orders) == 4
    assert {x["id"]: (str(x["amount"]), x["country"]) for x in w.table("gold.fact_orders")}[11] == ("25.00", "FR")
    assert {x["id"]: x["country"] for x in w.table("gold.fact_orders")}[
        10
    ] == "US"  # KEY_MERGE refreshed the row for the moved customer? (see below)
    assert sorted(x["id"] for x in w.table("gold.dim_customer")) == [1, 2, 3, 4]
    assert {x["id"]: x["country"] for x in w.table("gold.dim_customer")}[1] == "US"

    # nothing changed: every unit is NO_DATA, cursors are stable
    t3 = w.runner.run(objects=("customers", "orders", "fact_orders", "dim_customer"))
    assert {r.status for r in t3.results} == {"NO_DATA"} and t3.status == "SUCCEEDED"
    assert LayerCursorRepo(w.store).applied_seq("customers", "SILVER") >= 2
