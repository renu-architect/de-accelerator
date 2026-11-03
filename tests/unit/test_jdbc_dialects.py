"""SQL generation for SQL Server / Azure SQL / PostgreSQL (no database needed). SQL Server cannot be run in this sandbox, so
correctness of the generated T-SQL is pinned here; execution against a real server is tracked as pending evidence."""

import pytest

import de_accelerator.connectors.builtin  # noqa: F401
from de_accelerator.connectors.base import ConnectorContext, ExtractPlan, ObjectBinding
from de_accelerator.connectors.jdbc.connector import JdbcConnector, _transient_or_platform
from de_accelerator.connectors.jdbc.dialects import DIALECTS, check_ident
from de_accelerator.core.clock import ManualClock
from de_accelerator.core.errors import PlatformAccessError, SourceConfigError, TransientSourceError
from de_accelerator.core.secrets import StaticSecretProvider
from de_accelerator.metadata.packs.bundle import build_bundle
from de_accelerator.metadata.reader import InMemoryMetadataReader
from tests.packutil import EXAMPLE

MS, PG, AZ = DIALECTS["sqlserver"], DIALECTS["postgresql"], DIALECTS["azuresql"]


@pytest.fixture(scope="module")
def reader():
    b, _ = build_bundle(EXAMPLE, "DEV")
    return InMemoryMetadataReader.from_bundle(b)


def connector(reader, secrets=None):
    src = reader.source("sales", "erp_sql")
    ctx = ConnectorContext(src, reader.environment("sales"), StaticSecretProvider(secrets or {}), None, "run", ManualClock(__import__("datetime").datetime(2026, 11, 3, 12, tzinfo=__import__("datetime").timezone.utc)))
    return JdbcConnector(ctx), ObjectBinding(src, reader.object("sales", "orders"), tuple(reader.columns("sales", "orders")), reader.load_strategy("sales", "orders"))


@pytest.mark.parametrize("name", ["Orders", "order lines", "_x1", "A_b_9"])
def test_safe_identifiers(name):
    assert check_ident(name) == name


@pytest.mark.parametrize("name", ["a;b", "a--", "1abc", "a'b", 'a"b', "a]b", "a.b", "", "x" * 200, "Robert'); DROP TABLE Students;--"])
def test_hostile_identifiers_rejected(name):
    with pytest.raises(SourceConfigError):
        check_ident(name)


def test_sqlserver_quoting_and_literals():
    assert MS.quote("Order Lines") == "[Order Lines]" and MS.table("dbo", "Orders") == "[dbo].[Orders]" and MS.table(None, "T") == "[T]"
    assert MS.literal("2026-11-03T11:00:00.000000Z", "timestamp") == "CAST('2026-11-03T11:00:00.000000' AS datetime2(6))"
    assert MS.literal(42, "integer") == "42" and MS.literal("O'Brien") == "'O''Brien'" and MS.literal(None) == "NULL"
    assert MS.literal("12345", "rowversion") == "CAST(12345 AS binary(8))"  # sargable, big-endian ordering = rowversion ordering
    for bad in ("2026-11-03; DROP TABLE x", "yesterday", "2026-11-03T11:00:00' OR '1'='1"):
        with pytest.raises(SourceConfigError):
            MS.literal(bad, "timestamp")
    with pytest.raises(ValueError):
        MS.literal("abc", "integer")  # non-numeric never reaches SQL


def test_sqlserver_rowversion_safe_upper_bound_ignores_uncommitted_work():
    assert "MIN_ACTIVE_ROWVERSION()" in MS.safe_upper_bound_sql("rowversion") and "- 1" in MS.safe_upper_bound_sql("rowversion")
    assert MS.safe_upper_bound_sql("timestamp") is None
    assert MS.max_expr("RV", "rowversion") == "MAX(CAST([RV] AS bigint))" and MS.max_expr("ModifiedDate", "timestamp") == "MAX([ModifiedDate])"


def test_urls_enforce_tls_and_never_contain_credentials():
    u = MS.jdbc_url("h", 1433, "db", {})
    assert "encrypt=true" in u and "trustServerCertificate=false" in u and "password" not in u.lower() and "user=" not in u.lower()
    assert "trustServerCertificate=true" in MS.jdbc_url("h", None, "db", {"trust_server_certificate": True})
    az = AZ.jdbc_url("x.database.windows.net", None, "db", {"encrypt": False})
    assert "encrypt=true" in az and "hostNameInCertificate=*.database.windows.net" in az  # Azure SQL: TLS cannot be turned off
    assert PG.jdbc_url("h", None, "db", {}).startswith("jdbc:postgresql://h:5432/db?sslmode=require")
    assert "sslmode=disable" in PG.jdbc_url("h", 5, "db", {"encrypt": False})


def test_postgres_quoting_and_timestamp_literal():
    assert PG.quote("Orders") == '"Orders"' and PG.literal("2026-11-03T11:00:00Z", "timestamp") == "TIMESTAMP '2026-11-03T11:00:00'"


def test_generated_incremental_query_is_exact(reader):
    c, b = connector(reader)
    plan = ExtractPlan("2026-11-03T10:00:00.000000Z", "2026-11-03T11:00:00.000000Z", "PT2H", None,
                       {"mode": "incremental", "table": "[dbo].[Orders]", "column": "ModifiedDate", "wm_type": "timestamp",
                        "read_from": "2026-11-03T08:00:00.000000Z"})  # fmt: skip
    q = c._query(b, plan)
    assert q == ("SELECT [OrderID], [CustomerID], [Amount], [ModifiedDate] FROM [dbo].[Orders] WHERE [ModifiedDate] > "
                 "CAST('2026-11-03T08:00:00.000000' AS datetime2(6)) AND [ModifiedDate] <= CAST('2026-11-03T11:00:00.000000' AS datetime2(6))")
    full = c._query(b, ExtractPlan(None, "x", None, None, {"mode": "full", "table": "[dbo].[Orders]"}))
    assert full == "SELECT [OrderID], [CustomerID], [Amount], [ModifiedDate] FROM [dbo].[Orders]"


def test_first_batch_has_only_an_upper_bound(reader):
    c, b = connector(reader)
    q = c._query(b, ExtractPlan(None, "2026-11-03T11:00:00.000000Z", None, None,
                                {"mode": "incremental", "table": "[dbo].[Orders]", "wm_type": "timestamp", "read_from": None}))
    assert " > " not in q and "<= CAST('2026-11-03T11:00:00.000000' AS datetime2(6))" in q


def test_secrets_resolved_only_at_connect_time_and_never_in_plan_json(reader):
    c, b = connector(reader, {"erp-host": "sql.example", "erp-db": "ERP", "erp-user": "svc", "erp-pwd": "P@ss-w0rd!"})
    url, props = c._conn()
    assert "sql.example" in url and props["user"] == "svc" and props["password"] == "P@ss-w0rd!"
    from de_accelerator.core.logging import redact

    assert "P@ss-w0rd!" not in redact(f"connecting with {props}")  # registered secret values are masked in logs/messages
    plan = ExtractPlan(None, "x", None, None, {"mode": "full", "table": "[dbo].[Orders]", "query_sha256": "abc"})
    assert "P@ss-w0rd!" not in str(plan.to_dict()) and "svc" not in str(plan.to_dict())


def test_missing_secret_is_a_platform_error(reader):
    c, _ = connector(reader, {})
    with pytest.raises(PlatformAccessError):
        c._conn()


@pytest.mark.parametrize("msg,cls", [
    ("Login failed for user 'x'", PlatformAccessError), ("password authentication failed for user", PlatformAccessError),
    ("Connection to localhost:1 refused", TransientSourceError), ("The TCP/IP connection to the host has failed", TransientSourceError),
    ("Communications link failure", TransientSourceError),
])
def test_error_classification(msg, cls):
    assert isinstance(_transient_or_platform(Exception(msg)), cls)


def test_unknown_errors_pass_through_unclassified():
    e = ValueError("something else")
    assert _transient_or_platform(e) is e  # the engine turns these into LOGIC errors (not retried)


def test_validate_config_checks_connection_options_and_load_types(reader):
    src = reader.source("sales", "erp_sql")
    b = ObjectBinding(src, reader.object("sales", "orders"), (), reader.load_strategy("sales", "orders"))
    assert JdbcConnector.validate_config(src, [b]) == []
    from dataclasses import replace

    bad_src = replace(src, connection={"host": "kv:x"}, options={"nope": 1})
    msgs = " | ".join(f.message for f in JdbcConnector.validate_config(bad_src, [b]))
    assert "connection.database" in msgs and "connection.user" in msgs and "connection.password" in msgs and "unknown source option 'nope'" in msgs
