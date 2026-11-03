"""SQL dialects: identifier quoting, typed literals, URLs, catalog queries. All SQL is generated from metadata only;
identifiers are whitelisted and values are rendered as typed literals — never concatenated free text."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, ClassVar

from de_accelerator.core.errors import SourceConfigError

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_ ]{0,127}$")


def check_ident(name: str) -> str:
    if not isinstance(name, str) or not _IDENT.match(name):
        raise SourceConfigError(f"unsafe or invalid SQL identifier {name!r} (allowed: letters, digits, '_' and spaces; must not start with a digit)")
    return name


class Dialect:
    name: ClassVar[str] = "ansi"
    driver: ClassVar[str] = ""
    default_port: ClassVar[int] = 0
    # watermark types this dialect can compare
    wm_types: ClassVar[tuple[str, ...]] = ("timestamp", "integer")

    def quote(self, ident: str) -> str:
        return '"' + check_ident(ident) + '"'

    def table(self, schema: str | None, name: str) -> str:
        return (self.quote(schema) + "." if schema else "") + self.quote(name)

    def literal(self, value: Any, wm_type: str = "string") -> str:
        """Typed literal. Timestamps/ints/strings only; everything else is rejected."""
        if value is None:
            return "NULL"
        if wm_type in ("integer", "int", "bigint"):
            return str(int(value))
        if wm_type in ("timestamp", "datetime"):
            s = value.strftime("%Y-%m-%dT%H:%M:%S.%f") if isinstance(value, datetime) else str(value).replace("Z", "")
            if not re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d{1,6})?$", s):
                raise SourceConfigError(f"invalid timestamp literal {value!r}")
            return self._ts_literal(s)
        return "'" + str(value).replace("'", "''") + "'"

    def _ts_literal(self, iso: str) -> str:
        return f"'{iso}'"

    def safe_upper_bound_sql(self, wm_type: str) -> str | None:
        """Source-side safe upper bound that ignores uncommitted work (only meaningful for rowversion)."""
        return None

    def max_expr(self, col: str, wm_type: str) -> str:
        return f"MAX({self.quote(col)})"

    def jdbc_url(self, host: str, port: int | None, database: str, options: dict[str, Any]) -> str:
        raise NotImplementedError

    # catalog queries (discovery) -----------------------------------------------------------------
    def list_tables_sql(self, schema: str | None) -> str:
        where = f" AND TABLE_SCHEMA = {self.literal(schema)}" if schema else ""
        return f"SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'{where}"

    def columns_sql(self, schema: str | None, table: str) -> str:
        sch = f" AND TABLE_SCHEMA = {self.literal(schema)}" if schema else ""
        return (f"SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, IS_NULLABLE AS is_nullable, ORDINAL_POSITION AS ordinal "
                f"FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = {self.literal(table)}{sch}")

    def pk_sql(self, schema: str | None, table: str) -> str:
        sch = f" AND tc.TABLE_SCHEMA = {self.literal(schema)}" if schema else ""
        return ("SELECT kcu.COLUMN_NAME AS column_name FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA "
                f"WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_NAME = {self.literal(table)}{sch}")


class SqlServerDialect(Dialect):
    name = "sqlserver"
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    default_port = 1433
    wm_types = ("timestamp", "integer", "rowversion")

    def quote(self, ident: str) -> str:
        return "[" + check_ident(ident) + "]"

    def _ts_literal(self, iso: str) -> str:
        return f"CAST('{iso}' AS datetime2(6))"

    def literal(self, value: Any, wm_type: str = "string") -> str:
        if wm_type == "rowversion":  # carried as a decimal integer string; compared as binary(8) (sargable, big-endian ordering)
            return f"CAST({int(value)} AS binary(8))"
        return super().literal(value, wm_type)

    def safe_upper_bound_sql(self, wm_type: str) -> str | None:
        """Source-side safe upper bound that ignores uncommitted work (rowversion)."""
        if wm_type == "rowversion":
            return "SELECT CAST(CAST(MIN_ACTIVE_ROWVERSION() AS bigint) - 1 AS varchar(30)) AS v"
        return None

    def max_expr(self, col: str, wm_type: str) -> str:
        c = self.quote(col)
        return f"MAX(CAST({c} AS bigint))" if wm_type == "rowversion" else f"MAX({c})"

    def jdbc_url(self, host: str, port: int | None, database: str, options: dict[str, Any]) -> str:
        enc = "true" if options.get("encrypt", True) else "false"
        trust = "true" if options.get("trust_server_certificate", False) else "false"
        return (f"jdbc:sqlserver://{host}:{port or self.default_port};databaseName={database};encrypt={enc};"
                f"trustServerCertificate={trust};loginTimeout={int(options.get('login_timeout_sec', 30))}")


class AzureSqlDialect(SqlServerDialect):
    name = "azuresql"
    # Azure SQL: same T-SQL; TLS is mandatory, host name in certificate is verified
    def jdbc_url(self, host: str, port: int | None, database: str, options: dict[str, Any]) -> str:
        return (f"jdbc:sqlserver://{host}:{port or self.default_port};databaseName={database};encrypt=true;"
                f"trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout={int(options.get('login_timeout_sec', 30))}")


class PostgresDialect(Dialect):
    name = "postgresql"
    driver = "org.postgresql.Driver"
    default_port = 5432

    def _ts_literal(self, iso: str) -> str:
        return f"TIMESTAMP '{iso}'"

    def jdbc_url(self, host: str, port: int | None, database: str, options: dict[str, Any]) -> str:
        ssl = "require" if options.get("encrypt", True) else "disable"
        return f"jdbc:postgresql://{host}:{port or self.default_port}/{database}?sslmode={ssl}&connectTimeout={int(options.get('login_timeout_sec', 30))}"


DIALECTS: dict[str, Dialect] = {"sqlserver": SqlServerDialect(), "azuresql": AzureSqlDialect(), "postgresql": PostgresDialect()}
