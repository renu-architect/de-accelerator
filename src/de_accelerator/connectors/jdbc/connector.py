"""JdbcConnector: FULL / INCREMENTAL / SNAPSHOT extraction over Spark JDBC (SQL Server, Azure SQL, PostgreSQL).

plan(): cheap source queries (MAX / safe upper bound / partition bounds) -> frozen ExtractPlan.
extract(): exactly the frozen window as a (partitioned) Spark JDBC read. Credentials resolved from Key Vault references at runtime."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

from de_accelerator.connectors.base import (
    Capabilities,
    ColumnInfo,
    ConnectorContext,
    ExtractContext,
    ExtractPlan,
    ExtractResult,
    ObjectBinding,
    PlanRequest,
    SourceSchema,
    TestResult,
    register_connector,
)
from de_accelerator.connectors.jdbc.dialects import DIALECTS, Dialect, check_ident
from de_accelerator.core.errors import PlatformAccessError, SourceConfigError, TransientSourceError
from de_accelerator.core.logging import quiet_wire_loggers
from de_accelerator.core.secrets import resolve
from de_accelerator.ingestion.windows import incremental_window, ts_str
from de_accelerator.metadata.findings import Finding
from de_accelerator.metadata.packs.ids import sha256_hex

ALLOWED_SOURCE_OPTIONS = {"fetchsize", "max_connections", "encrypt", "trust_server_certificate", "query_timeout_sec", "login_timeout_sec",
                          "isolation_level"}  # fmt: skip
ALLOWED_READ_OPTIONS = {"query_timeout_sec", "max_window", "max_batches_per_run", "source_timezone", "change_table", "capture_instance"}
REQUIRED_CONNECTION = {"SECRET": ("host", "database", "user", "password"), "SERVICE_PRINCIPAL": ("host", "database", "client_id", "client_secret"),
                       "MANAGED_IDENTITY": ("host", "database", "service_credential")}  # fmt: skip


def _wm_text(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return ts_str(v if v.tzinfo else v.replace(tzinfo=timezone.utc))  # noqa: UP017
    return str(v)


def _transient_or_platform(exc: Exception) -> Exception:
    text = f"{type(exc).__name__} {exc}".lower()
    if any(k in text for k in ("login failed", "authentication", "password authentication failed", "permission denied", "access denied")):
        return PlatformAccessError(f"source authentication/authorisation failed: {type(exc).__name__}")
    if any(k in text for k in ("connection reset", "connection refused", "refused", "timed out", "unknown host", "unable to connect", "timeout", "communications link failure",
                               "the tcp/ip connection", "i/o error", "broken pipe", "connection is closed", "deadlock")):  # fmt: skip
        return TransientSourceError(f"transient source error: {type(exc).__name__}: {str(exc)[:200]}")
    return exc


@register_connector("sqlserver", "azuresql", "postgresql")
class JdbcConnector:
    capabilities = Capabilities(full=True, incremental=True, snapshot=True, discovery=True, count_pushdown=True, partitioned_read=True)

    def __init__(self, ctx: ConnectorContext) -> None:
        self.ctx = ctx
        self.dialect: Dialect = DIALECTS[ctx.source.source_type]
        self.opts: dict[str, Any] = dict(ctx.source.options or {})
        quiet_wire_loggers()
        try:  # Spark must not print credentials when it renders JDBC relations/plans
            ctx.spark.conf.set("spark.sql.redaction.options.regex", "(?i)url|password|user|token|accesstoken|secret")
        except Exception:  # noqa: BLE001 - static conf on some runtimes: set it in the cluster spark_conf instead
            pass

    # ---- configuration ---------------------------------------------------------------------------------------
    @classmethod
    def validate_config(cls, source: Any, bindings: Sequence[ObjectBinding]) -> list[Finding]:
        out: list[Finding] = []
        dialect = DIALECTS[source.source_type]
        opts = dict(source.options or {})
        need = REQUIRED_CONNECTION.get(source.auth_mode, ())
        loc = f"source:{source.code}"
        for k in need:
            if k not in (source.connection or {}):
                out.append(Finding("MDV-6010", "ERROR", f"{source.source_type}/{source.auth_mode} requires connection.{k}", loc, "SEMANTIC"))
        for k in set(opts) - ALLOWED_SOURCE_OPTIONS:
            out.append(Finding("MDV-6010", "ERROR", f"unknown source option '{k}' (allowed: {sorted(ALLOWED_SOURCE_OPTIONS)})", loc, "SEMANTIC"))
        for b in bindings:
            ro, st = b.strategy.read_options or {}, b.strategy
            bl = f"load_strategy:{b.code}"
            for k in set(ro) - ALLOWED_READ_OPTIONS:
                out.append(Finding("MDV-6010", "ERROR", f"unknown read_options key '{k}' for JDBC", bl, "SEMANTIC"))
            if st.load_type not in ("FULL", "SNAPSHOT", "INCREMENTAL", "CDC"):
                out.append(Finding("MDV-6010", "ERROR", f"JDBC does not support load_type {st.load_type}", bl, "SEMANTIC"))
            if st.load_type == "INCREMENTAL" and (st.watermark_type or "timestamp") not in dialect.wm_types:
                out.append(Finding("MDV-6010", "ERROR", f"watermark_type '{st.watermark_type}' not supported for {source.source_type} "
                                   f"(supported: {list(dialect.wm_types)})", bl, "SEMANTIC"))  # fmt: skip
            for ident in (b.obj.schema_name, b.obj.object_name, st.watermark_column, st.partition_column):
                if ident:
                    try:
                        check_ident(ident)
                    except SourceConfigError as exc:
                        out.append(Finding("MDV-6010", "ERROR", str(exc), bl, "SEMANTIC"))
        return out

    # ---- connection --------------------------------------------------------------------------------------------
    def _conn(self) -> tuple[str, dict[str, str]]:
        c = self.ctx.source.connection or {}
        sec = self.ctx.secrets
        host, db = resolve(sec, c.get("host")), resolve(sec, c.get("database"))
        port = resolve(sec, c.get("port"))
        if not host or not db:
            raise SourceConfigError("connection.host and connection.database are required")
        url = self.dialect.jdbc_url(str(host), int(port) if port else None, str(db), self.opts)
        props = {"driver": self.dialect.driver, "fetchsize": str(int(self.opts.get("fetchsize", 10000)))}
        mode = self.ctx.source.auth_mode
        if mode == "SECRET":
            props["user"], props["password"] = str(resolve(sec, c["user"])), str(resolve(sec, c["password"]))
        elif mode == "SERVICE_PRINCIPAL":
            props["user"], props["password"] = str(resolve(sec, c["client_id"])), str(resolve(sec, c["client_secret"]))
        elif mode == "MANAGED_IDENTITY":
            props["accessToken"] = self._access_token(str(resolve(sec, c["service_credential"])))
        if "query_timeout_sec" in self.opts:
            props["queryTimeout"] = str(int(self.opts["query_timeout_sec"]))
        return url, props

    def _access_token(self, credential: str) -> str:  # pragma: no cover - Databricks only (verification pending)
        try:
            dbutils = __import__("pyspark.dbutils", fromlist=["DBUtils"]).DBUtils(self.ctx.spark)
            return str(dbutils.credentials.getServiceCredentialsProvider(credential).get_token("https://database.windows.net/.default").token)
        except Exception as exc:
            raise PlatformAccessError("managed-identity token via Unity Catalog service credential is unavailable "
                                      "(Databricks runtime required)") from exc

    def _read(self, sql_or_table: str, **extra: Any) -> Any:
        url, props = self._conn()
        r = self.ctx.spark.read.format("jdbc").option("url", url)
        for k, v in props.items():
            r = r.option(k, v)
        r = r.option("dbtable", sql_or_table)
        for k, v in extra.items():
            r = r.option(k, v)
        return r

    def _scalar(self, sql: str) -> Any:
        try:
            rows = self._read(f"({sql}) q").load().limit(1).collect()
        except Exception as exc:
            raise _transient_or_platform(exc) from exc
        return rows[0][0] if rows else None

    # ---- discovery -----------------------------------------------------------------------------------------------
    def test_connection(self) -> TestResult:
        try:
            self._scalar("SELECT 1 AS v")
            return TestResult(True, "ok")
        except Exception as exc:
            return TestResult(False, f"{type(exc).__name__}")

    def get_schema(self, b: ObjectBinding) -> SourceSchema:
        t = self.dialect.table(b.obj.schema_name, b.obj.object_name)
        df = self._read(f"(SELECT * FROM {t} WHERE 1 = 0) q").load()
        return SourceSchema(tuple(ColumnInfo(f.name, f.dataType.simpleString(), f.nullable) for f in df.schema.fields))

    # ---- plan / extract ----------------------------------------------------------------------------------------
    def _where(self, b: ObjectBinding, lo: str | None, hi: str | None, wm_type: str) -> str:
        if not b.strategy.watermark_column:
            return ""
        col = self.dialect.quote(b.strategy.watermark_column)
        parts = []
        if lo is not None:
            parts.append(f"{col} > {self.dialect.literal(lo, wm_type)}")
        if hi is not None:
            parts.append(f"{col} <= {self.dialect.literal(hi, wm_type)}")
        return (" WHERE " + " AND ".join(parts)) if parts else ""

    def _select(self, b: ObjectBinding) -> str:
        cols = [c.name for c in b.columns]
        return ", ".join(self.dialect.quote(c) for c in cols) if cols else "*"

    def plan(self, req: PlanRequest) -> ExtractPlan:
        b, st = req.binding, req.binding.strategy
        d = self.dialect
        table = d.table(b.obj.schema_name, b.obj.object_name)
        now = self.ctx.clock.now()
        committed = req.committed.value if req.committed else None
        wm_type = (st.watermark_type or "timestamp").lower()
        if req.window_override:
            lo, hi = req.window_override
            plan_json: dict[str, Any] = {"mode": "window", "table": table, "read_from": lo, "wm_type": wm_type}
            return self._finish_plan(b, ExtractPlan(lo, hi, None, None, plan_json))
        if st.load_type in ("FULL", "SNAPSHOT"):
            return self._finish_plan(b, ExtractPlan(None, ts_str(now), None, None, {"mode": "full", "table": table}))
        if st.load_type != "INCREMENTAL":
            raise SourceConfigError(f"JDBC connector: load_type {st.load_type} is not implemented yet")
        if not st.watermark_column:
            raise SourceConfigError("INCREMENTAL requires watermark_column")
        safe = d.safe_upper_bound_sql(wm_type)
        if safe:
            source_max = _wm_text(self._scalar(safe))
        else:
            source_max = _wm_text(self._scalar(f"SELECT {d.max_expr(st.watermark_column, wm_type)} AS v FROM {table}"))
        win = incremental_window("integer" if wm_type == "rowversion" else wm_type, committed, source_max, now, st.lookback_window,
                                 st.late_arrival_window)  # fmt: skip
        plan_json = {"mode": "incremental", "table": table, "column": st.watermark_column, "wm_type": wm_type, "read_from": win.read_from}
        return self._finish_plan(b, ExtractPlan(win.wm_from, win.wm_to, st.lookback_window, None, plan_json, empty=win.empty))

    def _finish_plan(self, b: ObjectBinding, p: ExtractPlan) -> ExtractPlan:
        """Freeze partition bounds and the query hash into plan_json (so replays use the identical read)."""
        if p.empty:
            return p
        st = b.strategy
        pj = dict(p.plan_json)
        pj["query_sha256"] = sha256_hex(self._query(b, p))
        if st.partition_column and (st.num_partitions or 0) > 1:
            pc = self.dialect.quote(st.partition_column)
            where = self._where(b, pj.get("read_from"), p.wm_to, pj.get("wm_type", "timestamp")) if pj["mode"] != "full" else ""
            row = self._read(f"(SELECT MIN({pc}) AS lo, MAX({pc}) AS hi FROM {pj['table']}{where}) q").load().collect()[0]
            if row["lo"] is not None and row["hi"] is not None and row["lo"] != row["hi"]:
                pj["partition"] = {"column": st.partition_column, "lo": int(row["lo"]), "hi": int(row["hi"]), "n": int(st.num_partitions)}
        return ExtractPlan(p.wm_from, p.wm_to, p.lookback, p.file_list, pj, p.empty)

    def _query(self, b: ObjectBinding, p: ExtractPlan) -> str:
        pj = p.plan_json
        where = "" if pj["mode"] == "full" else self._where(b, pj.get("read_from"), p.wm_to, pj.get("wm_type", "timestamp"))
        return f"SELECT {self._select(b)} FROM {pj['table']}{where}"

    def extract(self, plan: ExtractPlan, req: PlanRequest, ctx: ExtractContext) -> ExtractResult:
        b = req.binding
        pj = plan.plan_json
        sql = self._query(b, plan)
        if sha256_hex(sql) != pj.get("query_sha256"):  # the replay must read exactly what was frozen
            raise SourceConfigError("extraction query no longer matches the manifest (columns/metadata changed mid-batch)")
        extra: dict[str, Any] = {}
        if "partition" in pj:
            p = pj["partition"]
            extra.update(partitionColumn=self.dialect.quote(p["column"]), lowerBound=str(p["lo"]), upperBound=str(p["hi"]), numPartitions=str(p["n"]))
        try:
            df = self._read(f"({sql}) q", **extra).load()
        except Exception as exc:
            raise _transient_or_platform(exc) from exc
        return ExtractResult(df, SourceSchema(tuple(ColumnInfo(f.name, f.dataType.simpleString(), f.nullable) for f in df.schema.fields)),
                             {"query_sha256": pj["query_sha256"]})  # fmt: skip

    def count(self, plan: ExtractPlan, req: PlanRequest) -> int | None:
        pj = plan.plan_json
        where = "" if pj["mode"] == "full" else self._where(req.binding, pj.get("read_from"), plan.wm_to, pj.get("wm_type", "timestamp"))
        v = self._scalar(f"SELECT COUNT(*) AS v FROM {pj['table']}{where}")
        return int(v) if v is not None else None

    def close(self) -> None:
        return None
