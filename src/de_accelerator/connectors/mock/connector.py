"""MockConnector: deterministic in-memory source for unit/component/crash tests and local demos (`source_type: mock`).

The source data lives in a process-local `MockSource` (registered by name in MOCK_SOURCES; `source.connection.dataset` or the
source code selects it). It counts reads so tests can assert that a recovery did NOT re-read the source (invariant I5)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
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
from de_accelerator.core.errors import SourceConfigError, TransientSourceError
from de_accelerator.ingestion.windows import incremental_window, ts_str
from de_accelerator.metadata.findings import Finding


@dataclass
class MockTable:
    ddl: str  # Spark DDL, e.g. "id bigint, amount double, modified string"
    rows: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class MockSource:
    tables: dict[str, MockTable] = field(default_factory=dict)
    plans: int = 0
    reads: int = 0
    fail_extract: list[Exception] = field(default_factory=list)  # raised one per extract call, in order
    fail_plan: list[Exception] = field(default_factory=list)

    def add(self, name: str, ddl: str, rows: list[dict[str, Any]]) -> MockTable:
        self.tables[name] = MockTable(ddl, list(rows))
        return self.tables[name]


MOCK_SOURCES: dict[str, MockSource] = {}


def _cols(ddl: str) -> list[tuple[str, str]]:
    out = []
    for part in ddl.split(","):
        name, typ = part.strip().split(" ", 1)
        out.append((name.strip("`"), typ.strip()))
    return out


@register_connector("mock")
class MockConnector:
    capabilities = Capabilities(full=True, incremental=True, snapshot=True, discovery=True)

    def __init__(self, ctx: ConnectorContext) -> None:
        self.ctx = ctx
        name = (ctx.source.connection or {}).get("dataset") or ctx.source.code
        if name not in MOCK_SOURCES:
            raise SourceConfigError(f"mock dataset '{name}' is not registered")
        self.src = MOCK_SOURCES[name]

    @classmethod
    def validate_config(cls, source: Any, bindings: Sequence[ObjectBinding]) -> list[Finding]:
        return []

    def test_connection(self) -> TestResult:
        return TestResult(True, "mock")

    def _table(self, b: ObjectBinding) -> MockTable:
        name = b.obj.object_name
        if name not in self.src.tables:
            raise SourceConfigError(f"mock table '{name}' not found")
        return self.src.tables[name]

    def get_schema(self, binding: ObjectBinding) -> SourceSchema:
        return SourceSchema(tuple(ColumnInfo(n, t) for n, t in _cols(self._table(binding).ddl)))

    def plan(self, req: PlanRequest) -> ExtractPlan:
        self.src.plans += 1
        if self.src.fail_plan:
            raise self.src.fail_plan.pop(0)
        b, st = req.binding, req.binding.strategy
        now = self.ctx.clock.now()
        committed = req.committed.value if req.committed else None
        if req.window_override:
            return ExtractPlan(req.window_override[0], req.window_override[1], None, None,
                               {"mode": "window", "table": b.obj.object_name, "read_from": req.window_override[0]})  # fmt: skip
        if st.load_type in ("FULL", "SNAPSHOT"):
            return ExtractPlan(None, ts_str(now), None, None, {"mode": "full", "table": b.obj.object_name})
        if st.load_type == "INCREMENTAL":
            col = st.watermark_column
            vals = [r[col] for r in self._table(b).rows if r.get(col) is not None]
            win = incremental_window(st.watermark_type or "timestamp", committed, max(vals) if vals else None, now,
                                     st.lookback_window, st.late_arrival_window)  # fmt: skip
            return ExtractPlan(win.wm_from, win.wm_to, st.lookback_window, None,
                               {"mode": "incremental", "table": b.obj.object_name, "column": col, "read_from": win.read_from},
                               empty=win.empty)  # fmt: skip
        raise SourceConfigError(f"mock connector does not support load_type {st.load_type}")

    def extract(self, plan: ExtractPlan, req: PlanRequest, ctx: ExtractContext) -> ExtractResult:
        self.src.reads += 1
        if self.src.fail_extract:
            raise self.src.fail_extract.pop(0)
        b = req.binding
        t = self._table(b)
        rows = t.rows
        pj = plan.plan_json
        if pj["mode"] in ("incremental", "window"):
            col = pj.get("column") or b.strategy.watermark_column
            lo, hi = pj.get("read_from"), plan.wm_to

            def ok(r: dict[str, Any]) -> bool:
                v = r.get(col)
                return v is not None and (lo is None or v > lo) and (hi is None or v <= hi)

            rows = [r for r in rows if ok(r)]
        names = [n for n, _ in _cols(t.ddl)]
        df = self.ctx.spark.createDataFrame([tuple(r.get(n) for n in names) for r in rows], t.ddl)
        return ExtractResult(df, self.get_schema(b), {"expected_rows": len(rows)})

    def count(self, plan: ExtractPlan, req: PlanRequest) -> int | None:
        return None

    def close(self) -> None:
        return None


def transient(msg: str = "mock connection reset") -> TransientSourceError:
    return TransientSourceError(msg)

