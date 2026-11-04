"""Typed Silver plan built from the immutable config snapshot (no engine reads pack YAML)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from de_accelerator.core.transform_spec import TARGET_TYPE, is_ident, parse_order
from de_accelerator.processing.common.errors import TransformConfigError


@dataclass(frozen=True)
class ColumnSpec:
    source: str
    target: str
    type: str | None
    ops: tuple[dict[str, Any], ...]
    nullable: bool = True


@dataclass(frozen=True)
class DqRule:
    code: str
    rule_type: str
    columns: tuple[str, ...]  # source column names
    params: dict[str, Any]
    threshold_pct: float
    action: str


@dataclass(frozen=True)
class SilverPlan:
    project: str
    object_code: str
    bronze_target: str
    silver_target: str
    merge_type: str
    delete_mode: str
    columns: tuple[ColumnSpec, ...]  # empty => PASSTHROUGH (resolved from the Bronze schema at run time)
    passthrough: bool
    business_keys: tuple[str, ...]  # source names
    scd_columns: tuple[str, ...]  # source names
    dedupe_order: tuple[tuple[str, bool], ...]  # (source name, descending)
    effective_column: str | None  # source name
    on_cast_error: str
    dq_rules: tuple[DqRule, ...]

    def target_of(self, source: str) -> str:
        for c in self.columns:
            if c.source == source:
                return c.target
        return source

    def logic_fingerprint(self) -> dict[str, Any]:
        return {"silver": self.silver_target, "merge": self.merge_type, "delete": self.delete_mode, "pass": self.passthrough,
                "cols": [(c.source, c.target, c.type, c.ops) for c in self.columns], "bk": self.business_keys,
                "scd": self.scd_columns, "order": self.dedupe_order, "eff": self.effective_column, "cast": self.on_cast_error,
                "dq": [(r.code, r.rule_type, r.columns, r.params, r.threshold_pct, r.action) for r in self.dq_rules]}  # fmt: skip


def build_silver_plan(reader: Any, project: str, object_code: str) -> SilverPlan:
    obj = reader.object(project, object_code)
    maps = {m.layer: m for m in reader.layer_mappings(project, object_code)}
    sm, bm = maps.get("SILVER"), maps.get("BRONZE")
    if sm is None:
        raise TransformConfigError(f"object '{object_code}' has no SILVER layer mapping")
    bronze = (bm.target_table if bm and bm.target_table else None) or obj.target_bronze
    silver = sm.target_table or obj.target_silver
    if not bronze or not silver:
        raise TransformConfigError(f"object '{object_code}' needs Bronze and Silver targets")
    cols = sorted(reader.columns(project, object_code), key=lambda c: (c.ordinal if c.ordinal is not None else 10**6, c.name))
    specs = []
    for c in cols:
        target = c.target_name or c.name
        if not is_ident(target):
            raise TransformConfigError(f"column '{c.name}': invalid target name '{target}'")
        if c.target_type and not TARGET_TYPE.match(c.target_type):
            raise TransformConfigError(f"column '{c.name}': target_type '{c.target_type}' is not supported")
        ops = list((c.cleanse_rules or {}).get("ops", []))
        if (
            c.default_value is not None
        ):  # cfg_column.default_value == a trailing `default` operation (before any parse_* operation)
            dflt = {"op": "default", "value": c.default_value}
            ops.insert(len(ops) - 1 if ops and ops[-1]["op"] in ("parse_date", "parse_timestamp") else len(ops), dflt)
        specs.append(ColumnSpec(c.name, target, c.target_type, tuple(ops), bool(c.is_nullable)))
    order = parse_order(sm.dedupe_order) if sm.dedupe_order else []
    if order is None:
        raise TransformConfigError(f"object '{object_code}': invalid dedupe_order '{sm.dedupe_order}'")
    dq = tuple(
        DqRule(r.code, r.rule_type, tuple(r.columns), dict(r.params or {}), float(r.threshold_pct or 0.0), r.action)
        for r in reader.dq_rules(project, object_code, "SILVER")
    )
    return SilverPlan(project, object_code, bronze, silver, sm.merge_type or "APPEND", obj.delete_mode or "soft", tuple(specs),
                      (sm.column_mode or "DECLARED") == "PASSTHROUGH", tuple(obj.business_keys), tuple(sm.scd_columns),
                      tuple(order), sm.effective_column, sm.on_cast_error or "REJECT", dq)  # fmt: skip
