"""M3 transformation vocabulary shared by the metadata validator and the runtime engines (single source of truth).

Everything here is a closed allowlist: anything not listed is rejected at validation time and again at run time.
"""

from __future__ import annotations

import re
from typing import Any

#: cleansing operations: name -> (required parameter names, {parameter: expected python type(s)})
CLEANSE_OPS: dict[str, tuple[tuple[str, ...], dict[str, type | tuple[type, ...]]]] = {
    "trim": ((), {}),
    "upper": ((), {}),
    "lower": ((), {}),
    "initcap": ((), {}),
    "nullif": (("values",), {"values": list}),
    "replace": (("from", "to"), {"from": str, "to": str}),
    "default": (("value",), {"value": (str, int, float, bool)}),
    "map_values": (("map",), {"map": dict}),
    "parse_date": (("format",), {"format": str}),
    "parse_timestamp": (("format",), {"format": str}),
}
PARAM_TYPES: tuple[str, ...] = ("string", "int", "decimal", "date", "bool")
DQ_RULE_TYPES: tuple[str, ...] = ("NOT_NULL", "VALID_VALUES", "RANGE")
DQ_ACTIONS: tuple[str, ...] = ("LOG", "QUARANTINE", "FAIL_BATCH")
GOLD_MERGE_TYPES: tuple[str, ...] = ("OVERWRITE", "MERGE")  # FULL, KEY_MERGE
TRANSFORM_TYPES: tuple[str, ...] = ("SQL", "PYSPARK")

TARGET_TYPE = re.compile(
    r"^(string|boolean|tinyint|smallint|int|bigint|float|double|date|timestamp|decimal\(\d{1,2},\s*\d{1,2}\))$", re.IGNORECASE
)
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_ORDER_TERM = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)(?:\s+(asc|desc))?\s*$", re.IGNORECASE)


def is_ident(value: str) -> bool:
    return bool(_IDENT.match(value))


def parse_order(spec: str) -> list[tuple[str, bool]] | None:
    """'ModifiedDate desc, Id' -> [('ModifiedDate', True), ('Id', False)] (True = descending); None if malformed."""
    out: list[tuple[str, bool]] = []
    for term in spec.split(","):
        m = _ORDER_TERM.match(term)
        if not m:
            return None
        out.append((m.group(1), (m.group(2) or "asc").lower() == "desc"))
    return out


def check_cleanse_rules(rules: Any) -> list[str]:
    """Problems with a cfg_column.cleanse_rules value (empty list = valid). Shape: {} or {"ops": [{"op": name, ...params}]}."""
    if rules in (None, {}):
        return []
    if not isinstance(rules, dict) or set(rules) != {"ops"} or not isinstance(rules["ops"], list):
        return ["cleanse_rules must be an object with a single key 'ops' holding a list of operations"]
    problems: list[str] = []
    for i, op in enumerate(rules["ops"]):
        if not isinstance(op, dict) or "op" not in op:
            problems.append(f"ops[{i}] must be an object with an 'op' key")
            continue
        name = op["op"]
        if name not in CLEANSE_OPS:
            problems.append(f"ops[{i}]: operation '{name}' is not in the allowlist {sorted(CLEANSE_OPS)}")
            continue
        if name in ("parse_date", "parse_timestamp") and i != len(rules["ops"]) - 1:
            problems.append(f"ops[{i}]: {name} must be the last operation")
        required, types = CLEANSE_OPS[name]
        extra = set(op) - {"op", *required}
        if extra:
            problems.append(f"ops[{i}] ({name}): unknown parameters {sorted(extra)}")
        for p in required:
            if p not in op:
                problems.append(f"ops[{i}] ({name}): missing parameter '{p}'")
            elif not isinstance(op[p], types[p]):
                problems.append(f"ops[{i}] ({name}): parameter '{p}' has the wrong type")
    return problems


def check_params(params: Any) -> list[str]:
    """Gold transformation params: {name: {"type": one of PARAM_TYPES, "value": scalar}}."""
    if params in (None, {}):
        return []
    if not isinstance(params, dict):
        return ["params must be an object"]
    problems: list[str] = []
    for name, spec in params.items():
        if not is_ident(str(name)):
            problems.append(f"parameter name '{name}' is not a valid identifier")
        if not isinstance(spec, dict) or set(spec) != {"type", "value"} or spec.get("type") not in PARAM_TYPES:
            problems.append(f"parameter '{name}' must be {{type: {'|'.join(PARAM_TYPES)}, value: <scalar>}}")
        elif isinstance(spec["value"], (dict, list)):
            problems.append(f"parameter '{name}': value must be a scalar")
    return problems
