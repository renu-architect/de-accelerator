"""Cleansing operations (closed allowlist, see core.transform_spec). Pure functions of Spark columns; no free-form SQL."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pyspark.sql import Column
from pyspark.sql import functions as F

from de_accelerator.core.transform_spec import CLEANSE_OPS
from de_accelerator.processing.common.errors import TransformConfigError

PARSE_OPS = ("parse_date", "parse_timestamp")


def _one(col: Column, op: dict[str, Any]) -> Column:
    name = op["op"]
    if name == "trim":
        return F.trim(col)
    if name == "upper":
        return F.upper(col)
    if name == "lower":
        return F.lower(col)
    if name == "initcap":
        return F.initcap(col)
    if name == "nullif":
        return F.when(col.isin(*[str(v) for v in op["values"]]), F.lit(None).cast("string")).otherwise(col)
    if name == "replace":
        return F.replace(col, F.lit(op["from"]), F.lit(op["to"]))  # literal replacement, never a regex
    if name == "default":
        return F.coalesce(col, F.lit(str(op["value"])))
    if name == "map_values":
        pairs = [x for k, v in op["map"].items() for x in (F.lit(str(k)), F.lit(str(v)))]
        return F.coalesce(F.try_element_at(F.create_map(*pairs), col), col)
    raise TransformConfigError(
        f"cleansing operation '{name}' is not in the allowlist"
    )  # pragma: no cover - validator rejects it first


def apply_ops(col: Column, ops: Sequence[dict[str, Any]]) -> tuple[Column, Column]:
    """Apply the operations in order. Returns (pre_parse, final): `pre_parse` is the value before a trailing parse_* operation
    (used to tell "was null" from "failed to parse"); both are the same when there is no parse operation."""
    for op in ops:
        if op["op"] not in CLEANSE_OPS:
            raise TransformConfigError(f"cleansing operation '{op['op']}' is not in the allowlist")
    if not ops:
        return col, col
    cur = col.cast("string")
    pre = cur
    for i, op in enumerate(ops):
        if op["op"] in PARSE_OPS:
            if i != len(ops) - 1:
                raise TransformConfigError("parse_date/parse_timestamp must be the last operation")
            pre = cur
            fmt = F.lit(op["format"])
            cur = F.try_to_timestamp(cur, fmt).cast("date") if op["op"] == "parse_date" else F.try_to_timestamp(cur, fmt)
        else:
            cur = _one(cur, op)
            pre = cur
    return pre, cur
