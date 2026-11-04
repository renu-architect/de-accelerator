"""TRANSFORM rules (MDV-7xxx): Bronze->Silver->Gold settings (M3). Active only for packs with metadata_schema_version >= 3, so
v2 packs keep validating exactly as before. Principle (as in M2): reject what the engine does not implement instead of ignoring it."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator

from de_accelerator.core import transform_spec as ts
from de_accelerator.metadata.validation.engine import ValidationContext, rule

L = "SEMANTIC"
E = "ERROR"


def _active(ctx: ValidationContext) -> bool:
    return int(ctx.bundle.manifest["metadata_schema_version"]) >= 3


def _objs(ctx: ValidationContext) -> dict[tuple[str, str], dict]:  # type: ignore[type-arg]
    return {(o["project_code"], o["code"]): o for o in ctx.rows("object")}


def _cols(ctx: ValidationContext) -> dict[tuple[str, str], list[dict]]:  # type: ignore[type-arg]
    out: dict[tuple[str, str], list[dict]] = defaultdict(list)  # type: ignore[type-arg]
    for c in ctx.rows("column"):
        out[(c["project_code"], c["object"])].append(c)
    return out


def _maps(ctx: ValidationContext, layer: str) -> Iterator[dict]:  # type: ignore[type-arg]
    return (m for m in ctx.rows("layer_mapping") if m["layer"] == layer)


def _loc(ctx: ValidationContext, m: dict) -> str:  # type: ignore[type-arg]
    return ctx.loc("layer_mapping", m, "object", "layer")


@rule("MDV-7001", E, L, "Silver mapping needs declared columns or column_mode PASSTHROUGH")
def silver_columns(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    cols = _cols(ctx)
    for m in _maps(ctx, "SILVER"):
        if not cols.get((m["project_code"], m["object"])) and (m["column_mode"] or "DECLARED") != "PASSTHROUGH":
            yield "Silver needs declared columns (cfg_column) or an explicit column_mode: PASSTHROUGH", _loc(ctx, m)


@rule("MDV-7002", E, L, "column_mode PASSTHROUGH cannot be combined with declared columns")
def passthrough_with_columns(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    cols = _cols(ctx)
    for m in _maps(ctx, "SILVER"):
        if m["column_mode"] == "PASSTHROUGH" and cols.get((m["project_code"], m["object"])):
            yield "PASSTHROUGH takes all Bronze columns unchanged; remove the declared columns or use DECLARED", _loc(ctx, m)


@rule("MDV-7003", E, L, "effective_column / dedupe_keys / scd_columns must be declared columns (source names)")
def column_refs(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    cols = _cols(ctx)
    for m in _maps(ctx, "SILVER"):
        declared = {c["name"] for c in cols.get((m["project_code"], m["object"]), [])}
        if not declared:
            continue
        refs = ([m["effective_column"]] if m["effective_column"] else []) + list(m["dedupe_keys"]) + list(m["scd_columns"])
        for ref in refs:
            if ref not in declared:
                yield f"'{ref}' is not a declared column of '{m['object']}'", _loc(ctx, m)


@rule("MDV-7004", E, L, "dedupe_order must be '<declared column> [asc|desc]' terms")
def dedupe_order(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    cols = _cols(ctx)
    for m in _maps(ctx, "SILVER"):
        if not m["dedupe_order"]:
            continue
        terms = ts.parse_order(m["dedupe_order"])
        declared = {c["name"] for c in cols.get((m["project_code"], m["object"]), [])}
        if terms is None:
            yield f"dedupe_order '{m['dedupe_order']}' must be a comma list of '<column> [asc|desc]'", _loc(ctx, m)
        elif declared:
            for name, _ in terms:
                if name not in declared:
                    yield f"dedupe_order column '{name}' is not declared", _loc(ctx, m)


@rule("MDV-7005", E, L, "target_name must be a valid identifier and unique within the object")
def target_names(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    for (_, obj), cols in _cols(ctx).items():
        seen: dict[str, str] = {}
        for c in cols:
            tgt = c["target_name"] or c["name"]
            if not ts.is_ident(tgt) or tgt.startswith("_"):
                yield (
                    f"target name '{tgt}' is not a valid identifier (the '_' prefix is reserved)",
                    ctx.loc("column", c, "object", "name"),
                )
            elif tgt.lower() in seen:
                yield (
                    f"target name '{tgt}' of '{obj}' collides with column '{seen[tgt.lower()]}'",
                    ctx.loc("column", c, "object", "name"),
                )
            seen.setdefault(tgt.lower(), c["name"])


@rule("MDV-7006", E, L, "cleanse_rules must use only allowlisted operations with valid parameters")
def cleanse_rules(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    for c in ctx.rows("column"):
        for problem in ts.check_cleanse_rules(c["cleanse_rules"]):
            yield problem, ctx.loc("column", c, "object", "name")


@rule("MDV-7007", E, L, "delete_mode 'hard' is only valid for SCD1/MERGE Silver mappings")
def hard_delete(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    objs = _objs(ctx)
    for m in _maps(ctx, "SILVER"):
        o = objs.get((m["project_code"], m["object"]))
        if o and o["delete_mode"] == "hard" and m["merge_type"] in ("SCD2", "APPEND"):
            yield f"hard delete would destroy history/append-only data ({m['merge_type']}); use soft or ignore", _loc(ctx, m)


@rule("MDV-7008", E, L, "Gold mapping must be OVERWRITE (FULL) or MERGE (KEY_MERGE)")
def gold_merge_type(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    for m in _maps(ctx, "GOLD"):
        if m["merge_type"] not in ts.GOLD_MERGE_TYPES:
            yield f"Gold merge_type '{m['merge_type']}' is not supported (use OVERWRITE or MERGE)", _loc(ctx, m)


@rule("MDV-7009", E, L, "Gold object needs at least one transformation step")
def gold_has_transform(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    have = {(t["project_code"], t["object"]) for t in ctx.rows("transformation") if t["layer"] == "GOLD"}
    for m in _maps(ctx, "GOLD"):
        if (m["project_code"], m["object"]) not in have:
            yield "Gold object has no GOLD transformation step", _loc(ctx, m)


@rule("MDV-7010", E, L, "transformation must be a reviewed SQL/PYSPARK file (no BUILTIN, no inline_sql)")
def transform_kind(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    for t in ctx.rows("transformation"):
        loc = ctx.loc("transformation", t, "object", "layer", "step_no")
        if t["type"] not in ts.TRANSFORM_TYPES:
            yield f"transformation type '{t['type']}' is not supported (SQL or PYSPARK files only)", loc
        elif t["inline_sql"]:
            yield "inline_sql is not allowed: SQL must live in a reviewed, checksum-verified file (body_ref)", loc
        elif not t["body_ref"]:
            yield "transformation needs body_ref (relative path under transform_root)", loc
        elif t["body_ref"].startswith("/") or ".." in t["body_ref"].replace("\\", "/").split("/"):
            yield "body_ref must be a relative path without '..'", loc


@rule("MDV-7011", E, L, "Gold inputs must be objects enabled for SILVER or GOLD")
def gold_inputs(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    objs = _objs(ctx)
    for t in ctx.rows("transformation"):
        if t["layer"] != "GOLD":
            continue
        for inp in t["input_objects"]:
            o = objs.get((t["project_code"], inp))
            if o is not None and not ({"SILVER", "GOLD"} & set(o["enabled_layers"])):
                yield (
                    f"input '{inp}' is not enabled for SILVER or GOLD",
                    ctx.loc("transformation", t, "object", "layer", "step_no"),
                )


@rule("MDV-7012", E, L, "transformation params must be typed scalars")
def transform_params(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    for t in ctx.rows("transformation"):
        for problem in ts.check_params(t["params"]):
            yield problem, ctx.loc("transformation", t, "object", "layer", "step_no")


@rule("MDV-7013", E, L, "only NOT_NULL/VALID_VALUES/RANGE data-quality rules on SILVER are executed")
def dq_supported(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    for r in ctx.rows("dq_rule"):
        loc = ctx.loc("dq_rule", r, "object", "code")
        if r["rule_type"] not in ts.DQ_RULE_TYPES:
            yield f"rule_type {r['rule_type']} is not implemented (supported: {', '.join(ts.DQ_RULE_TYPES)})", loc
        elif r["layer"] != "SILVER":
            yield "data-quality rules are only executed on the SILVER layer", loc
        elif r["action"] not in ts.DQ_ACTIONS:
            yield f"action {r['action']} is not supported", loc
        elif r["rule_type"] == "VALID_VALUES" and not isinstance((r["params"] or {}).get("values"), list):
            yield "VALID_VALUES needs params.values (list)", loc
        elif r["rule_type"] == "RANGE" and not ({"min", "max"} & set(r["params"] or {})):
            yield "RANGE needs params.min and/or params.max", loc


@rule("MDV-7014", E, L, "layer_mapping.target_table must agree with cfg_object target_<layer>")
def target_authority(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    objs = _objs(ctx)
    for m in ctx.rows("layer_mapping"):
        o = objs.get((m["project_code"], m["object"]))
        legacy = o.get(f"target_{m['layer'].lower()}") if o else None
        if m["target_table"] and legacy and m["target_table"] != legacy:
            yield f"target_table '{m['target_table']}' differs from object target '{legacy}'", _loc(ctx, m)


@rule("MDV-7015", E, L, "Silver MERGE/SCD and Gold MERGE need object business_keys")
def business_keys(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    objs = _objs(ctx)
    for m in ctx.rows("layer_mapping"):
        if m["layer"] == "BRONZE" or m["merge_type"] not in ("MERGE", "SCD1", "SCD2"):
            continue
        o = objs.get((m["project_code"], m["object"]))
        if o and not o["business_keys"]:
            yield f"{m['merge_type']} needs cfg_object.business_keys", _loc(ctx, m)


@rule("MDV-7016", E, L, "target_type must be one of the supported Spark types")
def target_types(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not _active(ctx):
        return
    for c in ctx.rows("column"):
        if c["target_type"] and not ts.TARGET_TYPE.match(c["target_type"]):
            yield (
                f"target_type '{c['target_type']}' is not supported (string, boolean, tinyint, smallint, int, bigint, float, double, date, timestamp, decimal(p,s))",
                ctx.loc("column", c, "object", "name"),
            )
