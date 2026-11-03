"""SEMANTIC rules (MDV-3xxx): load strategies, merge types, DAG, DQ/recon parameters, governance consistency."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterator

from de_accelerator.metadata.validation.engine import ValidationContext, rule

L = "SEMANTIC"
E, W = "ERROR", "WARNING"
_ISO_DURATION = re.compile(r"^P(?!$)(\d+Y)?(\d+M)?(\d+W)?(\d+D)?(T(?=\d)(\d+H)?(\d+M)?(\d+S)?)?$")


def _objs(ctx: ValidationContext) -> dict[tuple[str, str], dict]:  # type: ignore[type-arg]
    return {(o["project_code"], o["code"]): o for o in ctx.rows("object")}


@rule("MDV-3001", E, L, "INCREMENTAL load requires watermark_column")
def incr_watermark(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("load_strategy"):
        if r["load_type"] == "INCREMENTAL" and not r["watermark_column"]:
            yield "INCREMENTAL requires watermark_column", ctx.loc("load_strategy", r, "object")


@rule("MDV-3002", E, L, "CDC load requires cdc_op_column and cdc_seq_column")
def cdc_columns(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("load_strategy"):
        if r["load_type"] == "CDC" and not (r["cdc_op_column"] and r["cdc_seq_column"]):
            yield "CDC requires cdc_op_column and cdc_seq_column", ctx.loc("load_strategy", r, "object")


@rule("MDV-3003", E, L, "FILE_INCR requires a file/document object")
def file_incr_kind(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    o = _objs(ctx)
    for r in ctx.rows("load_strategy"):
        obj = o.get((r["project_code"], r["object"]))
        if r["load_type"] == "FILE_INCR" and obj and obj["object_kind"] not in ("file", "document"):
            yield f"FILE_INCR needs object_kind file|document (is '{obj['object_kind']}')", ctx.loc("load_strategy", r, "object")


@rule("MDV-3004", E, L, "API load requires api object, rest source and page_config")
def api_load(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    o = _objs(ctx)
    src = {(s["project_code"], s["code"]): s for s in ctx.rows("source")}
    for r in ctx.rows("load_strategy"):
        if r["load_type"] != "API":
            continue
        obj = o.get((r["project_code"], r["object"]))
        if obj and obj["object_kind"] != "api":
            yield "API load needs object_kind 'api'", ctx.loc("load_strategy", r, "object")
        s = src.get((r["project_code"], obj["source"])) if obj else None
        if s and s["source_type"] != "rest":
            yield "API load needs a 'rest' source", ctx.loc("load_strategy", r, "object")
        if not r["page_config"]:
            yield "API load requires page_config (pagination)", ctx.loc("load_strategy", r, "object")


@rule("MDV-3005", E, L, "MERGE/SCD merge types require business keys")
def merge_needs_keys(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    o = _objs(ctx)
    for m in ctx.rows("layer_mapping"):
        obj = o.get((m["project_code"], m["object"]))
        if m["merge_type"] in ("MERGE", "SCD1", "SCD2") and obj and not (obj["business_keys"] or m["dedupe_keys"]):
            yield f"{m['merge_type']} needs object business_keys or mapping dedupe_keys", ctx.loc("layer_mapping", m, "object", "layer")


@rule("MDV-3006", E, L, "SCD2 requires scd_columns")
def scd2_columns(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for m in ctx.rows("layer_mapping"):
        if m["merge_type"] == "SCD2" and not m["scd_columns"]:
            yield "SCD2 requires scd_columns (tracked attributes)", ctx.loc("layer_mapping", m, "object", "layer")


@rule("MDV-3007", E, L, "object depends on itself")
def self_dependency(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for d in ctx.rows("dependency"):
        if d["object"] == d["depends_on"]:
            yield "object cannot depend on itself", ctx.loc("dependency", d, "object", "depends_on")


@rule("MDV-3008", E, L, "dependency cycle")
def dependency_cycle(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    graph: dict[tuple[str, str], set[str]] = defaultdict(set)
    for d in ctx.rows("dependency"):
        if d["object"] != d["depends_on"]:
            graph[(d["project_code"], d["object"])].add(d["depends_on"])
    state: dict[tuple[str, str], int] = {}
    reported: set[str] = set()

    def visit(p: str, n: str, path: list[str]) -> Iterator[str]:
        key = (p, n)
        if state.get(key) == 1:
            cyc = path[path.index(n):] + [n] if n in path else path + [n]
            yield " -> ".join(cyc)
            return
        if state.get(key) == 2:
            return
        state[key] = 1
        for nxt in sorted(graph.get(key, ())):
            yield from visit(p, nxt, path + [n])
        state[key] = 2

    for (p, n) in sorted(graph):
        for cyc in visit(p, n, []):
            if cyc not in reported:
                reported.add(cyc)
                yield f"dependency cycle: {cyc}", f"dependency:{p}"


@rule("MDV-3009", E, L, "enabled layer has no layer mapping")
def layers_mapped(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    have = {(m["project_code"], m["object"], m["layer"]) for m in ctx.rows("layer_mapping")}
    for o in ctx.rows("object"):
        for layer in o["enabled_layers"]:
            if (o["project_code"], o["code"], layer) not in have:
                yield f"layer {layer} enabled but no layer_mapping defined", ctx.loc("object", o)


@rule("MDV-3010", E, L, "layer mapping for a layer that is not enabled")
def mapping_layer_enabled(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    o = _objs(ctx)
    for m in ctx.rows("layer_mapping"):
        obj = o.get((m["project_code"], m["object"]))
        if obj and m["layer"] not in obj["enabled_layers"]:
            yield f"layer {m['layer']} is not in enabled_layers of '{obj['code']}'", ctx.loc("layer_mapping", m, "object", "layer")


@rule("MDV-3011", E, L, "DQ threshold outside 0..100")
def dq_threshold(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("dq_rule"):
        if not 0 <= (r["threshold_pct"] or 0) <= 100:
            yield f"threshold_pct {r['threshold_pct']} not in [0,100]", ctx.loc("dq_rule", r, "object", "code")


@rule("MDV-3012", E, L, "DQ rule parameters invalid for rule_type")
def dq_params(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("dq_rule"):
        t, p, cols, loc = r["rule_type"], r["params"] or {}, r["columns"], ctx.loc("dq_rule", r, "object", "code")
        need_cols = {"NOT_NULL", "DUPLICATE", "UNIQUE", "VALID_VALUES", "DATA_TYPE", "FORMAT", "RANGE", "FRESHNESS",
                     "COMPLETENESS", "REF_INTEGRITY"}  # fmt: skip
        if t in need_cols and not cols:
            yield f"{t} requires 'columns'", loc
        if t == "RANGE" and not ({"min", "max"} & set(p)):
            yield "RANGE requires params.min and/or params.max", loc
        if t == "RANGE" and {"min", "max"} <= set(p) and p["min"] > p["max"]:
            yield "RANGE params.min > params.max", loc
        if t == "FORMAT" and "pattern" not in p:
            yield "FORMAT requires params.pattern", loc
        if t == "FORMAT" and "pattern" in p:
            try:
                re.compile(p["pattern"])
            except (re.error, TypeError):
                yield "FORMAT params.pattern is not a valid regex", loc
        if t == "VALID_VALUES" and not p.get("values"):
            yield "VALID_VALUES requires params.values", loc
        if t == "DATA_TYPE" and "type" not in p:
            yield "DATA_TYPE requires params.type", loc
        if t == "FRESHNESS" and "max_age" not in p:
            yield "FRESHNESS requires params.max_age (ISO-8601 duration)", loc
        if t == "REF_INTEGRITY" and not ({"ref_object", "ref_column"} <= set(p)):
            yield "REF_INTEGRITY requires params.ref_object and params.ref_column", loc
        if t in ("CUSTOM_SQL", "CUSTOM_PYSPARK", "BUSINESS_RULE") and not r["expression"]:
            yield f"{t} requires 'expression'", loc


@rule("MDV-3013", W, L, "FAIL_BATCH action on INFO severity rule")
def dq_fail_info(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("dq_rule"):
        if r["action"] == "FAIL_BATCH" and r["severity"] == "INFO":
            yield "action FAIL_BATCH with severity INFO is contradictory", ctx.loc("dq_rule", r, "object", "code")


@rule("MDV-3014", E, L, "reconciliation tolerance invalid")
def recon_tolerances(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("recon_rule"):
        if (r["tolerance_abs"] or 0) < 0 or not 0 <= (r["tolerance_pct"] or 0) <= 100:
            yield "tolerance_abs must be >= 0 and tolerance_pct within [0,100]", ctx.loc("recon_rule", r, "object", "code")
        if r["check_type"] in ("SUM", "NULL_COUNT", "DISTINCT_COUNT", "HASH") and not r["columns"]:
            yield f"{r['check_type']} reconciliation requires 'columns'", ctx.loc("recon_rule", r, "object", "code")


@rule("MDV-3015", W, L, "financial reconciliation should fail the batch")
def recon_financial(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("recon_rule"):
        if (r["is_financial"] or r["check_type"] == "FINANCIAL") and r["on_fail"] != "FAIL_BATCH":
            yield "financial reconciliation should use on_fail FAIL_BATCH", ctx.loc("recon_rule", r, "object", "code")


@rule("MDV-3016", E, L, "backfill range invalid")
def backfill_range(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("load_strategy"):
        a, b = r["backfill_from"], r["backfill_to"]
        if bool(a) != bool(b):
            yield "backfill_from and backfill_to must be given together", ctx.loc("load_strategy", r, "object")
        elif a and b and str(a) > str(b):
            yield "backfill_from is after backfill_to", ctx.loc("load_strategy", r, "object")


@rule("MDV-3017", E, L, "retry policy values invalid")
def retry_values(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("retry_policy"):
        if r["max_attempts"] < 1 or r["backoff_sec"] < 0 or r["backoff_mult"] < 1:
            yield "need max_attempts>=1, backoff_sec>=0, backoff_mult>=1", ctx.loc("retry_policy", r)


@rule("MDV-3018", E, L, "execution scope selects nothing")
def execution_scope(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("execution"):
        s = r["scope"] or {}
        if not any(s.get(k) for k in ("objects", "tags", "layers", "sources")):
            yield "scope must select objects/tags/layers/sources", ctx.loc("execution", r)
        if (r["concurrency"] or 0) < 1:
            yield "concurrency must be >= 1", ctx.loc("execution", r)


@rule("MDV-3019", E, L, "execution scope refers to unknown object")
def execution_scope_objects(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    known = {(o["project_code"], o["code"]) for o in ctx.rows("object")}
    for r in ctx.rows("execution"):
        for name in (r["scope"] or {}).get("objects", []):
            if (r["project_code"], name) not in known:
                yield f"scope.objects '{name}' is not a known object", ctx.loc("execution", r)


@rule("MDV-3020", E, L, "retention policy invalid")
def retention_valid(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("retention_policy"):
        if r["retain_days"] <= 0:
            yield "retain_days must be > 0", ctx.loc("retention_policy", r)
        if r["scope_kind"] == "OBJECT" and not r["object"]:
            yield "scope_kind OBJECT requires 'object'", ctx.loc("retention_policy", r)
        if r["scope_kind"] == "LAYER" and not r["layer"]:
            yield "scope_kind LAYER requires 'layer'", ctx.loc("retention_policy", r)


@rule("MDV-3022", E, L, "personal-data object lacks subject-key mapping")
def personal_needs_subject_key(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    by_obj: dict[tuple[str, str], list[dict]] = defaultdict(list)  # type: ignore[type-arg]
    for s in ctx.rows("subject_key"):
        by_obj[(s["project_code"], s["object"])].append(s)
    for o in ctx.rows("object"):
        if not o["personal_data"]:
            continue
        maps = by_obj.get((o["project_code"], o["code"]), [])
        ok = any((not m["exempt"]) or (m["exempt"] and m["exempt_reason"]) for m in maps)
        if not ok:
            yield "personal_data object needs a subject_key mapping (or an exemption with exempt_reason)", ctx.loc("object", o)
        for m in maps:
            if m["exempt"] and not m["exempt_reason"]:
                yield "exempt subject_key needs exempt_reason", ctx.loc("subject_key", m, "object", "column_name")


@rule("MDV-3023", W, L, "personal-data object has no retention policy")
def personal_needs_retention(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    have = {(r["project_code"], r["object"]) for r in ctx.rows("retention_policy") if r["object"]}
    for o in ctx.rows("object"):
        if o["personal_data"] and (o["project_code"], o["code"]) not in have:
            yield "personal_data object has no object-level retention_policy", ctx.loc("object", o)


@rule("MDV-3024", E, L, "transformation steps not contiguous from 1")
def transform_steps(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    steps: dict[tuple[str, str, str], list[int]] = defaultdict(list)
    for t in ctx.rows("transformation"):
        steps[(t["project_code"], t["object"], t["layer"])].append(t["step_no"])
    for (p, o, layer), nums in steps.items():
        if sorted(nums) != list(range(1, len(nums) + 1)):
            yield f"step_no values {sorted(nums)} must be 1..{len(nums)} without gaps/duplicates", f"transformation:{p}/{o}/{layer}"


@rule("MDV-3025", E, L, "transformation body missing")
def transform_body(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for t in ctx.rows("transformation"):
        loc = ctx.loc("transformation", t, "object", "layer", "step_no")
        if t["type"] == "SQL" and not (t["body_ref"] or t["inline_sql"]):
            yield "SQL transformation needs body_ref or inline_sql", loc
        if t["type"] == "PYSPARK" and not t["body_ref"]:
            yield "PYSPARK transformation needs body_ref", loc


@rule("MDV-3026", W, L, "CDC retention safeguard incomplete")
def cdc_retention(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("load_strategy"):
        if r["load_type"] == "CDC" and r["cdc_retention_check"] and not r["max_replay_age"]:
            yield "CDC with cdc_retention_check should declare max_replay_age (source change-log retention horizon)", ctx.loc("load_strategy", r, "object")
        if r["load_type"] == "CDC" and not r["cdc_retention_check"]:
            yield "CDC load has cdc_retention_check disabled: retention gaps would go undetected", ctx.loc("load_strategy", r, "object")


@rule("MDV-3027", E, L, "invalid ISO-8601 duration")
def durations(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("load_strategy"):
        for fld in ("lookback_window", "late_arrival_window", "max_replay_age"):
            if r[fld] and not _ISO_DURATION.match(r[fld]):
                yield f"{fld} '{r[fld]}' is not an ISO-8601 duration (e.g. PT2H, P7D)", ctx.loc("load_strategy", r, "object")


@rule("MDV-3028", E, L, "layer mapping has no target table")
def mapping_target(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    o = _objs(ctx)
    for m in ctx.rows("layer_mapping"):
        obj = o.get((m["project_code"], m["object"]))
        fallback = obj.get(f"target_{m['layer'].lower()}") if obj else None
        if not (m["target_table"] or fallback):
            yield "no target_table on mapping and no object-level target for this layer", ctx.loc("layer_mapping", m, "object", "layer")


@rule("MDV-3029", E, L, "semantic target needs entities")
def semantic_target_entities(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for t in ctx.rows("semantic_target"):
        if not t["entities"]:
            yield "semantic_target must list entities", ctx.loc("semantic_target", t)


@rule("MDV-3030", E, L, "environment default_concurrency invalid")
def env_concurrency(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for e in ctx.rows("environment"):
        if (e["default_concurrency"] or 0) < 1:
            yield "default_concurrency must be >= 1", ctx.loc("environment", e)


@rule("MDV-3033", E, L, "source needs connection details for its auth mode")
def source_connection(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for s in ctx.rows("source"):
        if s["source_type"] != "mock" and s["auth_mode"] != "NONE" and not s["connection"]:
            yield f"auth_mode {s['auth_mode']} requires a non-empty connection (kv: references)", ctx.loc("source", s)


@rule("MDV-3034", E, L, "ingestable object has no load strategy")
def load_strategy_present(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    have = {(r["project_code"], r["object"]) for r in ctx.rows("load_strategy")}
    for o in ctx.rows("object"):
        if o["object_kind"] != "derived" and (o["project_code"], o["code"]) not in have:
            yield "object has no load_strategy", ctx.loc("object", o)
