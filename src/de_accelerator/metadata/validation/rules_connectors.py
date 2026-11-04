"""CONNECTOR rules (MDV-6xxx, M2): layout consistency for ingestion + connector-specific static checks."""

from __future__ import annotations

from collections.abc import Iterator

from de_accelerator.metadata.reader.models import from_row
from de_accelerator.metadata.validation.engine import CONNECTOR_VALIDATORS, ValidationContext, rule

L = "SEMANTIC"
E = "ERROR"
_FAMILY = {"sqlserver": "jdbc", "azuresql": "jdbc", "postgresql": "jdbc", "oracle": "jdbc", "mysql": "jdbc",
           "csv": "file", "json": "file", "parquet": "file", "avro": "file", "xml": "file", "excel": "file", "rest": "rest", "mock": "mock"}  # fmt: skip


@rule("MDV-6001", E, L, "source connector_type inconsistent with source_type")
def connector_family(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for s in ctx.rows("source"):
        fam = _FAMILY.get(s["source_type"])
        if s["connector_type"] and fam and s["connector_type"] != fam:
            yield f"connector_type '{s['connector_type']}' does not match source_type '{s['source_type']}' (expected '{fam}')", ctx.loc("source", s)


@rule("MDV-6002", E, L, "layer target table must live in the schema of its layer")
def layer_schema(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for m in ctx.rows("layer_mapping"):
        t = m["target_table"]
        if t and not t.startswith(m["layer"].lower() + "."):
            yield f"target_table '{t}' must start with '{m['layer'].lower()}.' for layer {m['layer']}", ctx.loc("layer_mapping", m, "object", "layer")
    for o in ctx.rows("object"):
        for layer in ("bronze", "silver", "gold"):
            t = o[f"target_{layer}"]
            if t and not t.startswith(layer + "."):
                yield f"target_{layer} '{t}' must start with '{layer}.'", ctx.loc("object", o)


@rule("MDV-6003", E, L, "declared source column uses the reserved '_' prefix")
def reserved_columns(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for c in ctx.rows("column"):
        if c["name"].startswith("_"):
            yield f"column '{c['name']}' starts with '_' (reserved for Bronze audit columns)", ctx.loc("column", c, "object", "name")


@rule("MDV-6010", E, L, "connector-specific configuration invalid")
def connector_specific(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    objs = {(o["project_code"], o["code"]): o for o in ctx.rows("object")}
    for s in ctx.rows("source"):
        fn = CONNECTOR_VALIDATORS.get(s["source_type"])
        if fn is None:
            continue
        triples = []
        for (p, _code), o in objs.items():
            if p != s["project_code"] or o["source"] != s["code"]:
                continue
            cols = [from_row("column", c) for c in ctx.rows("column") if c["project_code"] == p and c["object"] == o["code"]]
            ls = [r for r in ctx.rows("load_strategy") if r["project_code"] == p and r["object"] == o["code"]]
            if ls:
                triples.append((from_row("object", o), cols, from_row("load_strategy", ls[0])))
        for f in fn(from_row("source", s), triples):
            yield f.message, f.location


@rule("MDV-6004", E, L, "ingestion setting accepted by the schema but not implemented")
def unsupported_ingestion_settings(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    """Reject (instead of silently ignoring) Bronze/ingestion settings the engine does not implement yet."""
    objs = {(o["project_code"], o["code"]): o for o in ctx.rows("object")}
    for ls in ctx.rows("load_strategy"):
        o = objs.get((ls["project_code"], ls["object"]))
        if not o or "BRONZE" not in o["enabled_layers"]:
            continue
        if o["partition_cols"]:
            yield "Bronze partitioning is not implemented: partition_cols would be ignored", ctx.loc("object", o)
        if ls["write_mode"] not in (None, "", "APPEND"):
            yield f"write_mode '{ls['write_mode']}' is not implemented (Bronze is append-only history)", ctx.loc("load_strategy", ls, "object")
        if ls["backfill_from"] or ls["backfill_to"]:
            yield "backfill_from/backfill_to are not implemented; use `de-accel reprocess` for an explicit window", ctx.loc("load_strategy", ls, "object")
