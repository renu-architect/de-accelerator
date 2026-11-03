"""REFERENTIAL rules (MDV-2xxx): references resolve, keys/columns exist, files + checksums match."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator

from de_accelerator.core.naming import catalog_name
from de_accelerator.metadata.schema.entities import BY_KIND, ENTITIES
from de_accelerator.metadata.validation.engine import ValidationContext, rule

L = "REFERENTIAL"
E, W = "ERROR", "WARNING"


@rule("MDV-2001", E, L, "reference to unknown entity")
def dangling_refs(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    idx = {k: {(r.get("project_code"), r["code"]) for r in ctx.rows(k)} for k in BY_KIND if "code" in BY_KIND[k].key}
    for e in ENTITIES:
        for row in ctx.rows(e.kind):
            for f in e.fields:
                if not f.ref:
                    continue
                vals = row.get(f.name)
                for v in (vals if f.type == "list" else [vals]) if vals else []:
                    if (row.get("project_code"), v) not in idx.get(f.ref, set()):
                        yield f"{e.kind}.{f.name} refers to unknown {f.ref} '{v}'", ctx.loc(e.kind, row, *e.key)


@rule("MDV-2002", E, L, "pack defines no project")
def has_project(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not ctx.rows("project"):
        yield "pack must define at least one project (projects/<code>/project.yaml)", "pack"


@rule("MDV-2003", E, L, "environment row missing for target environment")
def env_row_present(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    projects = {r["code"] for r in ctx.rows("project")}
    have = {r["project_code"] for r in ctx.rows("environment") if r["code"] == ctx.env}
    for p in sorted(projects - have):
        yield f"project '{p}' has no environment entry for {ctx.env}", f"environment:{p}/{ctx.env}"


@rule("MDV-2004", E, L, "catalog name violates <client>_<env> convention")
def catalog_convention(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("environment"):
        if r["catalog_name"]:
            try:
                expected = catalog_name(r["client_code"], r["code"])
            except ValueError as exc:
                yield str(exc), ctx.loc("environment", r)
                continue
            if r["catalog_name"] != expected:
                yield f"catalog_name '{r['catalog_name']}' must be '{expected}'", ctx.loc("environment", r)


@rule("MDV-2005", E, L, "business/primary key column not declared in columns")
def key_columns_declared(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for o in ctx.rows("object"):
        cols = ctx.columns_of(o["project_code"], o["code"])
        if cols is None:
            continue
        for fld in ("business_keys", "primary_keys", "partition_cols"):
            for c in o[fld]:
                if c not in cols:
                    yield f"{fld} column '{c}' is not declared in columns of '{o['code']}'", ctx.loc("object", o)


@rule("MDV-2006", E, L, "DQ rule column not declared in object columns")
def dq_columns(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("dq_rule"):
        cols = ctx.columns_of(r["project_code"], r["object"])
        if cols is None:
            continue
        for c in r["columns"]:
            if c not in cols:
                yield f"rule column '{c}' not declared on object '{r['object']}'", ctx.loc("dq_rule", r, "object", "code")


@rule("MDV-2007", E, L, "duplicate column ordinal")
def column_ordinals(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    seen = Counter((r["project_code"], r["object"], r["ordinal"]) for r in ctx.rows("column") if r["ordinal"] is not None)
    for (p, o, n), c in seen.items():
        if c > 1:
            yield f"ordinal {n} used {c} times in object '{o}'", f"column:{p}/{o}"


@rule("MDV-2008", E, L, "foreign key definition inconsistent")
def fk_consistency(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("column"):
        if bool(r["fk_object"]) != bool(r["fk_column"]):
            yield "fk_object and fk_column must be given together", ctx.loc("column", r, "object", "name")
        elif r["fk_object"]:
            cols = ctx.columns_of(r["project_code"], r["fk_object"])
            if cols is not None and r["fk_column"] not in cols:
                yield f"fk_column '{r['fk_column']}' not declared on '{r['fk_object']}'", ctx.loc("column", r, "object", "name")


@rule("MDV-2009", E, L, "subject key column not declared")
def subject_columns(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("subject_key"):
        cols = ctx.columns_of(r["project_code"], r["object"])
        if cols is not None and r["column_name"] not in cols:
            yield f"column '{r['column_name']}' not declared on '{r['object']}'", ctx.loc("subject_key", r, "object", "column_name")


@rule("MDV-2010", E, L, "transform file missing from pack")
def transform_file_exists(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("transformation"):
        if r["body_ref"] and r["body_ref"] not in ctx.bundle.files:
            yield f"body_ref '{r['body_ref']}' not found under transform_files/", ctx.loc("transformation", r, "object", "layer", "step_no")


@rule("MDV-2011", E, L, "transform file checksum mismatch")
def transform_checksum(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for r in ctx.rows("transformation"):
        actual = ctx.bundle.files.get(r["body_ref"] or "")
        if actual and r["checksum"] and r["checksum"] != actual:
            yield f"declared checksum does not match file '{r['body_ref']}'", ctx.loc("transformation", r, "object", "layer", "step_no")


@rule("MDV-2012", W, L, "semantic entity not backed by a Gold-enabled object")
def semantic_gold(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    objs = {(o["project_code"], o["code"]): o for o in ctx.rows("object")}
    for s in ctx.rows("semantic_entity"):
        o = objs.get((s["project_code"], s["object"]))
        if o and "GOLD" not in o["enabled_layers"]:
            yield f"object '{o['code']}' does not enable the GOLD layer", ctx.loc("semantic_entity", s)


@rule("MDV-2013", E, L, "duplicate column name (case-insensitive)")
def column_names_unique(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    seen = Counter((r["project_code"], r["object"], r["name"].lower()) for r in ctx.rows("column"))
    for (p, o, n), c in seen.items():
        if c > 1:
            yield f"column name '{n}' appears {c} times (case-insensitive) in '{o}'", f"column:{p}/{o}"


@rule("MDV-2014", W, L, "is_pk/is_bk flags inconsistent with object keys")
def key_flag_consistency(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    objs = {(o["project_code"], o["code"]): o for o in ctx.rows("object")}
    for c in ctx.rows("column"):
        o = objs.get((c["project_code"], c["object"]))
        if not o:
            continue
        if c["is_pk"] and o["primary_keys"] and c["name"] not in o["primary_keys"]:
            yield f"column '{c['name']}' is_pk but not in object primary_keys", ctx.loc("column", c, "object", "name")
        if c["is_bk"] and o["business_keys"] and c["name"] not in o["business_keys"]:
            yield f"column '{c['name']}' is_bk but not in object business_keys", ctx.loc("column", c, "object", "name")
