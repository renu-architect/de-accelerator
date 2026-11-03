"""JSON-Schemas generated from entity definitions (single source of truth) + pack manifest/override schemas."""

from __future__ import annotations

from typing import Any

from de_accelerator.metadata.schema.entities import BY_KIND, ENTITIES, EntityDef, Field

CODE_PATTERN = r"^[A-Za-z][A-Za-z0-9_.\-]{0,127}$"
PACK_FORMAT = 1


def _field_schema(f: Field) -> dict[str, Any]:
    if f.type == "str":
        s: dict[str, Any] = {"type": "string"}
        if f.enum:
            s["enum"] = list(f.enum)
        if f.ref or f.name == "code":
            s["pattern"] = CODE_PATTERN
        return s
    if f.type == "int":
        return {"type": "integer"}
    if f.type == "float":
        return {"type": "number"}
    if f.type == "bool":
        return {"type": "boolean"}
    if f.type == "list":
        item: dict[str, Any] = {"type": "string"}
        if f.enum:
            item["enum"] = list(f.enum)
        if f.ref:
            item["pattern"] = CODE_PATTERN
        return {"type": "array", "items": item}
    if f.type == "json":
        return {"type": "object"}
    raise ValueError(f"unknown field type {f.type}")


def item_schema(e: EntityDef) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [f.name for f in e.fields if f.required],
        "properties": {f.name: _field_schema(f) for f in e.fields},
    }


def file_schema() -> dict[str, Any]:
    """Schema of one item file: {kind, items[]}; items are validated per kind afterwards."""
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "items"],
        "properties": {
            "kind": {"enum": [e.kind for e in ENTITIES if e.origin == "items"]},
            "items": {"type": "array", "items": {"type": "object"}},
        },
    }


def manifest_schema() -> dict[str, Any]:
    client_props = {f.name: _field_schema(f) for f in BY_KIND["client"].fields}
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["pack_format", "pack_name", "pack_version", "requires_core", "metadata_schema_version", "client",
                     "environments"],
        "properties": {
            "pack_format": {"type": "integer"},
            "pack_name": {"type": "string", "pattern": r"^[a-z][a-z0-9\-_]{1,62}$"},
            "pack_version": {"type": "string"},
            "requires_core": {"type": "string"},
            "metadata_schema_version": {"type": "integer", "minimum": 1},
            "client": {"type": "object", "required": ["code", "name"], "additionalProperties": False, "properties": client_props},
            "environments": {"type": "array", "minItems": 1, "uniqueItems": True, "items": {"enum": ["DEV", "QA", "PROD"]}},
        },
    }


def project_file_schema() -> dict[str, Any]:
    props = {f.name: _field_schema(f) for f in BY_KIND["project"].fields if f.name != "code"}
    return {"type": "object", "additionalProperties": False, "required": ["name"], "properties": props}


def overrides_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["overrides"],
        "properties": {
            "overrides": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["kind", "key"],
                    "properties": {
                        "kind": {"enum": [e.kind for e in ENTITIES if e.origin == "items"]},
                        "project": {"type": "string"},
                        "key": {"type": "object"},
                        "op": {"enum": ["set", "remove"]},
                        "set": {"type": "object"},
                    },
                },
            }
        },
    }
