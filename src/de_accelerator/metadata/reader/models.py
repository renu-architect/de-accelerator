"""Typed, immutable configuration models generated from the entity definitions (single source of truth).

Generated at import time with `dataclasses.make_dataclass`, so they cannot drift from the pack schema/DDL. They are
runtime-typed (static checkers see `ConfigModel`); attribute access by field name is the supported API.
"""

from __future__ import annotations

from dataclasses import fields, make_dataclass
from typing import Any, ClassVar

from de_accelerator.metadata.schema.entities import ENTITIES, EntityDef


class ConfigModel:
    """Base for generated models."""

    entity_kind: ClassVar[str] = ""

    def to_row(self) -> dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self)}  # type: ignore[arg-type]


def _py_type(f: Any) -> Any:
    base: Any = {"str": str, "int": int, "float": float, "bool": bool, "list": tuple[str, ...], "json": dict[str, Any]}[f.type]
    return base if (f.required or f.type in ("list", "json", "bool")) else (base | None)


def _make(e: EntityDef) -> type[ConfigModel]:
    flds: list[tuple[str, Any]] = [(e.id_column, str), ("client_code", str)]
    if e.project_scoped:
        flds.append(("project_code", str))
    flds += [(f.name, _py_type(f)) for f in e.fields]
    flds.append(("row_hash", str))
    name = "".join(p.capitalize() for p in e.kind.split("_")) + "Config"
    cls = make_dataclass(name, flds, bases=(ConfigModel,), frozen=True, namespace={"entity_kind": e.kind})
    cls.__module__ = __name__
    return cls


MODELS: dict[str, type[ConfigModel]] = {e.kind: _make(e) for e in ENTITIES}
globals().update({cls.__name__: cls for cls in MODELS.values()})  # ObjectConfig, ColumnConfig, ...


def from_row(kind: str, row: dict[str, Any]) -> ConfigModel:
    cls = MODELS[kind]
    names = {f.name for f in fields(cls)}  # type: ignore[arg-type]
    data = {k: v for k, v in row.items() if k in names}
    for e in ENTITIES:
        if e.kind == kind:
            for f in e.fields:
                if f.type == "list":
                    data[f.name] = tuple(data.get(f.name) or ())
    return cls(**data)
