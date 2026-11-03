"""TableSpec model + deterministic Delta DDL renderer.

Flavors: LOCAL (two-level names, for local Spark/Delta tests) and DATABRICKS (catalog.schema.table, UC).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from de_accelerator.core.naming import SCHEMAS, qualified_table

Status = Literal["ACTIVE", "DDL_ONLY"]
SQL_TYPES = {"str": "STRING", "int": "BIGINT", "float": "DOUBLE", "bool": "BOOLEAN", "list": "ARRAY<STRING>",
             "json": "STRING", "ts": "TIMESTAMP", "bin": "BINARY"}  # fmt: skip


@dataclass(frozen=True)
class Col:
    name: str
    type: str  # key of SQL_TYPES
    nullable: bool = True
    comment: str = ""


@dataclass(frozen=True)
class TableSpec:
    schema: str
    name: str
    columns: tuple[Col, ...]
    pk: tuple[str, ...] = ()
    comment: str = ""
    append_only: bool = False
    status: Status = "ACTIVE"
    properties: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema not in SCHEMAS:
            raise ValueError(f"{self.name}: unknown schema {self.schema}")
        names = [c.name for c in self.columns]
        if len(set(names)) != len(names):
            raise ValueError(f"{self.name}: duplicate column names")
        missing = [k for k in self.pk if k not in names]
        if missing:
            raise ValueError(f"{self.name}: pk columns not in table: {missing}")

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)


def _esc(text: str) -> str:
    return text.replace("'", "''")


def render_create(spec: TableSpec, catalog: str | None = None, flavor: str = "DATABRICKS") -> str:
    """Render a deterministic CREATE TABLE IF NOT EXISTS statement."""
    cat = catalog if flavor == "DATABRICKS" else None
    cols = []
    for c in spec.columns:
        line = f"  `{c.name}` {SQL_TYPES[c.type]}"
        if not c.nullable:
            line += " NOT NULL"
        if c.comment:
            line += f" COMMENT '{_esc(c.comment)}'"
        cols.append(line)
    props = dict(sorted(spec.properties.items()))
    if spec.append_only:
        props["delta.appendOnly"] = "true"
    if flavor == "DATABRICKS":
        props.setdefault("delta.enableChangeDataFeed", "false")
    comment = spec.comment + (" [DDL_ONLY: no runtime logic yet]" if spec.status == "DDL_ONLY" else "")
    parts = [f"CREATE TABLE IF NOT EXISTS {qualified_table(spec.schema, spec.name, cat)} (", ",\n".join(cols), ")", "USING DELTA"]
    if comment:
        parts.append(f"COMMENT '{_esc(comment)}'")
    if props:
        parts.append("TBLPROPERTIES (" + ", ".join(f"'{k}' = '{v}'" for k, v in props.items()) + ")")
    return "\n".join(parts)  # note: PK is informational metadata kept in the spec, see render_pk_comment


def render_pk_constraint(spec: TableSpec, catalog: str | None = None, flavor: str = "DATABRICKS") -> str | None:
    """Informational UC primary key (Databricks only; requires NOT NULL key columns)."""
    if flavor != "DATABRICKS" or not spec.pk:
        return None
    cat = catalog
    return (f"ALTER TABLE {qualified_table(spec.schema, spec.name, cat)} ADD CONSTRAINT pk_{spec.name} "
            f"PRIMARY KEY ({', '.join('`' + k + '`' for k in spec.pk)})")  # fmt: skip


def render_drop(spec: TableSpec, catalog: str | None = None, flavor: str = "DATABRICKS") -> str:
    cat = catalog if flavor == "DATABRICKS" else None
    return f"DROP TABLE IF EXISTS {qualified_table(spec.schema, spec.name, cat)}"
