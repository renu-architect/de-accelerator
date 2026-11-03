"""Naming conventions (approved): catalog `<client>_<env>`; fixed schema set."""

from __future__ import annotations

import re

SCHEMAS: tuple[str, ...] = ("meta", "audit", "dq", "recon", "gov", "bronze", "silver", "gold")
ENVIRONMENTS: tuple[str, ...] = ("DEV", "QA", "PROD")
_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_CODE = re.compile(r"^[A-Za-z][A-Za-z0-9_.\-]{0,127}$")


def is_identifier(value: str) -> bool:
    return bool(_IDENT.match(value))


def is_code(value: str) -> bool:
    return bool(_CODE.match(value))


def catalog_name(client_code: str, env: str) -> str:
    client = client_code.strip().lower()
    if not is_identifier(client):
        raise ValueError(f"invalid client code for catalog name: {client_code!r}")
    if env.upper() not in ENVIRONMENTS:
        raise ValueError(f"unknown environment: {env!r}")
    return f"{client}_{env.lower()}"


def qualified_table(schema: str, table: str, catalog: str | None = None) -> str:
    if schema not in SCHEMAS:
        raise ValueError(f"unknown schema: {schema!r}")
    return f"{catalog}.{schema}.{table}" if catalog else f"{schema}.{table}"
