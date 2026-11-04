"""Table naming for Silver/Gold/reject tables (same conventions as Bronze: `<schema>.<table>`, catalog-qualified on Databricks)."""

from __future__ import annotations

import re

from de_accelerator.core.naming import qualified_table
from de_accelerator.processing.common.errors import TransformConfigError


def layer_table(target: str, layer: str, catalog: str | None, flavor: str) -> str:
    """`silver.<table>` (layer_mapping.target_table) -> `<catalog>.silver.<table>` (DATABRICKS) or `silver.<table>` (LOCAL)."""
    parts = target.split(".")
    if len(parts) != 2 or parts[0] != layer.lower() or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", parts[1]):
        raise TransformConfigError(f"{layer} target must be '{layer.lower()}.<table>', got '{target}'")
    return qualified_table(layer.lower(), parts[1], catalog if flavor == "DATABRICKS" else None)


def reject_table(object_code: str, catalog: str | None, flavor: str) -> str:
    ident = re.sub(r"[^a-z0-9_]", "_", object_code.lower())
    return qualified_table("dq", f"rej_{ident}", catalog if flavor == "DATABRICKS" else None)


def q(name: str) -> str:
    """Backtick-quote a validated identifier for SQL text."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise TransformConfigError(f"invalid identifier '{name}'")
    return f"`{name}`"
