"""Environment override resolution — performed ONLY at pack load/bundle time (approved decision; no runtime logic)."""

from __future__ import annotations

import copy
from typing import Any

from de_accelerator.metadata.findings import ERROR, Finding
from de_accelerator.metadata.packs.parser import ParsedPack, RawItem, validate_item
from de_accelerator.metadata.schema.entities import BY_KIND


def deep_merge(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _matches(item: RawItem, kind: str, project: str | None, key: dict[str, Any]) -> bool:
    if item.kind != kind or (project is not None and item.project != project):
        return False
    return all(item.data.get(k) == v for k, v in key.items())


def apply_overrides(pack: ParsedPack, env: str) -> tuple[list[RawItem], list[Finding]]:
    """Return env-resolved items (a deep copy; the parsed pack is never mutated) + findings (MDV-1006)."""
    items = [RawItem(i.kind, i.project, copy.deepcopy(i.data), i.file, i.index) for i in pack.items]
    findings: list[Finding] = []
    src = pack.override_files.get(env, f"overrides/{env}.yaml")

    def bad(msg: str, n: int) -> None:
        findings.append(Finding("MDV-1006", ERROR, msg, f"{src}[{n}]", "SYNTAX"))

    for n, ov in enumerate(pack.overrides.get(env, [])):
        kind, project, key, op = ov["kind"], ov.get("project"), ov["key"], ov.get("op", "set")
        ent = BY_KIND[kind]
        if ent.project_scoped and project is None:
            bad(f"override of project-scoped kind '{kind}' needs 'project'", n)
            continue
        if set(key) != set(ent.key) and not set(key) <= set(ent.key):
            bad(f"override key fields {sorted(key)} are not business-key fields {list(ent.key)}", n)
            continue
        hits = [i for i in items if _matches(i, kind, project, key)]
        if len(hits) != 1:
            bad(f"override target {kind}{key} matched {len(hits)} items (expected exactly 1)", n)
            continue
        target = hits[0]
        if op == "remove":
            items.remove(target)
            continue
        changes = ov.get("set", {})
        touched = set(changes) & set(ent.key)
        if touched:
            bad(f"override must not change key fields: {sorted(touched)}", n)
            continue
        merged = deep_merge(target.data, changes)
        errs = validate_item(kind, merged)
        if errs:
            for e in errs:
                bad(f"override produces invalid {kind}: {e}", n)
            continue
        target.data = merged
    return items, findings
