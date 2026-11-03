"""Resolved, hash-identified pack bundle for one environment (the artifact CI publishes and the loader consumes)."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from de_accelerator.core.errors import PackError
from de_accelerator.metadata.findings import ERROR, Finding
from de_accelerator.metadata.packs.ids import canonical_json, entity_id, sha256_hex
from de_accelerator.metadata.packs.overrides import apply_overrides
from de_accelerator.metadata.packs.parser import RawItem, parse_pack
from de_accelerator.metadata.schema.entities import BY_KIND, ENTITIES

BUNDLE_FORMAT = 1
Row = dict[str, Any]


@dataclass
class PackBundle:
    manifest: dict[str, Any]  # pack_name, pack_version, pack_format, requires_core, metadata_schema_version, client_code
    env: str
    entities: dict[str, list[Row]]
    files: dict[str, str] = field(default_factory=dict)  # transform file path -> sha256
    bundle_sha256: str = ""

    def content(self) -> dict[str, Any]:
        return {"bundle_format": BUNDLE_FORMAT, "manifest": self.manifest, "env": self.env,
                "entities": self.entities, "files": self.files}  # fmt: skip

    def compute_hash(self) -> str:
        return sha256_hex(canonical_json(self.content()))

    def to_json(self) -> str:
        return canonical_json({**self.content(), "bundle_sha256": self.compute_hash()})

    @classmethod
    def from_json(cls, text: str) -> PackBundle:
        try:
            d = json.loads(text)
            b = cls(d["manifest"], d["env"], d["entities"], d.get("files", {}), d["bundle_sha256"])
        except (KeyError, ValueError) as exc:
            raise PackError(f"malformed bundle: {exc}") from exc
        if d.get("bundle_format") != BUNDLE_FORMAT:
            raise PackError(f"unsupported bundle_format {d.get('bundle_format')}")
        if b.compute_hash() != b.bundle_sha256:
            raise PackError("bundle hash mismatch: content was modified after build", expected=b.bundle_sha256)
        return b

    @classmethod
    def read(cls, path: str | Path) -> PackBundle:
        return cls.from_json(Path(path).read_text(encoding="utf-8"))

    def write(self, path: str | Path) -> None:
        Path(path).write_text(self.to_json(), encoding="utf-8")
        Path(str(path) + ".sha256").write_text(self.compute_hash() + "\n", encoding="utf-8")

    def counts(self) -> dict[str, int]:
        return {k: len(v) for k, v in sorted(self.entities.items()) if v}

    def rows(self, kind: str) -> list[Row]:
        return self.entities.get(kind, [])


def _row(client_code: str, item: RawItem) -> Row:
    ent = BY_KIND[item.kind]
    row: Row = {}
    key_vals = [item.data[k] for k in ent.key]
    row[ent.id_column] = entity_id(client_code, item.kind, item.project, key_vals)
    row["client_code"] = client_code
    if ent.project_scoped:
        row["project_code"] = item.project
    for f in ent.fields:
        row[f.name] = copy.deepcopy(item.data[f.name]) if f.name in item.data else copy.deepcopy(f.default)
    row["row_hash"] = sha256_hex(canonical_json(row))
    return row


def build_bundle(root: str | Path, env: str) -> tuple[PackBundle | None, list[Finding]]:
    """Parse -> resolve env overrides -> assign deterministic ids/hashes. Returns (bundle|None, findings).

    The bundle is None only when the manifest itself is unusable; otherwise it is returned together with findings so the
    validator can report everything in one pass. Callers must treat any ERROR finding as a failed build.
    """
    pack = parse_pack(root)
    findings = list(pack.findings)
    if not pack.manifest or any(f.code == "MDV-1005" for f in findings):
        return None, findings
    items, ofind = apply_overrides(pack, env)
    findings += ofind

    client_code = pack.manifest["client"]["code"]
    seen: dict[tuple[Any, ...], RawItem] = {}
    entities: dict[str, list[Row]] = {e.kind: [] for e in ENTITIES}
    for it in items:
        ent = BY_KIND[it.kind]
        if it.kind == "environment" and it.data.get("code") != env:
            continue  # only the target environment's row is part of this env's bundle
        missing = [k for k in ent.key if k not in it.data]
        if missing:
            findings.append(Finding("MDV-1002", ERROR, f"{it.kind}: missing key field(s) {missing}", it.location, "SYNTAX"))
            continue
        dup_key = (it.kind, it.project, *[it.data[k] for k in ent.key])
        if dup_key in seen:
            msg = f"duplicate {it.kind} key {dup_key[2:]} (first at {seen[dup_key].location})"
            findings.append(Finding("MDV-1004", ERROR, msg, it.location, "SYNTAX"))
            continue
        seen[dup_key] = it
        entities[it.kind].append(_row(client_code, it))

    # transformation checksum: fill from file when absent (mismatch is reported by validator rule MDV-2011)
    for row in entities["transformation"]:
        ref = row.get("body_ref")
        if ref and not row.get("checksum") and ref in pack.transform_files:
            row["checksum"] = pack.transform_files[ref]
            row["row_hash"] = sha256_hex(canonical_json({k: v for k, v in row.items() if k != "row_hash"}))
    for kind, rows in entities.items():
        rows.sort(key=lambda r, c=BY_KIND[kind].id_column: r[c])  # type: ignore[misc]

    m = pack.manifest
    manifest = {"pack_name": m["pack_name"], "pack_version": m["pack_version"], "pack_format": m["pack_format"],
                "requires_core": m["requires_core"], "metadata_schema_version": m["metadata_schema_version"],
                "client_code": client_code, "environments": sorted(m["environments"])}  # fmt: skip
    bundle = PackBundle(manifest, env, entities, dict(sorted(pack.transform_files.items())))
    bundle.bundle_sha256 = bundle.compute_hash()
    return bundle, findings
