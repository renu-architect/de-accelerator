"""Immutable run-configuration snapshots (ADR-0007).

A snapshot is the fully resolved configuration (for a scope) of ONE release, serialised canonically. Its id is the sha256 of
that content, which includes the release id: an execution therefore always retains both its release and its exact config.
Engines read only the snapshot (never `cfg_*`), so metadata changes mid-run cannot affect a run, and restarts reuse the id.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from de_accelerator.core import versioning
from de_accelerator.core.errors import CompatibilityError, NotFoundError
from de_accelerator.metadata.packs.ids import canonical_json, sha256_hex
from de_accelerator.metadata.reader.source import ConfigSource, business_sort_key
from de_accelerator.metadata.schema.entities import ENTITIES

SNAPSHOT_FORMAT = 1


@dataclass(frozen=True)
class SnapshotScope:
    projects: tuple[str, ...] = ()  # empty = all projects
    objects: tuple[str, ...] = ()  # object codes; empty = all objects of the selected projects
    execution: str | None = None  # convenience: take the object selection from this execution's scope.objects

    def to_dict(self) -> dict[str, Any]:
        return {"projects": sorted(self.projects), "objects": sorted(self.objects), "execution": self.execution}


@dataclass(frozen=True)
class SnapshotRef:
    snapshot_id: str
    release_id: str


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: str
    release_id: str
    env: str
    scope: dict[str, Any]
    content: dict[str, Any]

    @property
    def ref(self) -> SnapshotRef:
        return SnapshotRef(self.snapshot_id, self.release_id)

    def counts(self) -> dict[str, int]:
        return {k: len(v) for k, v in sorted(self.content["entities"].items()) if v}


class SnapshotBuilder:
    def __init__(self, source: ConfigSource, release_info: dict[str, Any] | None = None) -> None:
        self.source = source
        self.release = release_info or getattr(source, "release", None) or {}

    def build(self, scope: SnapshotScope | None = None) -> Snapshot:
        scope = scope or SnapshotScope()
        spec = self.release.get("requires_core")
        if spec and not versioning.satisfies(versioning.framework_version(), spec):
            raise CompatibilityError(f"release requires core '{spec}' but framework is {versioning.framework_version()}")
        src = self.source
        all_rows = {e.kind: src.rows(e.kind) for e in ENTITIES}

        projects = {p["code"] for p in all_rows["project"]}
        if scope.projects:
            unknown = set(scope.projects) - projects
            if unknown:
                raise NotFoundError(f"unknown project(s) in scope: {sorted(unknown)}")
            projects = set(scope.projects)

        wanted_objects = set(scope.objects)
        if scope.execution:
            ex = [e for e in all_rows["execution"] if e["code"] == scope.execution and e["project_code"] in projects]
            if not ex:
                raise NotFoundError(f"unknown execution '{scope.execution}' in scope")
            wanted_objects |= set((ex[0]["scope"] or {}).get("objects", []))
        objects = [
            o for o in all_rows["object"]
            if o["project_code"] in projects and (not wanted_objects or o["code"] in wanted_objects)
        ]  # fmt: skip
        if wanted_objects:
            missing = wanted_objects - {o["code"] for o in objects}
            if missing:
                raise NotFoundError(f"unknown object(s) in scope: {sorted(missing)}")
        in_scope = {(o["project_code"], o["code"]) for o in objects}

        def keep(kind: str, row: dict[str, Any]) -> bool:
            p = row.get("project_code")
            if kind == "client":
                return True
            if p not in projects:
                return False
            if kind == "object":
                return (p, row["code"]) in in_scope
            if kind == "source":
                return any(o["source"] == row["code"] and o["project_code"] == p for o in objects)
            if kind in ("column", "load_strategy", "layer_mapping", "transformation", "dq_rule", "recon_rule", "dependency",
                        "subject_key"):  # fmt: skip
                return (p, row["object"]) in in_scope
            if kind == "retention_policy":
                return row["object"] is None or (p, row["object"]) in in_scope
            if kind == "semantic_entity":
                return (p, row["object"]) in in_scope
            if kind == "doc_pipeline":
                return any(o["source"] == row["source"] and o["project_code"] == p for o in objects)
            return True  # project, environment, retry_policy, execution, orchestration_provider, glossary, consumers, targets

        entities: dict[str, list[dict[str, Any]]] = {}
        for e in ENTITIES:
            kept = [r for r in all_rows[e.kind] if keep(e.kind, r)]
            entities[e.kind] = sorted(kept, key=lambda r, k=e.kind: business_sort_key(k, r))  # type: ignore[misc]
        # semantic children follow the kept semantic entities
        kept_entities: dict[str, set[str]] = defaultdict(set)
        for s in entities["semantic_entity"]:
            kept_entities[s["project_code"]].add(s["code"])
        entities["semantic_measure"] = [
            m for m in entities["semantic_measure"] if m["entity"] in kept_entities[m["project_code"]]
        ]  # fmt: skip
        entities["semantic_relationship"] = [
            r for r in entities["semantic_relationship"]
            if r["from_entity"] in kept_entities[r["project_code"]] and r["to_entity"] in kept_entities[r["project_code"]]
        ]  # fmt: skip
        entities["semantic_target"] = [t for t in entities["semantic_target"]
                                       if any(x in kept_entities[t["project_code"]] for x in t["entities"])]  # fmt: skip

        client = entities["client"][0]["code"] if entities["client"] else None
        content = {
            "snapshot_format": SNAPSHOT_FORMAT,
            "release_id": src.release_id,
            "env": src.env,
            "client_code": client,
            "pack": {"name": self.release.get("pack_name"), "version": self.release.get("pack_version"),
                     "bundle_sha256": self.release.get("bundle_sha256")},  # fmt: skip
            "scope": scope.to_dict(),
            "entities": entities,
        }
        return Snapshot(sha256_hex(canonical_json(content)), src.release_id, src.env, scope.to_dict(), content)
