"""MetadataReader: read-only typed access to one resolved configuration.

Three implementations share one contract (tests run the same suite on all):
  DeltaMetadataReader     — the env's active (or an explicit) release in Delta/in-memory store (tools, CLI, planner)
  SnapshotMetadataReader  — an immutable run snapshot; needs no Spark and no cfg_* access (engines at runtime)
  InMemoryMetadataReader  — built from a PackBundle (unit tests, local what-if)
There is NO environment-override logic here: overrides were resolved when the pack was loaded.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from de_accelerator.core.errors import NotFoundError
from de_accelerator.metadata.packs.bundle import PackBundle
from de_accelerator.metadata.reader.models import ConfigModel, from_row
from de_accelerator.metadata.reader.source import ConfigSource, DictConfigSource, SnapshotConfigSource, StoreConfigSource
from de_accelerator.metadata.schema.entities import BY_KIND
from de_accelerator.metadata.snapshot.builder import Snapshot
from de_accelerator.metadata.store.base import TableStore


class MetadataReader:
    def __init__(self, source: ConfigSource) -> None:
        self._src = source
        self._cache: dict[str, list[ConfigModel]] = {}

    # ---- identity
    @property
    def release_id(self) -> str:
        return self._src.release_id

    @property
    def env(self) -> str:
        return self._src.env

    # ---- generic access
    def all(self, kind: str) -> list[Any]:
        if kind not in BY_KIND:
            raise NotFoundError(f"unknown entity kind: {kind}")
        if kind not in self._cache:
            self._cache[kind] = [from_row(kind, r) for r in self._src.rows(kind)]
        return list(self._cache[kind])

    def find(self, kind: str, **eq: Any) -> list[Any]:
        return self.find_where(kind, None, **eq)

    def find_where(self, kind: str, pred: Callable[[Any], bool] | None, **eq: Any) -> list[Any]:
        return [m for m in self.all(kind) if all(getattr(m, k) == v for k, v in eq.items()) and (pred is None or pred(m))]

    def get(self, kind: str, **key: Any) -> Any:
        hits = self.find(kind, **key)
        if len(hits) != 1:
            raise NotFoundError(f"{kind} {key}: expected exactly one match, found {len(hits)}")
        return hits[0]

    # ---- typed helpers
    def client(self) -> Any:
        return self.all("client")[0]

    def projects(self) -> list[Any]:
        return self.all("project")

    def project(self, code: str) -> Any:
        return self.get("project", code=code)

    def environment(self, project: str) -> Any:
        return self.get("environment", project_code=project, code=self.env)

    def sources(self, project: str) -> list[Any]:
        return self.find("source", project_code=project)

    def source(self, project: str, code: str) -> Any:
        return self.get("source", project_code=project, code=code)

    def objects(self, project: str | None = None, layer: str | None = None, tag: str | None = None) -> list[Any]:
        def ok(o: Any) -> bool:
            return (layer is None or layer in o.enabled_layers) and (tag is None or tag in o.tags)

        res = self.find_where("object", ok, **({"project_code": project} if project else {}))
        return sorted(res, key=lambda o: (o.priority if o.priority is not None else 100, o.project_code, o.code))

    def object(self, project: str, code: str) -> Any:
        return self.get("object", project_code=project, code=code)

    def columns(self, project: str, obj: str) -> list[Any]:
        cols = self.find("column", project_code=project, object=obj)
        return sorted(cols, key=lambda c: (c.ordinal is None, c.ordinal or 0, c.name))

    def load_strategy(self, project: str, obj: str) -> Any:
        return self.get("load_strategy", project_code=project, object=obj)

    def layer_mappings(self, project: str, obj: str) -> list[Any]:
        order = {"BRONZE": 0, "SILVER": 1, "GOLD": 2}
        return sorted(self.find("layer_mapping", project_code=project, object=obj), key=lambda m: order[m.layer])

    def transformations(self, project: str, obj: str, layer: str | None = None) -> list[Any]:
        res = self.find("transformation", project_code=project, object=obj, **({"layer": layer} if layer else {}))
        return sorted(res, key=lambda t: (t.layer, t.step_no))

    def dq_rules(self, project: str, obj: str, layer: str | None = None) -> list[Any]:
        return sorted(self.find("dq_rule", project_code=project, object=obj, **({"layer": layer} if layer else {})),
                      key=lambda r: r.code)  # fmt: skip

    def recon_rules(self, project: str, obj: str, scope: str | None = None) -> list[Any]:
        return sorted(self.find("recon_rule", project_code=project, object=obj, **({"scope": scope} if scope else {})),
                      key=lambda r: r.code)  # fmt: skip

    def dependencies(self, project: str | None = None) -> list[Any]:
        return self.find("dependency", **({"project_code": project} if project else {}))

    def upstream_of(self, project: str, obj: str) -> list[Any]:
        return self.find("dependency", project_code=project, object=obj)

    def execution(self, project: str, code: str) -> Any:
        return self.get("execution", project_code=project, code=code)

    def retry_policy(self, project: str, code: str) -> Any:
        return self.get("retry_policy", project_code=project, code=code)

    def retention_policies(self, project: str, obj: str | None = None) -> list[Any]:
        return self.find("retention_policy", project_code=project, **({"object": obj} if obj else {}))

    def subject_keys(self, project: str, obj: str) -> list[Any]:
        return self.find("subject_key", project_code=project, object=obj)

    def semantic_entities(self, project: str) -> list[Any]:
        return self.find("semantic_entity", project_code=project)

    def measures(self, project: str, entity: str) -> list[Any]:
        return self.find("semantic_measure", project_code=project, entity=entity)


class DeltaMetadataReader(MetadataReader):
    def __init__(self, store: TableStore, env: str, release_id: str | None = None) -> None:
        super().__init__(StoreConfigSource(store, env, release_id))


class SnapshotMetadataReader(MetadataReader):
    def __init__(self, snapshot: Snapshot) -> None:
        super().__init__(SnapshotConfigSource(snapshot))
        self.snapshot_id = snapshot.snapshot_id


class InMemoryMetadataReader(MetadataReader):
    def __init__(self, release_id: str, env: str, entities: dict[str, list[dict[str, Any]]]) -> None:
        super().__init__(DictConfigSource(release_id, env, entities))

    @classmethod
    def from_bundle(cls, bundle: PackBundle, release_id: str = "in-memory") -> InMemoryMetadataReader:
        return cls(release_id, bundle.env, bundle.entities)
