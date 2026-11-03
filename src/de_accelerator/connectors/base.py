"""Connector contract (docs/architecture/m2/01_connector_interfaces.md) and registry."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal, Protocol

from de_accelerator.core.clock import Clock
from de_accelerator.core.errors import SourceConfigError
from de_accelerator.core.registry import Registry
from de_accelerator.core.secrets import SecretProvider
from de_accelerator.metadata.findings import Finding
from de_accelerator.metadata.packs.ids import canonical_json, sha256_hex
from de_accelerator.metadata.validation.engine import CONNECTOR_VALIDATORS

BatchType = Literal["LIVE", "BACKFILL", "REPROCESS", "RESYNC"]


@dataclass(frozen=True)
class Capabilities:
    full: bool = False
    incremental: bool = False
    cdc: bool = False
    snapshot: bool = False
    file_incremental: bool = False
    pagination: bool = False
    discovery: bool = False
    profiling: bool = False
    count_pushdown: bool = False
    partitioned_read: bool = False

    def supports(self, load_type: str) -> bool:
        return {"FULL": self.full, "INCREMENTAL": self.incremental, "CDC": self.cdc, "SNAPSHOT": self.snapshot,
                "FILE_INCR": self.file_incremental, "API": self.pagination}.get(load_type, False)  # fmt: skip


@dataclass(frozen=True)
class WatermarkState:
    """The last COMMITTED source->Bronze cursor."""

    value: str | None
    seq: int
    epoch: int = 0


@dataclass(frozen=True)
class FileRef:
    path: str
    size: int
    mtime: str  # ISO-8601 UTC
    etag: str | None = None


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    native_type: str
    nullable: bool = True
    is_pk: bool = False


@dataclass(frozen=True)
class SourceSchema:
    columns: tuple[ColumnInfo, ...]

    @property
    def schema_hash(self) -> str:
        return sha256_hex(canonical_json([[c.name, c.native_type, c.nullable] for c in self.columns]))


@dataclass(frozen=True)
class ObjectBinding:
    """Everything a connector needs about one object (immutable M1 typed models)."""

    source: Any  # SourceConfig
    obj: Any  # ObjectConfig
    columns: tuple[Any, ...]  # ColumnConfig*
    strategy: Any  # LoadStrategyConfig

    @property
    def code(self) -> str:
        return str(self.obj.code)


@dataclass(frozen=True)
class PlanRequest:
    binding: ObjectBinding
    stream_key: str
    batch_type: BatchType
    committed: WatermarkState | None
    window_override: tuple[str | None, str | None] | None = None


@dataclass(frozen=True)
class ExtractPlan:
    """Frozen, secret-free description of exactly what a batch reads. Persisted in the manifest."""

    wm_from: str | None
    wm_to: str | None
    lookback: str | None = None
    file_list: tuple[FileRef, ...] | None = None
    plan_json: dict[str, Any] = field(default_factory=dict)
    empty: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {"wm_from": self.wm_from, "wm_to": self.wm_to, "lookback": self.lookback,
                "file_list": [f.__dict__ for f in self.file_list] if self.file_list else None,
                "plan_json": self.plan_json}  # fmt: skip

    @property
    def plan_hash(self) -> str:
        return sha256_hex(canonical_json(self.to_dict()))

    @classmethod
    def from_manifest(cls, wm_from: str | None, wm_to: str | None, file_list: Any, plan_json: dict[str, Any]) -> ExtractPlan:
        files = tuple(FileRef(**f) for f in file_list) if file_list else None
        return cls(wm_from, wm_to, plan_json.get("lookback"), files, plan_json)


@dataclass(frozen=True)
class ExtractContext:
    batch_id: str
    batch_seq: int
    stream_key: str
    landing_dir: str | None = None


@dataclass
class ExtractResult:
    df: Any  # pyspark DataFrame of RAW source rows (+ optional _op_type/_op_seq/_source_file/_source_file_mod_ts)
    source_schema: SourceSchema
    stats: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TestResult:
    __test__ = False  # not a pytest class
    ok: bool
    message: str = ""


@dataclass
class ConnectorContext:
    source: Any  # SourceConfig
    env: Any  # EnvironmentConfig
    secrets: SecretProvider
    spark: Any
    run_id: str
    clock: Clock
    landing_root: str | None = None


class Connector(Protocol):
    source_types: ClassVar[tuple[str, ...]]
    capabilities: ClassVar[Capabilities]

    def __init__(self, ctx: ConnectorContext) -> None: ...

    @classmethod
    def validate_config(cls, source: Any, bindings: Sequence[ObjectBinding]) -> list[Finding]: ...

    def test_connection(self) -> TestResult: ...

    def get_schema(self, binding: ObjectBinding) -> SourceSchema: ...

    def plan(self, req: PlanRequest) -> ExtractPlan: ...

    def extract(self, plan: ExtractPlan, req: PlanRequest, ctx: ExtractContext) -> ExtractResult: ...

    def count(self, plan: ExtractPlan, req: PlanRequest) -> int | None: ...

    def close(self) -> None: ...


CONNECTORS: Registry[type] = Registry("connector")


def register_connector(*source_types: str) -> Callable[[type], type]:
    def deco(cls: type) -> type:
        cls.source_types = tuple(source_types)  # type: ignore[attr-defined]
        for st in source_types:
            CONNECTORS.register(st, cls)
            CONNECTOR_VALIDATORS[st] = _adapter(cls)
        return cls

    return deco


def _adapter(cls: type) -> Callable[[Any, list[tuple[Any, Any, Any]]], list[Finding]]:
    def run(source: Any, triples: list[tuple[Any, Any, Any]]) -> list[Finding]:
        bindings = [ObjectBinding(source, o, tuple(c), s) for o, c, s in triples]
        return list(cls.validate_config(source, bindings))  # type: ignore[attr-defined]

    return run


def connector_class(source_type: str) -> type:
    try:
        return CONNECTORS.get(source_type)
    except KeyError as exc:
        raise SourceConfigError(f"no connector registered for source_type '{source_type}'",
                                registered=CONNECTORS.keys()) from exc  # fmt: skip


def load_entry_point_connectors() -> None:
    """Load client/extension connectors registered under the `de_accelerator.connectors` entry-point group."""
    from importlib.metadata import entry_points

    for ep in entry_points(group="de_accelerator.connectors"):
        ep.load()
