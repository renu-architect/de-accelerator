"""Bootstrap/deployment YAML loader (YAML is for bootstrap + packs only; runtime metadata is Delta)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jsonschema
import yaml

from de_accelerator.core.errors import BootstrapConfigError
from de_accelerator.core.naming import ENVIRONMENTS, catalog_name

BOOTSTRAP_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["client_code", "environment"],
    "additionalProperties": False,
    "properties": {
        "client_code": {"type": "string", "pattern": "^[a-z][a-z0-9_]{0,40}$"},
        "environment": {"enum": list(ENVIRONMENTS)},
        "catalog": {"type": "string"},
        "flavor": {"enum": ["LOCAL", "DATABRICKS"], "default": "DATABRICKS"},
        "warehouse_dir": {"type": "string"},
        "secret_scope": {"type": "string"},
        "framework_version_pin": {"type": "string"},
        "topology": {"enum": ["ISOLATED", "NONPROD_SHARED", "SINGLE_WORKSPACE"], "default": "ISOLATED"},
        "transform_root": {"type": "string"},
    },
}


@dataclass(frozen=True)
class BootstrapConfig:
    client_code: str
    environment: str
    catalog: str
    flavor: str = "DATABRICKS"
    warehouse_dir: str | None = None
    secret_scope: str | None = None
    framework_version_pin: str | None = None
    topology: str = "ISOLATED"
    transform_root: str | None = None  # M3: directory holding reviewed Gold SQL/PySpark files (deployment artefact)


def _merge(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    out.update(over)
    return out


def load_bootstrap(base_path: str | Path, env_path: str | Path | None = None) -> BootstrapConfig:
    """Load base.yaml (+ optional per-env yaml overlay; shallow merge)."""
    try:
        data = yaml.safe_load(Path(base_path).read_text(encoding="utf-8")) or {}
        if env_path:
            data = _merge(data, yaml.safe_load(Path(env_path).read_text(encoding="utf-8")) or {})
    except (OSError, yaml.YAMLError) as exc:
        raise BootstrapConfigError(f"cannot read bootstrap config: {exc}") from exc
    try:
        jsonschema.validate(data, BOOTSTRAP_SCHEMA)
    except jsonschema.ValidationError as exc:
        raise BootstrapConfigError(f"invalid bootstrap config: {exc.message}", path=list(exc.path)) from exc
    return BootstrapConfig(
        client_code=data["client_code"],
        environment=data["environment"],
        catalog=data.get("catalog") or catalog_name(data["client_code"], data["environment"]),
        flavor=data.get("flavor", "DATABRICKS"),
        warehouse_dir=data.get("warehouse_dir"),
        secret_scope=data.get("secret_scope"),
        framework_version_pin=data.get("framework_version_pin"),
        topology=data.get("topology", "ISOLATED"),
        transform_root=data.get("transform_root"),
    )
