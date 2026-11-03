"""Parse a YAML metadata pack directory into raw items (no semantic validation here).

Layout:
  pack.yaml                                 manifest
  projects/<project_code>/project.yaml      project entity (code = directory name)
  projects/<project_code>/**/*.yaml         item files: {kind: <kind>, items: [...]}
  overrides/<ENV>.yaml                      load-time environment overrides
  transform_files/**                        SQL/PySpark referenced by transformation.body_ref
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import jsonschema
import yaml

from de_accelerator.core.naming import is_code
from de_accelerator.metadata.findings import ERROR, Finding
from de_accelerator.metadata.packs import schemas
from de_accelerator.metadata.packs.ids import sha256_hex
from de_accelerator.metadata.schema.entities import BY_KIND

MAX_FILE_BYTES = 5_000_000


@dataclass
class RawItem:
    kind: str
    project: str | None
    data: dict[str, Any]
    file: str
    index: int = 0

    @property
    def location(self) -> str:
        return f"{self.file}[{self.index}]"


@dataclass
class ParsedPack:
    root: Path
    manifest: dict[str, Any] = field(default_factory=dict)
    items: list[RawItem] = field(default_factory=list)
    overrides: dict[str, list[dict[str, Any]]] = field(default_factory=dict)  # env -> override list
    override_files: dict[str, str] = field(default_factory=dict)
    transform_files: dict[str, str] = field(default_factory=dict)  # relative path -> sha256
    findings: list[Finding] = field(default_factory=list)

    def add(self, code: str, message: str, location: str = "", severity: str = ERROR) -> None:
        self.findings.append(Finding(code, severity, message, location, "SYNTAX"))


def _load_yaml(path: Path, pack: ParsedPack, rel: str) -> Any | None:
    try:
        if path.stat().st_size > MAX_FILE_BYTES:
            pack.add("MDV-1001", f"file larger than {MAX_FILE_BYTES} bytes", rel)
            return None
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except (yaml.YAMLError, UnicodeDecodeError, OSError) as exc:
        pack.add("MDV-1001", f"cannot parse YAML: {str(exc).splitlines()[0] if str(exc) else exc!r}", rel)
        return None


def _schema_errors(instance: Any, schema: dict[str, Any]) -> list[str]:
    v = jsonschema.Draft202012Validator(schema)
    errs = sorted(v.iter_errors(instance), key=lambda e: [str(x) for x in e.absolute_path])
    return [f"{'/'.join(map(str, e.absolute_path)) or '<root>'}: {e.message}" for e in errs]


def validate_item(kind: str, data: Any) -> list[str]:
    return _schema_errors(data, schemas.item_schema(BY_KIND[kind]))


def parse_pack(root: str | Path) -> ParsedPack:
    root = Path(root)
    pack = ParsedPack(root=root)
    manifest_path = root / "pack.yaml"
    if not manifest_path.is_file():
        pack.add("MDV-1005", "pack.yaml not found", "pack.yaml")
        return pack
    manifest = _load_yaml(manifest_path, pack, "pack.yaml")
    if manifest is None:
        return pack
    errs = _schema_errors(manifest, schemas.manifest_schema())
    for e in errs:
        pack.add("MDV-1005", f"manifest: {e}", "pack.yaml")
    if isinstance(manifest, dict):
        pack.manifest = manifest
    if errs or not isinstance(manifest, dict):
        return pack

    # client entity from manifest
    pack.items.append(RawItem("client", None, dict(manifest["client"]), "pack.yaml", 0))

    projects_dir = root / "projects"
    for pdir in sorted(p for p in projects_dir.glob("*") if p.is_dir()) if projects_dir.is_dir() else []:
        pcode = pdir.name
        if not is_code(pcode):
            pack.add("MDV-1007", f"invalid project directory name '{pcode}'", f"projects/{pcode}")
            continue
        proj_file = pdir / "project.yaml"
        if not proj_file.is_file():
            pack.add("MDV-1007", "project.yaml missing", f"projects/{pcode}/project.yaml")
        else:
            rel = f"projects/{pcode}/project.yaml"
            pdata = _load_yaml(proj_file, pack, rel)
            if pdata is not None:
                perr = _schema_errors(pdata, schemas.project_file_schema())
                for e in perr:
                    pack.add("MDV-1002", e, rel)
                if not perr:
                    pack.items.append(RawItem("project", pcode, {"code": pcode, **pdata}, rel, 0))
        for f in sorted(list(pdir.rglob("*.yaml")) + list(pdir.rglob("*.yml"))):
            if f == proj_file:
                continue
            rel = f.relative_to(root).as_posix()
            doc = _load_yaml(f, pack, rel)
            if doc is None:
                continue
            ferr = _schema_errors(doc, schemas.file_schema())
            if ferr:
                for e in ferr:
                    pack.add("MDV-1003", f"item file: {e}", rel)
                continue
            for i, item in enumerate(doc["items"]):
                ierr = validate_item(doc["kind"], item)
                for e in ierr:
                    pack.add("MDV-1002", f"{doc['kind']}: {e}", f"{rel}[{i}]")
                if not ierr:
                    pack.items.append(RawItem(doc["kind"], pcode, item, rel, i))

    odir = root / "overrides"
    for f in sorted(odir.glob("*.y*ml")) if odir.is_dir() else []:
        env = f.stem.upper()
        rel = f.relative_to(root).as_posix()
        doc = _load_yaml(f, pack, rel)
        if doc is None:
            continue
        oerr = _schema_errors(doc, schemas.overrides_schema())
        for e in oerr:
            pack.add("MDV-1006", f"override file: {e}", rel)
        if not oerr:
            pack.overrides[env] = doc["overrides"]
            pack.override_files[env] = rel

    tdir = root / "transform_files"
    if tdir.is_dir():
        for f in sorted(p for p in tdir.rglob("*") if p.is_file()):
            pack.transform_files[f.relative_to(root).as_posix()] = sha256_hex(f.read_bytes())
    return pack
