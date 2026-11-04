"""Load reviewed transformation artefacts (SQL / PySpark files) from `transform_root` (doc 00 section 5, trust model).

Only a file whose SHA-256 equals the active release's `cfg_transformation.checksum` is ever executed, and only if its resolved path
stays inside `transform_root` (no `..`, no symlink escape). Nothing stored in a metadata table is executed as code.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from de_accelerator.processing.common.errors import TransformConfigError


def read_artifact(transform_root: str | None, body_ref: str, expected_checksum: str | None) -> bytes:
    if not transform_root:
        raise TransformConfigError("transform_root is not configured (bootstrap setting): cannot load Gold transformation files")
    if not body_ref or Path(body_ref).is_absolute() or ".." in Path(body_ref).parts:
        raise TransformConfigError(f"body_ref '{body_ref}' must be a relative path without '..'")
    if not expected_checksum:
        raise TransformConfigError(
            f"transformation '{body_ref}' has no recorded checksum: refusing to execute an unverified file"
        )
    root = Path(transform_root).resolve()
    path = (root / body_ref).resolve()
    if root != path and root not in path.parents:
        raise TransformConfigError(f"body_ref '{body_ref}' resolves outside transform_root")
    if not path.is_file():
        raise TransformConfigError(f"transformation file '{body_ref}' not found under transform_root")
    data = path.read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected_checksum:
        raise TransformConfigError(f"transformation file '{body_ref}' does not match the approved checksum (file changed after review)",
                                   body_ref=body_ref)  # fmt: skip
    return data


def load_pyspark(transform_root: str | None, body_ref: str, expected_checksum: str | None) -> Callable[..., Any]:
    """Import the verified file and return its `transform(inputs, params, ctx)` function. Runs with the job's own permissions."""
    data = read_artifact(transform_root, body_ref, expected_checksum)
    name = f"_m3_transform_{uuid.uuid4().hex}"
    path = (Path(transform_root or "").resolve() / body_ref).resolve()
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:  # pragma: no cover
        raise TransformConfigError(f"cannot import '{body_ref}'")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        exec(compile(data, str(path), "exec"), module.__dict__)  # noqa: S102 - the bytes were just checksum-verified
    except Exception as exc:  # noqa: BLE001
        raise TransformConfigError(f"transformation file '{body_ref}' failed to load: {type(exc).__name__}: {exc}") from exc
    finally:
        sys.modules.pop(name, None)
    fn = module.__dict__.get("transform")
    if not callable(fn):
        raise TransformConfigError(f"transformation file '{body_ref}' must define transform(inputs, params, ctx)")
    return fn  # type: ignore[no-any-return]
