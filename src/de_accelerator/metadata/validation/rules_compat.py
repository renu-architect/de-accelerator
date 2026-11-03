"""COMPAT rules (MDV-5xxx): pack/framework/schema compatibility (ADR-0008)."""

from __future__ import annotations

from collections.abc import Iterator

from de_accelerator.core import versioning
from de_accelerator.metadata.validation.engine import ValidationContext, rule

L = "COMPAT"
E, W = "ERROR", "WARNING"


@rule("MDV-5001", E, L, "pack requires a different framework version")
def requires_core(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    spec = ctx.bundle.manifest["requires_core"]
    if not versioning.valid_specifier(spec):
        yield f"requires_core '{spec}' is not a valid version specifier", "pack.yaml"
    elif not versioning.satisfies(ctx.core_version, spec):
        yield f"pack requires core '{spec}' but framework is {ctx.core_version}", "pack.yaml"


@rule("MDV-5002", E, L, "pack metadata schema version newer than supported/deployed")
def schema_version(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    want = ctx.bundle.manifest["metadata_schema_version"]
    if want > versioning.METADATA_SCHEMA_VERSION:
        yield f"pack needs metadata schema {want}; this framework supports up to {versioning.METADATA_SCHEMA_VERSION}", "pack.yaml"
    if ctx.deployed_schema_version is not None and want > ctx.deployed_schema_version:
        yield f"pack needs metadata schema {want}; deployed schema is {ctx.deployed_schema_version} (run migrate first)", "pack.yaml"


@rule("MDV-5003", E, L, "pack_version is not semantic")
def pack_version(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if not versioning.is_semver(ctx.bundle.manifest["pack_version"]):
        yield f"pack_version '{ctx.bundle.manifest['pack_version']}' is not MAJOR.MINOR.PATCH", "pack.yaml"


@rule("MDV-5004", E, L, "unsupported pack_format")
def pack_format(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if ctx.bundle.manifest["pack_format"] not in versioning.SUPPORTED_PACK_FORMATS:
        yield f"pack_format {ctx.bundle.manifest['pack_format']} not supported (supported: {sorted(versioning.SUPPORTED_PACK_FORMATS)})", "pack.yaml"


@rule("MDV-5005", W, L, "deprecated configuration key in use")
def deprecated(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for dotted, hint in ctx.deprecated_keys.items():
        kind, fld = dotted.split(".", 1)
        for r in ctx.rows(kind):
            if r.get(fld) not in (None, [], {}, False, ""):
                yield f"{dotted} is deprecated: {hint}", ctx.loc(kind, r)


@rule("MDV-5006", E, L, "environment not declared by the pack")
def env_declared(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    if ctx.env not in ctx.bundle.manifest.get("environments", []):
        yield f"environment {ctx.env} is not listed in pack.yaml environments", "pack.yaml"
