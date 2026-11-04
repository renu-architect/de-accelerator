"""Validation engine: rule registry, context, report. Rules live in rules_*.py (stable codes MDV-nnnn)."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from de_accelerator.core.versioning import METADATA_SCHEMA_VERSION, framework_version
from de_accelerator.metadata.findings import ERROR, INFO, WARNING, Finding
from de_accelerator.metadata.packs.bundle import PackBundle, Row, build_bundle

RuleFn = Callable[["ValidationContext"], Iterator[tuple[str, str]]]  # yields (message, location)


@dataclass(frozen=True)
class Rule:
    code: str
    severity: str
    layer: str
    title: str
    fn: RuleFn


RULES: list[Rule] = []
#: connector-specific static validation, registered by connectors (source_type -> fn(source_model, [(obj, columns, strategy)]) -> findings)
CONNECTOR_VALIDATORS: dict[str, Callable[[Any, list[tuple[Any, Any, Any]]], list[Finding]]] = {}


def rule(code: str, severity: str, layer: str, title: str) -> Callable[[RuleFn], RuleFn]:
    def deco(fn: RuleFn) -> RuleFn:
        if any(r.code == code for r in RULES):
            raise ValueError(f"duplicate rule code {code}")
        RULES.append(Rule(code, severity, layer, title, fn))
        return fn

    return deco


@dataclass
class ValidationContext:
    bundle: PackBundle
    env: str
    core_version: str = field(default_factory=framework_version)
    deployed_schema_version: int | None = None  # None = unknown (skip deployed-schema check)
    options: dict[str, Any] = field(default_factory=dict)
    deprecated_keys: dict[str, str] = field(default_factory=dict)  # "kind.field" -> hint

    def rows(self, kind: str) -> list[Row]:
        return self.bundle.rows(kind)

    def index(self, kind: str, key_fields: tuple[str, ...] = ("code",)) -> dict[tuple[Any, ...], Row]:
        return {(r.get("project_code"), *[r[k] for k in key_fields]): r for r in self.rows(kind)}

    def columns_of(self, project: str, obj: str) -> set[str] | None:
        """Declared column names for an object, or None if the object declares no columns."""
        names = {r["name"] for r in self.rows("column") if r["project_code"] == project and r["object"] == obj}
        return names or None

    @staticmethod
    def loc(kind: str, row: Row, *key: str) -> str:
        keys = "/".join(str(row.get(k)) for k in (key or ("code",)))
        return f"{kind}:{row.get('project_code')}/{keys}"


@dataclass
class ValidationReport:
    env: str
    findings: list[Finding] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)

    @property
    def errors(self) -> list[Finding]:
        return [f for f in self.findings if f.severity == ERROR]

    @property
    def warnings(self) -> list[Finding]:
        return [f for f in self.findings if f.severity == WARNING]

    @property
    def ok(self) -> bool:
        return not self.errors

    def codes(self) -> set[str]:
        return {f.code for f in self.findings}

    def to_dict(self) -> dict[str, Any]:
        return {"env": self.env, "ok": self.ok, "errors": len(self.errors), "warnings": len(self.warnings),
                "stats": self.stats, "findings": [f.to_dict() for f in self.findings]}  # fmt: skip

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)

    def to_text(self) -> str:
        lines = [f"Validation report — env={self.env}: {'OK' if self.ok else 'FAILED'} "
                 f"({len(self.errors)} errors, {len(self.warnings)} warnings)"]  # fmt: skip
        lines += [str(f) for f in sorted(self.findings, key=lambda f: (f.severity != ERROR, f.code, f.location))]
        return "\n".join(lines)


def run_rules(ctx: ValidationContext) -> list[Finding]:
    from de_accelerator.metadata.validation import (  # noqa: F401
        rules_compat,
        rules_connectors,
        rules_policy,
        rules_referential,
        rules_semantic,
        rules_transform,
    )

    try:  # soft, lazy dependency so built-in connector validators are always registered (connectors never import ingestion)
        import de_accelerator.connectors.builtin  # noqa: F401
    except ImportError:  # pragma: no cover
        pass

    overrides: dict[str, str] = ctx.options.get("severity_overrides", {})
    disabled: set[str] = set(ctx.options.get("disabled_rules", []))
    out: list[Finding] = []
    for r in RULES:
        if r.code in disabled:
            continue
        sev = overrides.get(r.code, r.severity)
        for msg, loc in r.fn(ctx):
            out.append(Finding(r.code, sev, msg, loc, r.layer))
    return out


def validate_bundle(bundle: PackBundle, env: str | None = None, core_version: str | None = None,
                    deployed_schema_version: int | None = None, options: dict[str, Any] | None = None,
                    extra_findings: list[Finding] | None = None,
                    deprecated_keys: dict[str, str] | None = None) -> ValidationReport:  # fmt: skip
    ctx = ValidationContext(bundle, env or bundle.env, core_version or framework_version(), deployed_schema_version,
                            options or {}, deprecated_keys or {})  # fmt: skip
    findings = list(extra_findings or []) + run_rules(ctx)
    return ValidationReport(ctx.env, findings, {"entities": bundle.counts(), "bundle_sha256": bundle.bundle_sha256,
                                                "rules_run": len(RULES), "metadata_schema_version": METADATA_SCHEMA_VERSION})  # fmt: skip


def validate_pack(root: str | Path, env: str, core_version: str | None = None, deployed_schema_version: int | None = None,
                  options: dict[str, Any] | None = None,
                  deprecated_keys: dict[str, str] | None = None) -> tuple[ValidationReport, PackBundle | None]:  # fmt: skip
    """Parse + resolve + validate one environment. Returns (report, bundle). Bundle is usable only if report.ok."""
    bundle, findings = build_bundle(root, env)
    if bundle is None:
        return ValidationReport(env, findings), None
    if any(f.severity == ERROR and f.layer == "SYNTAX" for f in findings):
        # syntax errors make downstream rule results unreliable: report them alone
        return ValidationReport(env, findings, {"entities": bundle.counts()}), bundle
    return validate_bundle(bundle, env, core_version, deployed_schema_version, options, findings, deprecated_keys), bundle


__all__ = ["ERROR", "INFO", "RULES", "WARNING", "ValidationContext", "ValidationReport", "rule", "validate_bundle", "validate_pack"]
