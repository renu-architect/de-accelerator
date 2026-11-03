"""POLICY rules (MDV-4xxx): secrets only as Key Vault references; PII governance; organisational data-handling policy."""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any

from de_accelerator.metadata.validation.engine import ValidationContext, rule

L = "POLICY"
E, W = "ERROR", "WARNING"

KV_REF = re.compile(r"^kv:[A-Za-z0-9\-]{1,127}$")
_SECRET_KEY = re.compile(
    r"(?i)(^|_|-)(password|passwd|pwd|secret|token|api_?key|access_?key|account_?key|client_?secret|sas|credential|connection_?string)($|_|-)")
_SECRET_VALUE = [
    ("password/key assignment", re.compile(r"(?i)\b(password|pwd|accountkey|sharedaccesskey|secret)\s*=\s*[^\s;]+")),
    ("JWT-like token", re.compile(r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{5,}")),
    ("private key block", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("cloud access key id", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("bearer token", re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-]{20,}")),
    ("credentials in URL", re.compile(r"://[^/\s:@]+:[^/\s@]+@")),
]
_SSN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_PAN_IN = re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b")
_AADHAAR = re.compile(r"\b\d{4}\s\d{4}\s\d{4}\b")
_CARD = re.compile(r"\b(?:\d[ -]?){15,16}\b")
_SKIP_KEYS = {"row_hash", "checksum", "bundle_sha256"}


def _walk(obj: Any, path: str = "") -> Iterator[tuple[str, str, Any]]:
    """Yield (path, last_key, leaf) for every scalar leaf."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _walk(v, f"{path}.{k}" if path else str(k))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _walk(v, f"{path}[{i}]")
    else:
        yield path, path.rsplit(".", 1)[-1].split("[")[0], obj


def _where(ctx: ValidationContext, kind: str, r: dict[str, Any]) -> str:
    return ctx.loc(kind, r, *(("code",) if "code" in r else ("object",)))


def _luhn(digits: str) -> bool:
    nums = [int(c) for c in digits if c.isdigit()]
    if len(nums) < 13:
        return False
    total = 0
    for i, n in enumerate(reversed(nums)):
        if i % 2:
            n *= 2
            n -= 9 if n > 9 else 0
        total += n
    return total % 10 == 0


@rule("MDV-4001", E, L, "secret-like setting holds a literal instead of a kv: reference")
def secret_keys_use_kv(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for s in ctx.rows("source"):
        for fld in ("connection", "options"):
            for path, key, val in _walk(s[fld] or {}, fld):
                if _SECRET_KEY.search(key) and isinstance(val, str) and not val.startswith("kv:"):
                    yield f"'{path}' looks like a secret but is not a Key Vault reference (kv:<name>)", ctx.loc("source", s)


@rule("MDV-4002", E, L, "secret-like value found in metadata")
def secret_values(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for kind, rows in ctx.bundle.entities.items():
        for r in rows:
            for path, key, val in _walk(r):
                if not isinstance(val, str) or key in _SKIP_KEYS or key.endswith("_id") or val.startswith("kv:"):
                    continue
                for label, pat in _SECRET_VALUE:
                    if pat.search(val):
                        # never echo the value itself
                        yield f"field '{path}' contains a {label}; use a Key Vault reference", _where(ctx, kind, r)


@rule("MDV-4003", E, L, "malformed Key Vault reference")
def kv_ref_format(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for s in ctx.rows("source"):
        for fld in ("connection", "options"):
            for path, _key, val in _walk(s[fld] or {}, fld):
                if isinstance(val, str) and val.startswith("kv:") and not KV_REF.match(val):
                    yield f"'{path}': Key Vault reference must match kv:<secret-name> ([A-Za-z0-9-]{{1,127}})", ctx.loc("source", s)


@rule("MDV-4004", E, L, "PII column without classification")
def pii_classified(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for c in ctx.rows("column"):
        if c["is_pii"] and not c["classification"]:
            yield "is_pii column requires a classification", ctx.loc("column", c, "object", "name")


@rule("MDV-4005", W, L, "PII column without masking policy")
def pii_masked(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for c in ctx.rows("column"):
        if c["is_pii"] and not c["mask_policy"]:
            yield "is_pii column has no mask_policy", ctx.loc("column", c, "object", "name")


@rule("MDV-4006", E, L, "sensitive identifier pattern in metadata text")
def pii_in_text(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    """Org policy: no SSN/Aadhaar/PAN/card-like values may appear in metadata (descriptions, defaults, params ...)."""
    for kind, rows in ctx.bundle.entities.items():
        for r in rows:
            for path, key, val in _walk(r):
                if not isinstance(val, str) or key in _SKIP_KEYS or key.endswith("_id"):
                    continue
                hit = None
                if _SSN.search(val):
                    hit = "SSN-like"
                elif _AADHAAR.search(val):
                    hit = "Aadhaar-like"
                elif _PAN_IN.search(val):
                    hit = "PAN-like"
                else:
                    m = _CARD.search(val)
                    if m and _luhn(m.group(0)):
                        hit = "payment-card-like"
                if hit:
                    yield f"field '{path}' contains a {hit} value; remove it (metadata must not hold personal identifiers)", _where(ctx, kind, r)


@rule("MDV-4007", W, L, "personal-data object with low sensitivity")
def personal_sensitivity(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for o in ctx.rows("object"):
        if o["personal_data"] and o["sensitivity"] in ("public", "internal"):
            yield f"personal_data object has sensitivity '{o['sensitivity']}'", ctx.loc("object", o)


@rule("MDV-4008", E, L, "PII column on object not flagged personal_data")
def pii_object_flag(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    objs = {(o["project_code"], o["code"]): o for o in ctx.rows("object")}
    for c in ctx.rows("column"):
        o = objs.get((c["project_code"], c["object"]))
        if c["is_pii"] and o and not o["personal_data"]:
            yield f"column '{c['name']}' is_pii but object '{o['code']}' is not personal_data", ctx.loc("column", c, "object", "name")


@rule("MDV-4009", E, L, "PROD environment needs a Key Vault-backed secret scope")
def prod_secret_scope(ctx: ValidationContext) -> Iterator[tuple[str, str]]:
    for e in ctx.rows("environment"):
        if e["code"] == "PROD" and not e["secret_scope"]:
            yield "PROD environment must declare secret_scope", ctx.loc("environment", e)
