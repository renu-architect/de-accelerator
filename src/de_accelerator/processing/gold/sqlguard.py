"""Gold SQL trust controls (doc 00 section 5): defence in depth, NOT a security boundary.

The real controls are organisational: SQL lives in reviewed files, the file's SHA-256 must equal the active release's checksum, the
path must stay inside `transform_root`, and `inline_sql` is rejected. This tokenizer (it is not a SQL parser) additionally refuses text
that is not a single query: first keyword must be SELECT/WITH, no `;`, no DDL/DML keywords outside comments/strings/quoted identifiers.
It can over-block (a column literally named `update`: quote it with backticks); it never executes anything.
"""

from __future__ import annotations

import re

from de_accelerator.processing.common.errors import TransformConfigError

FORBIDDEN = frozenset({"insert", "update", "delete", "merge", "drop", "create", "alter", "truncate", "grant", "revoke", "optimize",
                       "vacuum", "copy", "call", "msck", "refresh", "analyze"})  # fmt: skip
_PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z_]+)\.([A-Za-z0-9_.\-]+)\s*\}\}")
_ANY_PLACEHOLDER = re.compile(r"\{\{.*?\}\}", re.DOTALL)
_TOKEN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_PARAM = re.compile(r"(?<![:\w]):([A-Za-z_][A-Za-z0-9_]*)")


def strip_noise(sql: str) -> str:
    """Remove comments, string literals and quoted identifiers (replaced by a space) so keywords can be inspected safely."""
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        two = sql[i : i + 2]
        if two == "--":
            j = sql.find("\n", i)
            i = n if j < 0 else j
        elif two == "/*":
            j = sql.find("*/", i + 2)
            if j < 0:
                raise TransformConfigError("unterminated /* comment in Gold SQL")
            i = j + 2
            out.append(" ")
        elif c in "'\"`":
            quote, j = c, i + 1
            while j < n:
                if sql[j] == "\\" and quote != "`":
                    j += 2
                    continue
                if sql[j] == quote:
                    if j + 1 < n and sql[j + 1] == quote:  # doubled quote = escaped quote
                        j += 2
                        continue
                    break
                j += 1
            else:
                raise TransformConfigError("unterminated string/identifier quote in Gold SQL")
            i = j + 1
            out.append(" ")
        else:
            out.append(c)
            i += 1
    return "".join(out)


def check_query_text(sql: str) -> None:
    """Raise TransformConfigError unless `sql` (after placeholder substitution) looks like exactly one SELECT/WITH query."""
    clean = strip_noise(sql)
    if ";" in clean:
        raise TransformConfigError("Gold SQL must be a single statement: ';' is not allowed")
    tokens = [t.lower() for t in _TOKEN.findall(clean)]
    if not tokens or tokens[0] not in ("select", "with"):
        raise TransformConfigError("Gold SQL must start with SELECT or WITH")
    bad = sorted({t for t in tokens if t in FORBIDDEN})
    if bad:
        raise TransformConfigError(f"Gold SQL contains forbidden keyword(s) {bad}: only read-only queries are allowed "
                                   "(quote a column with backticks if it is really named like a keyword)")  # fmt: skip


def parameters_used(sql: str) -> set[str]:
    return set(_PARAM.findall(strip_noise(sql)))


def resolve_placeholders(sql: str, inputs: dict[str, str], steps: dict[int, str]) -> str:
    """`{{ input.<object> }}` -> engine-generated view of a DECLARED input; `{{ step.<n> }}` -> view of an earlier step.
    Anything else inside {{ }} (e.g. `delta.x`, undeclared objects) is an error: no free text ever reaches the SQL."""

    def sub(m: re.Match[str]) -> str:
        kind, name = m.group(1), m.group(2)
        if kind == "input":
            if name not in inputs:
                raise TransformConfigError(f"placeholder input.{name} is not a declared input_object of this transformation")
            return inputs[name]
        if kind == "step":
            if not name.isdigit() or int(name) not in steps:
                raise TransformConfigError(f"placeholder step.{name} does not refer to an earlier step")
            return steps[int(name)]
        raise TransformConfigError(f"placeholder {{{{ {kind}.{name} }}}} is not supported (only input.<object> and step.<n>)")

    out = _PLACEHOLDER.sub(sub, sql)
    leftover = _ANY_PLACEHOLDER.search(out)
    if leftover or "{{" in out or "}}" in out:
        raise TransformConfigError(f"malformed placeholder {leftover.group(0) if leftover else '{{ or }}'!r}")
    return out
