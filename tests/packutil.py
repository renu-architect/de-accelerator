"""Helpers: copy the shipped synthetic example pack into tmp and mutate it."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

EXAMPLE = Path(__file__).resolve().parents[1] / "metadata" / "examples" / "acme_pack"


def copy_pack(dst: Path) -> Path:
    out = dst / "pack"
    shutil.copytree(EXAMPLE, out)
    return out


def edit_yaml(path: Path, fn: Callable[[Any], Any]) -> None:
    """Apply fn(data) in place (return value ignored) and write back."""
    data = yaml.safe_load(path.read_text())
    fn(data)
    path.write_text(yaml.safe_dump(data, sort_keys=False))


def edit_items(root: Path, rel: str, fn: Callable[[list[dict[str, Any]]], Any]) -> None:
    edit_yaml(root / rel, lambda d: fn(d["items"]))


def codes(findings) -> set[str]:
    return {f.code for f in findings}
