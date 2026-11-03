"""Validation findings shared by the pack parser and the validator (stable codes MDV-nnnn)."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

ERROR, WARNING, INFO = "ERROR", "WARNING", "INFO"


@dataclass(frozen=True)
class Finding:
    code: str
    severity: str
    message: str
    location: str = ""  # file[:index] or entity path
    layer: str = ""  # SYNTAX | REFERENTIAL | SEMANTIC | POLICY | COMPAT

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def __str__(self) -> str:
        loc = f" ({self.location})" if self.location else ""
        return f"{self.severity:<7} {self.code} {self.message}{loc}"
