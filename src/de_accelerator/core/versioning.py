"""Framework / metadata-schema versions and compatibility checks (ADR-0008)."""

from __future__ import annotations

import re

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from de_accelerator import __version__

#: Version of the metadata schema produced by migrations in this release. Independent of framework SemVer.
METADATA_SCHEMA_VERSION = 3
#: Pack file-format versions this framework can read.
SUPPORTED_PACK_FORMATS: frozenset[int] = frozenset({1})


_SEMVER = re.compile(r"^\d+\.\d+\.\d+([.\-+]?[0-9A-Za-z.\-+]*)?$")


def framework_version() -> str:
    return __version__


def is_semver(value: str) -> bool:
    return bool(_SEMVER.match(value))


def satisfies(version: str, specifier: str) -> bool:
    """True if `version` satisfies the PEP 440 specifier (e.g. '>=1.0,<2.0'). Dev/pre-releases are allowed."""
    try:
        return SpecifierSet(specifier).contains(Version(version), prereleases=True)
    except (InvalidSpecifier, InvalidVersion):
        return False


def valid_specifier(specifier: str) -> bool:
    try:
        SpecifierSet(specifier)
    except InvalidSpecifier:
        return False
    return True
