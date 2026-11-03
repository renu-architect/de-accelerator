"""Importing this module registers all built-in connectors (and their static validators)."""

from de_accelerator.connectors.jdbc import connector as _jdbc  # noqa: F401
from de_accelerator.connectors.mock import connector as _mock  # noqa: F401
