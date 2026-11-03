"""Error taxonomy with stable codes (doc 04 §15). Every error carries a code and structured context."""

from __future__ import annotations

from typing import Any


class AcceleratorError(Exception):
    code = "ACC-0000"
    error_class = "PLATFORM"  # TRANSIENT | DATA | CONFIG | PLATFORM | LOGIC

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context = context

    def __str__(self) -> str:
        return f"[{self.code}] {self.message}"


class ConfigError(AcceleratorError):
    code = "ACC-1000"
    error_class = "CONFIG"


class BootstrapConfigError(ConfigError):
    code = "ACC-1001"


class MetadataError(AcceleratorError):
    code = "ACC-2000"
    error_class = "CONFIG"


class PackError(MetadataError):
    code = "ACC-2001"


class ValidationFailedError(MetadataError):
    code = "ACC-2002"


class CompatibilityError(MetadataError):
    code = "ACC-2003"


class ReleaseError(MetadataError):
    code = "ACC-2004"


class ActivationConflictError(ReleaseError):
    """Optimistic concurrency failure on the active-release pointer."""

    code = "ACC-2005"
    error_class = "TRANSIENT"


class SnapshotIntegrityError(MetadataError):
    code = "ACC-2006"
    error_class = "DATA"


class MigrationError(MetadataError):
    code = "ACC-2007"


class StoreError(AcceleratorError):
    code = "ACC-3000"


class NotFoundError(MetadataError):
    code = "ACC-2008"


# ---- ingestion (M2) -------------------------------------------------------------------------------------------------
class IngestionError(AcceleratorError):
    code = "ACC-4000"
    error_class = "LOGIC"


class TransientSourceError(IngestionError):
    """Network/connection loss, throttling, commit conflicts: safe to retry."""

    code = "ACC-4001"
    error_class = "TRANSIENT"


class DataError(IngestionError):
    """Source data/schema problem that a retry cannot fix."""

    code = "ACC-4002"
    error_class = "DATA"


class SchemaChangeError(DataError):
    code = "ACC-4003"


class RetentionGapError(DataError):
    """CDC/change-log retention no longer covers the committed cursor."""

    code = "ACC-4004"


class SourceConfigError(IngestionError):
    code = "ACC-4005"
    error_class = "CONFIG"


class PlatformAccessError(IngestionError):
    """Authentication/authorisation failure (source, Key Vault, Unity Catalog)."""

    code = "ACC-4006"
    error_class = "PLATFORM"


class BatchBusyError(IngestionError):
    """Another live runner holds the batch lease; not a failure."""

    code = "ACC-4007"
    error_class = "TRANSIENT"


class WatermarkConflictError(IngestionError):
    code = "ACC-4008"
    error_class = "DATA"


class IntegrityError(IngestionError):
    """Receipt/manifest/target disagree: halt and alert."""

    code = "ACC-4009"
    error_class = "DATA"
