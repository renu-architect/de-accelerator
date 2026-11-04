"""M3 error classes (codes ACC-7xxx). They extend the shared taxonomy without touching `core.errors`."""

from __future__ import annotations

from de_accelerator.core.errors import AcceleratorError


class TransformError(AcceleratorError):
    code = "ACC-7000"
    error_class = "LOGIC"


class TransformDataError(TransformError):
    """Data problem: not retried (cast error under FAIL, DQ FAIL_BATCH, SCD2 chain violation)."""

    code = "ACC-7001"
    error_class = "DATA"


class TransformConfigError(TransformError):
    """Configuration/code-artefact problem: not retried (checksum mismatch, missing file, unsupported setting)."""

    code = "ACC-7002"
    error_class = "CONFIG"


class VerificationError(TransformDataError):
    """The merged target does not contain the staged content: the unit fails and the cursor does not move."""

    code = "ACC-7003"
