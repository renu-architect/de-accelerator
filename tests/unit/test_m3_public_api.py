"""M3 depends only on M2's public API (D8). Static check of every M3 module's imports + pins of the M2 behaviour M3 relies on."""

import ast
from pathlib import Path

import pytest

from de_accelerator.audit.models import DONE, FAILED_RETRYABLE, PLANNED, Manifest, batch_id_for
from de_accelerator.audit.repos import EventLog, ManifestRepo, open_batch
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.store.memory import InMemoryTableStore

SRC = Path(__file__).resolve().parents[2] / "src" / "de_accelerator"
M3_FILES = [p for d in ("common", "silver", "gold") for p in sorted((SRC / "processing" / d).glob("*.py"))]
M3_FILES += [SRC / "processing" / "runner.py", SRC / "audit" / "layer_cursor.py", SRC / "cli" / "transform.py",
             SRC / "core" / "transform_spec.py", SRC / "metadata" / "validation" / "rules_transform.py"]  # fmt: skip
M3_FILES = [p for p in M3_FILES if p.exists()]

#: module -> allowed imported names (None = any public name)
ALLOWED: dict[str, set[str] | None] = {
    "de_accelerator.audit.repos": {"ManifestRepo", "EventLog", "ReceiptRepo", "LeaseRepo", "RunLog", "open_batch"},
    "de_accelerator.audit.models": {
        "Manifest",
        "Receipt",
        "batch_id_for",
        "PLANNED",
        "DONE",
        "FAILED_RETRYABLE",
        "FAILED_FATAL",
        "ABANDONED",
        "TERMINAL",
    },  # fmt: skip
    "de_accelerator.audit.layer_cursor": None,
}
ALLOWED_PREFIXES = ("de_accelerator.core", "de_accelerator.metadata", "de_accelerator.processing.common",
                    "de_accelerator.processing.silver", "de_accelerator.processing.gold", "de_accelerator.processing.runner",
                    "de_accelerator.cli.transform", "de_accelerator.audit.layer_cursor")  # fmt: skip
FORBIDDEN_PREFIXES = ("de_accelerator.ingestion", "de_accelerator.connectors", "de_accelerator.processing.bronze")


def _imports(path: Path):
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("de_accelerator") and node.level == 0:
            yield node.module, [a.name for a in node.names]
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name.startswith("de_accelerator"):
                    yield a.name, []


@pytest.mark.parametrize("path", M3_FILES, ids=lambda p: str(p.relative_to(SRC)))
def test_m3_module_uses_only_public_m2_api(path):
    for module, names in _imports(path):
        assert not module.startswith(FORBIDDEN_PREFIXES), f"{path.name} imports forbidden module {module}"
        assert all(not n.startswith("_") for n in names), f"{path.name} imports a private name from {module}: {names}"
        if module in ALLOWED:
            allowed = ALLOWED[module]
            assert allowed is None or set(names) <= allowed, (
                f"{path.name}: {set(names) - (allowed or set())} not in the approved M2 API"
            )
        else:
            assert module.startswith(ALLOWED_PREFIXES), f"{path.name} imports unapproved module {module}"


def test_import_check_actually_sees_modules():
    assert M3_FILES, "no M3 modules found: the allowlist test would be vacuous"


# ---- M2 behaviour M3 relies on (a future M2 change that breaks it must fail here) --------------------------------------
@pytest.fixture
def store():
    s = InMemoryTableStore()
    Migrator(s, MIGRATIONS).apply()
    return s


def _planned(store):
    m = Manifest(batch_id_for("o", "SILVER:live", 1), "o", "SILVER:live", 1, "SILVER", "s", "r", "1", "1", None, [], {}, "run")
    ManifestRepo(store).create(m)
    ev = EventLog(store)
    ev.append(m.batch_id, PLANNED, run_id="run")
    return m, ev


def test_unknown_informational_events_do_not_break_state_or_open_batch(store):
    m, ev = _planned(store)
    ev.append(m.batch_id, "TARGET_COMMITTED", run_id="run", rows=3)
    ev.append(m.batch_id, "CURSOR_ADVANCED", run_id="run")
    assert ev.state(m.batch_id) == PLANNED  # unknown events are ignored by the fold
    assert open_batch(ManifestRepo(store), ev, "o", "SILVER:live").batch_id == m.batch_id
    ev.append(m.batch_id, DONE, run_id="run")
    assert ev.state(m.batch_id) == DONE and open_batch(ManifestRepo(store), ev, "o", "SILVER:live") is None


def test_failure_then_recovery_events_keep_the_batch_open(store):
    m, ev = _planned(store)
    ev.append(m.batch_id, FAILED_RETRYABLE, run_id="run")
    assert ev.state(m.batch_id) == FAILED_RETRYABLE
    ev.append(m.batch_id, "TARGET_COMMITTED", run_id="run")  # recovery progressed after the failure
    assert open_batch(ManifestRepo(store), ev, "o", "SILVER:live") is not None


def test_bronze_contract_columns_m3_reads_exist():
    """Documented Bronze contract (M3 hard-codes these names; M2's own list must contain them)."""
    from de_accelerator.processing.bronze.audit_columns import AUDIT_COLUMNS

    for c in ("_batch_id", "_batch_seq", "_stream_key", "_op_type", "_op_seq", "_row_hash", "_ingest_ts"):
        assert c in AUDIT_COLUMNS
