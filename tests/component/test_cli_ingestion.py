"""B4: CLI ingestion commands (`ingest`, `batch-status`, plus `abandon-batch`) end-to-end on the mock source and local Delta Bronze."""

import json

import pytest

from de_accelerator.cli.main import run
from de_accelerator.core.errors import DataError
from tests.ingest_util import DDL, build_world, rows

pytestmark = pytest.mark.component


@pytest.fixture
def cli(spark, tmp_path):
    w = build_world(tmp_path, spark, [{"code": c, "table": c, "load_type": "FULL"} for c in "abc"] +
                    [{"code": "inc", "table": "inc", "load_type": "INCREMENTAL"}], {t: (DDL, rows(9, 10, 11)) for t in ["a", "b", "c", "inc"]})  # fmt: skip
    (tmp_path / "base.yaml").write_text("client_code: acme\nenvironment: DEV\nflavor: LOCAL\nwarehouse_dir: /unused\n")
    lines: list[str] = []

    def call(*argv, expect=0):
        lines.clear()
        rc = run(["--bootstrap", str(tmp_path / "base.yaml"), "--actor", "tester", *argv], lambda cfg: w.store, out=lines.append,
                 engine_factory=lambda cfg, st: w.engine)  # fmt: skip
        assert rc == expect, "\n".join(lines)
        return "\n".join(lines)

    call.world = w
    yield call
    for t in ("a", "b", "c", "inc"):
        spark.sql(f"DROP TABLE IF EXISTS bronze.{t}")


def test_ingest_execution_reports_every_object_and_exits_zero(cli):
    out = cli("ingest", "--execution", "run_all")
    assert "SUCCEEDED" in out and all(f"{c} " in out for c in ("a", "b", "c", "inc")) and out.count("DONE") == 4
    assert all(cli.world.spark.table(f"bronze.{c}").count() == 3 for c in ("a", "b", "c", "inc"))


def test_ingest_json_output_is_machine_readable(cli):
    d = json.loads(cli("--json", "ingest", "--execution", "run_all"))
    assert d["status"] == "SUCCEEDED" and len(d["snapshot_id"]) == 64 and {o["object"] for o in d["objects"]} == {"a", "b", "c", "inc"}
    assert all(o["status"] == "DONE" and o["rows_written"] == 3 for o in d["objects"])


def test_ingest_object_subset(cli):
    out = cli("ingest", "--objects", "a,inc")
    assert out.count("DONE") == 2 and not cli.world.spark.catalog.tableExists("bronze.b")


def test_ingest_exit_code_and_message_when_an_object_fails(cli):
    cli.world.source.fail_extract = [DataError("source table is corrupt")]
    out = cli("ingest", "--execution", "run_all", expect=1)
    assert "PARTIAL" in out and "FAILED" in out and "[DATA]" in out and "source table is corrupt" in out and out.count("DONE") == 3


def test_ingest_unknown_execution_is_a_clean_error(cli):
    assert "unknown execution" in cli("ingest", "--execution", "nope", expect=1)


def test_second_ingest_run_is_a_no_data_run_for_incremental(cli):
    cli("ingest", "--execution", "run_all")
    out = cli("ingest", "--objects", "inc")
    assert "NO_DATA" in out and cli.world.spark.table("bronze.inc").count() == 3  # nothing new, nothing re-read, nothing duplicated


def test_batch_status_shows_cursor_batches_and_states(cli):
    cli("ingest", "--objects", "inc")
    out = cli("batch-status", "--object", "inc")
    assert "inc/live" in out and "seq=1" in out and "DONE" in out and "rows=3" in out
    d = json.loads(cli("--json", "batch-status", "--object", "inc"))
    assert d["cursor"] == "2026-11-03T11:00:00.000000Z" and d["batches"][0]["state"] == "DONE" and d["batches"][0]["rows"] == 3


def test_batch_status_for_an_object_without_batches(cli):
    out = cli("batch-status", "--object", "a")
    assert "cursor=None seq=0" in out


def test_batch_status_shows_a_failed_open_batch_then_recovers(cli):
    cli.world.source.fail_extract = [DataError("bad")]
    cli("ingest", "--objects", "a", expect=1)
    assert "FAILED_FATAL" in cli("batch-status", "--object", "a")
    assert "--restart" in cli("ingest", "--objects", "a", expect=1)  # does not hammer the source
    cli("ingest", "--objects", "a", "--restart")
    assert "DONE" in cli("batch-status", "--object", "a")


def test_abandon_batch_paths(cli):
    assert "no open batch" in cli("abandon-batch", "--object", "a", "--reason", "x", expect=1)
    cli.world.source.fail_extract = [DataError("bad")]
    cli("ingest", "--objects", "a", expect=1)
    assert "abandoned batch" in cli("abandon-batch", "--object", "a", "--reason", "operator decision")
    assert "ABANDONED" in cli("batch-status", "--object", "a")
