"""Gold SQL trust controls: tokenizer guard and placeholder resolution (no Spark needed)."""

import pytest

from de_accelerator.processing.common.errors import TransformConfigError
from de_accelerator.processing.gold.sqlguard import check_query_text, parameters_used, resolve_placeholders, strip_noise


@pytest.mark.parametrize("sql", [
    "SELECT 1", "select a, b FROM t WHERE x > :min_amount", "WITH c AS (SELECT 1 AS a) SELECT * FROM c",
    "SELECT `update`, 'drop table x; --' AS s FROM t  -- delete everything\n", "/* insert */ SELECT 1",
    "SELECT replace(a, 'x', 'y') FROM t", "SELECT \"it's\" FROM t",
])  # fmt: skip
def test_read_only_queries_pass(sql):
    check_query_text(sql)


@pytest.mark.parametrize("sql", [
    "SELECT 1; SELECT 2", "SELECT 1;", "DROP TABLE t", "INSERT INTO t SELECT 1", "WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c",
    "UPDATE t SET a = 1", "DELETE FROM t", "MERGE INTO t USING s ON 1=1 WHEN MATCHED THEN DELETE", "SELECT 1 FROM t WHERE (select 1) = 1 DELETE",
    "CREATE TABLE x AS SELECT 1", "ALTER TABLE t ADD COLUMNS (a int)", "TRUNCATE TABLE t", "GRANT SELECT ON t TO u", "OPTIMIZE t",
    "VACUUM t", "SELECT 1 /* unterminated", "SELECT 'unterminated", "", "   ", "EXPLAIN SELECT 1", "SELECT 1 FROM t;DROP TABLE t",
])  # fmt: skip
def test_non_queries_and_dml_are_rejected(sql):
    with pytest.raises(TransformConfigError):
        check_query_text(sql)


def test_strip_noise_hides_comments_strings_and_quoted_identifiers():
    assert "drop" not in strip_noise("SELECT 'drop' /* delete */ -- insert\n, `update` FROM t").lower()
    assert strip_noise("SELECT 'a''b' FROM t").count("'") == 0


def test_parameter_markers_are_found_outside_strings_only():
    assert parameters_used("SELECT * FROM t WHERE a > :min AND b = ':notparam' /* :c */ AND d::int > :max") == {"min", "max"}


def test_placeholders_resolve_only_to_declared_inputs_and_earlier_steps():
    sql = "SELECT * FROM {{ input.orders }} o JOIN {{ step.1 }} s ON 1=1"
    assert resolve_placeholders(sql, {"orders": "__in_0"}, {1: "__step_1"}) == "SELECT * FROM __in_0 o JOIN __step_1 s ON 1=1"
    for bad, kw in (("{{ input.secret }}", "declared"), ("{{ step.2 }}", "earlier step"), ("{{ delta.orders }}", "not supported"),
                    ("{{ input.orders ", "malformed"), ("{{ ; DROP TABLE x }}", "malformed")):  # fmt: skip
        with pytest.raises(TransformConfigError, match=kw):
            resolve_placeholders(f"SELECT * FROM {bad}", {"orders": "v"}, {1: "s"})


def test_parameters_are_never_substituted_into_the_text():
    sql = "SELECT * FROM {{ input.orders }} WHERE a = :v"
    out = resolve_placeholders(sql, {"orders": "__in_0"}, {})
    assert ":v" in out and parameters_used(out) == {"v"}  # bound by the engine as a typed literal, still a marker here
