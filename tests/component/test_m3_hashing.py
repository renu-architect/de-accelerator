"""Canonical content hashing: typed, null-aware, order-aware and independent of the session time zone."""

import pytest
from pyspark.sql import functions as F

from de_accelerator.processing.common.hashing import content_hash, hex128

pytestmark = pytest.mark.component


def _hash(spark, rows, schema, cols, fn=content_hash):
    df = spark.createDataFrame(rows, schema)
    return [r["h"] for r in df.select(fn(cols).alias("h")).collect()]


def test_equal_rows_equal_hash_and_different_rows_differ(spark):
    h = _hash(spark, [(1, "a"), (1, "a"), (1, "b"), (2, "a")], "k int, v string", ["k", "v"])
    assert h[0] == h[1] and len({h[0], h[2], h[3]}) == 3 and len(h[0]) == 64


def test_null_is_distinct_from_empty_string_and_from_the_text_null(spark):
    h = _hash(spark, [(1, None), (1, ""), (1, "null")], "k int, v string", ["k", "v"])
    assert len(set(h)) == 3


def test_column_order_is_part_of_the_identity(spark):
    h1 = _hash(spark, [("a", "b")], "x string, y string", ["x", "y"])
    h2 = _hash(spark, [("a", "b")], "x string, y string", ["y", "x"])
    assert h1 != h2


def test_types_are_part_of_the_identity(spark):
    assert _hash(spark, [(1,)], "v int", ["v"]) == _hash(spark, [(1,)], "v int", ["v"])
    assert _hash(spark, [(1,)], "v int", ["v"]) != _hash(spark, [("1",)], "v string", ["v"])


def test_timestamp_hash_does_not_depend_on_the_session_time_zone(spark):
    original = spark.conf.get("spark.sql.session.timeZone")
    try:
        out = []
        for tz in ("UTC", "America/Los_Angeles", "Asia/Kolkata"):
            spark.conf.set("spark.sql.session.timeZone", tz)
            inst = spark.sql("SELECT timestamp'2026-03-01 12:00:00+00:00' AS t")  # the same instant in every zone
            out.append(inst.select(content_hash(["t"]).alias("h")).collect()[0]["h"])
    finally:
        spark.conf.set("spark.sql.session.timeZone", original)
    assert len(set(out)) == 1


def test_hex128_is_32_chars_and_prefix_of_the_full_hash(spark):
    df = spark.createDataFrame([(1, "a")], "k int, v string")
    r = df.select(content_hash(["k", "v"]).alias("full"), hex128(["k", "v"]).alias("short")).collect()[0]
    assert len(r["short"]) == 32 and r["full"].startswith(r["short"])


def test_hash_accepts_column_expressions(spark):
    df = spark.createDataFrame([(1, " A ")], "k int, v string")
    r = df.select(content_hash(["k", F.trim("v")]).alias("a"), content_hash(["k", F.lit("A")]).alias("b")).collect()[0]
    assert r["a"] == r["b"]
