import pytest
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from de_accelerator.core.errors import SchemaChangeError
from de_accelerator.processing.bronze.schema_policy import decide, is_safe_widening


def S(**cols):
    return StructType([StructField(k, v) for k, v in cols.items()])


@pytest.mark.parametrize("a,b,ok", [
    (IntegerType(), LongType(), True), (LongType(), IntegerType(), False), (FloatType(), DoubleType(), True),
    (DecimalType(10, 2), DecimalType(12, 2), True), (DecimalType(10, 2), DecimalType(10, 3), False),
    (DecimalType(10, 2), DecimalType(12, 4), True), (DateType(), TimestampType(), True), (TimestampType(), DateType(), False),
    (IntegerType(), DecimalType(12, 0), True), (IntegerType(), DecimalType(5, 0), False), (StringType(), IntegerType(), False),
    (IntegerType(), IntegerType(), True),
])
def test_safe_widening(a, b, ok):
    assert is_safe_widening(a, b) is ok


def test_created_when_no_table():
    assert decide(S(a=IntegerType()), None, "ADD_COLUMNS").change_type == "CREATED"


def test_no_change_and_dropped_columns_kept():
    d = decide(S(a=IntegerType()), S(a=IntegerType(), b=StringType(), _batch_id=StringType()), "ADD_COLUMNS")
    assert d.change_type == "DROPPED" and d.dropped == ["b"] and not d.add_columns  # audit (_) columns are not compared


@pytest.mark.parametrize("mode,outcome", [("FAIL", "raise"), ("ADD_COLUMNS", "ADDED"), ("ALLOW_WIDENING", "ADDED"), ("RESCUE", "ADDED")])
def test_new_column_per_mode(mode, outcome):
    inc, ex = S(a=IntegerType(), c=StringType()), S(a=IntegerType())
    if outcome == "raise":
        with pytest.raises(SchemaChangeError):
            decide(inc, ex, mode)
    else:
        d = decide(inc, ex, mode)
        assert d.change_type == outcome and d.add_columns == ["c"] and d.needs_merge_schema


def test_case_insensitive_names():
    assert decide(S(ID=IntegerType()), S(id=IntegerType()), "FAIL").change_type == "NONE"


@pytest.mark.parametrize("mode,expect", [("FAIL", "raise"), ("ADD_COLUMNS", "raise"), ("ALLOW_WIDENING", "WIDENED"), ("RESCUE", "RESCUED")])
def test_incoming_wider_than_table(mode, expect):
    inc, ex = S(a=LongType()), S(a=IntegerType())
    if expect == "raise":
        with pytest.raises(SchemaChangeError):
            decide(inc, ex, mode)
    else:
        assert decide(inc, ex, mode).change_type == expect


def test_incoming_narrower_than_table_is_cast_up_in_every_mode():
    for mode in ("FAIL", "ADD_COLUMNS", "ALLOW_WIDENING", "RESCUE"):
        d = decide(S(a=IntegerType()), S(a=LongType()), mode)
        assert d.change_type == "NONE" and d.cast_to_table == {"a": LongType()}


@pytest.mark.parametrize("mode", ["FAIL", "ADD_COLUMNS", "ALLOW_WIDENING"])
def test_incompatible_type_change(mode):
    with pytest.raises(SchemaChangeError, match="incompatible"):
        decide(S(a=StringType()), S(a=IntegerType()), mode)


def test_incompatible_type_is_rescued():
    d = decide(S(a=StringType(), b=IntegerType()), S(a=IntegerType(), b=IntegerType()), "RESCUE")
    assert d.rescue == ["a"] and d.change_type == "RESCUED"


def test_combined_change_types():
    d = decide(S(a=LongType(), n=StringType()), S(a=IntegerType(), gone=StringType()), "ALLOW_WIDENING")
    assert d.change_type == "ADDED+WIDENED+DROPPED"
