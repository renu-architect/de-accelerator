"""The closed vocabulary shared by the validator and the engines."""

import pytest

from de_accelerator.core import transform_spec as ts


def test_parse_order_accepts_column_lists_and_rejects_anything_else():
    assert ts.parse_order("ModifiedDate desc, Id") == [("ModifiedDate", True), ("Id", False)]
    assert ts.parse_order("a ASC") == [("a", False)]
    for bad in ("a; drop table x", "a desc desc", "", "a b c", "1a", "a desc,", "f(a)"):
        assert ts.parse_order(bad) is None


def test_identifier_and_target_type_checks():
    assert ts.is_ident("customer_id") and ts.is_ident("_x") and not ts.is_ident("a b") and not ts.is_ident("1a") and not ts.is_ident("a;b")
    for good in ("string", "BIGINT", "decimal(18,2)", "decimal(10, 0)", "timestamp", "date", "boolean"):
        assert ts.TARGET_TYPE.match(good)
    for bad in ("varchar(10)", "decimal", "array<int>", "decimal(18,2); drop", "map<string,int>", "binary"):
        assert not ts.TARGET_TYPE.match(bad)


@pytest.mark.parametrize("rules", [None, {}, {"ops": []}, {"ops": [{"op": "trim"}, {"op": "upper"}, {"op": "map_values", "map": {"UK": "GB"}}]},
                                   {"ops": [{"op": "replace", "from": ",", "to": ""}, {"op": "parse_date", "format": "yyyy-MM-dd"}]},
                                   {"ops": [{"op": "default", "value": 0}, {"op": "nullif", "values": ["", "N/A"]}]}])  # fmt: skip
def test_valid_cleanse_rules(rules):
    assert ts.check_cleanse_rules(rules) == []


@pytest.mark.parametrize("rules, needle", [
    ([{"op": "trim"}], "single key 'ops'"), ({"ops": "trim"}, "single key 'ops'"), ({"ops": [], "x": 1}, "single key 'ops'"),
    ({"ops": ["trim"]}, "must be an object"), ({"ops": [{"x": 1}]}, "'op' key"), ({"ops": [{"op": "eval"}]}, "not in the allowlist"),
    ({"ops": [{"op": "regex_replace", "pattern": "a", "to": "b"}]}, "not in the allowlist"),
    ({"ops": [{"op": "nullif"}]}, "missing parameter 'values'"), ({"ops": [{"op": "nullif", "values": "x"}]}, "wrong type"),
    ({"ops": [{"op": "trim", "extra": 1}]}, "unknown parameters"), ({"ops": [{"op": "parse_date", "format": "y"}, {"op": "trim"}]}, "must be the last"),
])  # fmt: skip
def test_invalid_cleanse_rules(rules, needle):
    assert any(needle in p for p in ts.check_cleanse_rules(rules)), ts.check_cleanse_rules(rules)


def test_params_must_be_typed_scalars():
    assert ts.check_params(None) == [] and ts.check_params({"a": {"type": "decimal", "value": "0"}, "b_1": {"type": "bool", "value": True}}) == []
    assert ts.check_params([1])
    assert ts.check_params({"a": {"type": "blob", "value": 1}})
    assert ts.check_params({"a": {"type": "string"}})
    assert ts.check_params({"a b": {"type": "string", "value": "x"}})
    assert ts.check_params({"a": {"type": "string", "value": [1]}})
