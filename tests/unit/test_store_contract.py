"""TableStore contract: runs on in-memory (unit) and local Delta (component)."""

import pytest

from de_accelerator.core.errors import StoreError
from de_accelerator.metadata.schema.tables import table_map
from de_accelerator.metadata.store.base import now_utc

T = table_map()


@pytest.fixture
def tables(store):
    specs = [T["cfg_active_release"], T["cfg_release_event"]]
    for s in specs:
        store.drop_table(s)
        store.create_table(s)
    yield store
    for s in specs:
        store.drop_table(s)


def ptr(env="DEV", rel="r1", seq=1):
    return {"env_id": env, "release_id": rel, "previous_release_id": None, "activation_seq": seq,
            "activated_by": "t", "activated_ts": now_utc()}  # fmt: skip


def test_insert_select_count(tables):
    assert tables.insert("cfg_active_release", [ptr(), ptr("QA")]) == 2
    assert tables.count("cfg_active_release") == 2
    r = tables.select("cfg_active_release", {"env_id": "QA"})
    assert len(r) == 1 and r[0]["release_id"] == "r1"


def test_insert_if_absent_idempotent(tables):
    assert tables.insert_if_absent("cfg_active_release", [ptr()], ["env_id"]) == 1
    assert tables.insert_if_absent("cfg_active_release", [ptr(rel="other")], ["env_id"]) == 0
    assert tables.select("cfg_active_release")[0]["release_id"] == "r1"
    with pytest.raises(StoreError):
        tables.insert_if_absent("cfg_active_release", [{**ptr(), "env_id": None}], ["env_id"])


def test_conditional_update(tables):
    tables.insert("cfg_active_release", [ptr(seq=1)])
    assert tables.update_where("cfg_active_release", {"release_id": "r2", "activation_seq": 2},
                               {"env_id": "DEV", "activation_seq": 1}) == 1
    # stale precondition => 0 rows
    assert tables.update_where("cfg_active_release", {"release_id": "r3", "activation_seq": 2},
                               {"env_id": "DEV", "activation_seq": 1}) == 0
    assert tables.select("cfg_active_release")[0]["release_id"] == "r2"


def test_not_null_enforced(tables):
    with pytest.raises(Exception):  # StoreError (memory) / AnalysisException|Py4J (delta)
        tables.insert("cfg_active_release", [{**ptr(), "release_id": None}])


def test_append_only_blocks_update_delete(tables):
    ev = {"event_id": "e1", "release_id": "r", "env_id": "DEV", "event_type": "X", "actor": "a", "detail": "{}",
          "event_ts": now_utc()}  # fmt: skip
    tables.insert("cfg_release_event", [ev])
    with pytest.raises(Exception):
        tables.update_where("cfg_release_event", {"actor": "evil"}, {"event_id": "e1"})
    with pytest.raises(Exception):
        tables.delete_where("cfg_release_event", {"event_id": "e1"})
    assert tables.select("cfg_release_event")[0]["actor"] == "a"
