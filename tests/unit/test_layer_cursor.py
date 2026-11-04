"""LayerCursorRepo: same conditional-advance contract as WatermarkRepo (pinned by running both through identical sequences)."""

import threading

import pytest

from de_accelerator.audit.layer_cursor import LayerCursorRepo
from de_accelerator.audit.repos import WatermarkRepo
from de_accelerator.core.errors import WatermarkConflictError
from de_accelerator.metadata.migrations import MIGRATIONS, Migrator
from de_accelerator.metadata.store.memory import InMemoryTableStore


@pytest.fixture
def store():
    s = InMemoryTableStore()
    Migrator(s, MIGRATIONS).apply()
    return s


def test_first_advance_and_idempotent_repeat(store):
    c = LayerCursorRepo(store)
    assert c.get("o", "SILVER") is None and c.applied_seq("o", "SILVER") == 0
    assert c.advance("o", "SILVER", 3, "b1") is True
    cur = c.get("o", "SILVER")
    assert cur is not None and cur.applied_seq == 3 and cur.applied_batch_id == "b1" and cur.input_versions is None
    assert c.advance("o", "SILVER", 3, "b1") is False  # the same batch advanced it already: idempotent no-op
    assert store.count("wm_layer_cursor") == 1


def test_conflicts_and_independent_layers(store):
    c = LayerCursorRepo(store)
    c.advance("o", "SILVER", 5, "b5")
    with pytest.raises(WatermarkConflictError):
        c.advance("o", "SILVER", 5, "other")  # same seq, different batch
    with pytest.raises(WatermarkConflictError):
        c.advance("o", "SILVER", 2, "b2")  # never backwards
    assert c.advance("o", "GOLD", 1, "g1") is True  # layers are independent cursors
    assert c.applied_seq("o", "SILVER") == 5 and c.applied_seq("o", "GOLD") == 1
    assert c.advance("o", "SILVER", 6, "b6") is True


def test_gold_input_versions_round_trip(store):
    c = LayerCursorRepo(store)
    c.advance("fact", "GOLD", 1, "g1", {"orders": 7, "customers": 3})
    assert c.get("fact", "GOLD").input_versions == {"orders": 7, "customers": 3}  # type: ignore[union-attr]
    c.advance("fact", "GOLD", 2, "g2", {"orders": 9, "customers": 3})
    assert c.get("fact", "GOLD").input_versions == {"orders": 9, "customers": 3}  # type: ignore[union-attr]


def test_rewind_is_an_explicit_admin_operation(store):
    c = LayerCursorRepo(store)
    c.advance("o", "SILVER", 4, "b4")
    c.rewind("o", "SILVER", 1, "b1")
    assert c.applied_seq("o", "SILVER") == 1
    assert c.advance("o", "SILVER", 2, "b2") is True


def test_concurrent_advances_exactly_one_winner(store):
    c = LayerCursorRepo(store)
    results: list[object] = []

    def go(name: str) -> None:
        try:
            results.append(c.advance("o", "SILVER", 1, name))
        except WatermarkConflictError as exc:
            results.append(exc)

    ts = [threading.Thread(target=go, args=(f"b{i}",)) for i in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sum(r is True for r in results) == 1 and sum(isinstance(r, WatermarkConflictError) for r in results) == 5
    assert c.get("o", "SILVER").applied_seq == 1  # type: ignore[union-attr]


def test_contract_equivalent_to_watermark_repo(store):
    """Identical call sequences give identical outcomes on WatermarkRepo (M2) and LayerCursorRepo (M3)."""
    wm, lc = WatermarkRepo(store), LayerCursorRepo(store)
    script = [(1, "a"), (1, "a"), (1, "x"), (3, "c"), (2, "b"), (4, "d"), (4, "d"), (5, "e")]

    def outcome(fn, *args) -> object:
        try:
            return fn(*args)
        except WatermarkConflictError:
            return "conflict"

    for seq, bid in script:
        a = outcome(wm.commit, "o", "live", None, seq, bid)
        b = outcome(lc.advance, "o", "SILVER", seq, bid)
        assert a == b, (seq, bid, a, b)
