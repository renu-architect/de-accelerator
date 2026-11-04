"""Silver unit planning: which receipted Bronze `live` batches form the next unit (D1)."""

from __future__ import annotations

from dataclasses import dataclass

from de_accelerator.audit.models import ABANDONED
from de_accelerator.audit.repos import EventLog, ManifestRepo, ReceiptRepo

BRONZE_STREAM = "live"
MAX_BATCHES_PER_UNIT = 50


@dataclass(frozen=True)
class UnitPlan:
    seq_from: int
    seq_to: int
    batch_ids: tuple[str, ...]
    bronze_rows_expected: int  # sum of Bronze receipt rows_written (authoritative)
    bronze_target: str
    snapshot_like: bool  # the single batch is a full extract (its candidate-absent count is reported, nothing deleted)


def plan_unit(manifests: ManifestRepo, events: EventLog, receipts: ReceiptRepo, object_code: str, cursor_seq: int,
              max_batches: int = MAX_BATCHES_PER_UNIT) -> UnitPlan | None:  # fmt: skip
    """Consecutive eligible batches after the cursor. A batch is eligible when it has a Bronze receipt (verified commit) and is not
    abandoned; a lower batch that is still open (no receipt) blocks every later one so the order is preserved."""
    eligible = []
    for m in manifests.list(object_code, BRONZE_STREAM):
        if m.batch_seq <= cursor_seq:
            continue
        if events.state(m.batch_id) == ABANDONED:
            continue
        r = receipts.get(m.batch_id, "BRONZE")
        if r is None:
            break
        eligible.append((m, int(r["rows_written"] or 0)))
    if not eligible:
        return None
    first = eligible[0][0]
    if first.plan_json.get("mode") == "full":
        chosen = eligible[:1]
        snap = True
    else:
        chosen = []
        for m, n in eligible:
            if len(chosen) >= max_batches or (chosen and m.plan_json.get("mode") == "full"):
                break
            chosen.append((m, n))
        snap = False
    target = str(first.expected_targets[0]["table"])
    return UnitPlan(chosen[0][0].batch_seq, chosen[-1][0].batch_seq, tuple(m.batch_id for m, _ in chosen), sum(n for _, n in chosen),
                    target, snap)  # fmt: skip
