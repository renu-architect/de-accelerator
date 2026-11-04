"""Independent per-layer cursors (Silver/Gold) on `wm_layer_cursor` (ADR-0009, doc 08).

Semantics mirror `WatermarkRepo.commit`: the cursor advances only by a conditional update on the previous `applied_seq`, is idempotent
when the same batch already advanced it, and raises `WatermarkConflictError` for any other precondition failure. It is a separate
repository (different table/columns, adds `input_versions`); a shared contract test pins the equivalent behaviour.
Silver: `applied_seq` = last consumed Bronze `batch_seq`. Gold: `applied_seq` = last committed Gold unit sequence and
`input_versions` = {input object: consumed Delta version}.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from de_accelerator.core.errors import WatermarkConflictError
from de_accelerator.metadata.store.base import TableStore, now_utc


@dataclass(frozen=True)
class LayerCursor:
    object_code: str
    layer: str
    applied_seq: int
    applied_batch_id: str | None
    input_versions: dict[str, int] | None = None


class LayerCursorRepo:
    TABLE = "wm_layer_cursor"

    def __init__(self, store: TableStore) -> None:
        self.store = store

    def get(self, object_code: str, layer: str) -> LayerCursor | None:
        rows = self.store.select(self.TABLE, {"object_code": object_code, "layer": layer})
        if not rows:
            return None
        r = rows[0]
        iv = r.get("input_versions")
        return LayerCursor(r["object_code"], r["layer"], int(r["applied_seq"]), r["applied_batch_id"],
                           {k: int(v) for k, v in json.loads(iv).items()} if iv else None)  # fmt: skip

    def applied_seq(self, object_code: str, layer: str) -> int:
        cur = self.get(object_code, layer)
        return cur.applied_seq if cur else 0

    def advance(
        self, object_code: str, layer: str, seq: int, batch_id: str, input_versions: dict[str, int] | None = None
    ) -> bool:
        """Advance to `seq`. True if advanced now, False if this very batch had already advanced it. Raises WatermarkConflictError otherwise."""
        cur = self.get(object_code, layer)
        prev = cur.applied_seq if cur else 0
        if cur and cur.applied_seq >= seq:
            if cur.applied_batch_id == batch_id:
                return False
            raise WatermarkConflictError(f"cursor already at seq {cur.applied_seq} by batch {cur.applied_batch_id}",
                                         object=object_code, layer=layer)  # fmt: skip
        iv = json.dumps(input_versions, sort_keys=True) if input_versions is not None else None
        ts = now_utc()
        if cur is None:
            self.store.insert_if_absent(self.TABLE, [{"object_code": object_code, "layer": layer, "applied_seq": seq,
                                                      "applied_batch_id": batch_id, "updated_ts": ts, "input_versions": iv}],
                                        ["object_code", "layer"])  # fmt: skip
            now = self.get(object_code, layer)
            if now and now.applied_batch_id == batch_id and now.applied_seq == seq:
                return True
        else:
            n = self.store.update_where(self.TABLE, {"applied_seq": seq, "applied_batch_id": batch_id, "updated_ts": ts,
                                                     "input_versions": iv}, {"object_code": object_code, "layer": layer,
                                                                             "applied_seq": prev})  # fmt: skip
            if n == 1:
                return True
            now = self.get(object_code, layer)
            if now and now.applied_batch_id == batch_id and now.applied_seq == seq:
                return False  # a concurrent identical advance won
        raise WatermarkConflictError("cursor precondition failed (concurrent change)", object=object_code, layer=layer)

    def rewind(self, object_code: str, layer: str, seq: int, batch_id: str | None) -> None:
        """Audited admin operation (the caller records the audit event)."""
        self.store.update_where(self.TABLE, {"applied_seq": seq, "applied_batch_id": batch_id, "updated_ts": now_utc()},
                                {"object_code": object_code, "layer": layer})  # fmt: skip
