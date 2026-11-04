"""Repositories for the batch protocol on top of the M1 `TableStore` (works on in-memory and Delta stores).

Concurrency/atomicity rules (doc 04): manifest + receipts are insert-once; events are append-only with `event_seq` allocated via
insert-if-absent; the watermark moves only by conditional update; counts returned by the store are never trusted for decisions.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from de_accelerator.audit.models import (
    ABANDONED,
    BRONZE_COMMITTED,
    BRONZE_WRITTEN,
    DONE,
    EXTRACTING,
    FAILED_FATAL,
    FAILED_RETRYABLE,
    PLANNED,
    PROGRESS_EVENTS,
    SEALED,
    SRC_WM_COMMITTED,
    TERMINAL,
    Manifest,
    Receipt,
    Watermark,
    batch_id_for,
)
from de_accelerator.core.errors import WatermarkConflictError
from de_accelerator.metadata.store.base import TableStore, now_utc


def _utc(d: datetime) -> datetime:
    """Compare instants safely: a naive datetime is UTC by convention in this codebase."""
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)  # noqa: UP017


_ORDER = {PLANNED: 1, EXTRACTING: 2, SEALED: 3, BRONZE_WRITTEN: 4, BRONZE_COMMITTED: 5, SRC_WM_COMMITTED: 6, DONE: 7}


class ManifestRepo:
    def __init__(self, store: TableStore) -> None:
        self.store = store

    def list(self, object_code: str, stream_key: str) -> list[Manifest]:
        rows = self.store.select("run_batch_manifest", {"object_code": object_code, "stream_key": stream_key})
        return sorted((Manifest.from_row(r) for r in rows), key=lambda m: m.batch_seq)

    def next_seq(self, object_code: str, stream_key: str) -> int:
        ms = self.list(object_code, stream_key)
        return (ms[-1].batch_seq + 1) if ms else 1

    def get(self, batch_id: str) -> Manifest | None:
        rows = self.store.select("run_batch_manifest", {"batch_id": batch_id})
        return Manifest.from_row(rows[0]) if rows else None

    def create(self, m: Manifest) -> tuple[Manifest, bool]:
        """Insert-once. Returns (the manifest that now exists, created_by_me). Never trusts store counts."""
        row = m.to_row()
        row["planned_ts"] = row["planned_ts"] or now_utc()
        self.store.insert_if_absent("run_batch_manifest", [row], ["object_code", "stream_key", "batch_seq"])
        existing = self.get(m.batch_id)
        assert existing is not None
        return existing, existing.manifest_hash == m.manifest_hash and existing.planned_by_run_id == m.planned_by_run_id


class EventLog:
    def __init__(self, store: TableStore) -> None:
        self.store = store

    def append(self, batch_id: str, event_type: str, *, stage: str | None = None, run_id: str | None = None, attempt: int = 1,
               **payload: Any) -> int:  # fmt: skip
        """Append with a monotonically increasing event_seq (insert-if-absent loop handles concurrent appenders)."""
        for _ in range(50):
            seq = max((e["event_seq"] for e in self.store.select("run_batch_event", {"batch_id": batch_id})), default=0) + 1
            # `_nonce` makes every append unique, so "did MY insert win this event_seq?" is decided by payload equality - never by
            # comparing timestamps (stores may round or re-type them) and never by trusting insert counts.
            body = {**payload, "_nonce": uuid.uuid4().hex}
            row = {"batch_id": batch_id, "event_seq": seq, "event_type": event_type, "stage": stage, "run_id": run_id,
                   "attempt": attempt, "payload": json.dumps(body, sort_keys=True, default=str), "event_ts": now_utc()}  # fmt: skip
            self.store.insert_if_absent("run_batch_event", [row], ["batch_id", "event_seq"])
            got = self.store.select("run_batch_event", {"batch_id": batch_id, "event_seq": seq})
            if got and got[0]["payload"] == row["payload"]:
                return seq
        raise RuntimeError("could not allocate event_seq")  # pragma: no cover

    def events(self, batch_id: str) -> list[dict[str, Any]]:
        return sorted(self.store.select("run_batch_event", {"batch_id": batch_id}), key=lambda e: e["event_seq"])

    def state(self, batch_id: str) -> str:
        """Fold: furthest progress event wins; a failure after progress is reported as FAILED_* only if it is the last event."""
        evs = self.events(batch_id)
        if not evs:
            return "UNKNOWN"
        if any(e["event_type"] in TERMINAL for e in evs):
            return str(next(e["event_type"] for e in reversed(evs) if e["event_type"] in TERMINAL))
        last = evs[-1]["event_type"]
        if last in (FAILED_RETRYABLE, FAILED_FATAL):
            return str(last)
        return str(max((e["event_type"] for e in evs if e["event_type"] in PROGRESS_EVENTS), key=lambda t: _ORDER.get(t, 0)))


class ReceiptRepo:
    def __init__(self, store: TableStore) -> None:
        self.store = store

    def get(self, batch_id: str, layer: str = "BRONZE") -> dict[str, Any] | None:
        rows = self.store.select("run_target_receipt", {"batch_id": batch_id, "layer": layer})
        return rows[0] if rows else None

    def insert(self, r: Receipt) -> dict[str, Any]:
        row = {"batch_id": r.batch_id, "layer": r.layer, "target_table": r.target_table, "delta_version": r.delta_version,
               "commit_tag": r.commit_tag, "rows_written": r.rows_written, "rows_rejected": r.rows_rejected,
               "content_checksum": r.content_checksum, "verified_ts": now_utc(), "txn_app_id": r.txn_app_id,
               "txn_version": r.txn_version, "rows_read": r.rows_read}  # fmt: skip
        self.store.insert_if_absent("run_target_receipt", [row], ["batch_id", "layer", "target_table"])
        got = self.get(r.batch_id, r.layer)
        assert got is not None
        return got


class WatermarkRepo:
    TABLE = "wm_watermark"

    def __init__(self, store: TableStore) -> None:
        self.store = store

    def get(self, object_code: str, stream_key: str) -> Watermark | None:
        rows = self.store.select(self.TABLE, {"object_code": object_code, "stream_key": stream_key})
        if not rows:
            return None
        r = rows[0]
        return Watermark(r["object_code"], r["stream_key"], r["committed_value"], int(r["committed_seq"]), r["committed_batch_id"],
                         int(r["epoch"] or 0))  # fmt: skip

    def commit(self, object_code: str, stream_key: str, value: str | None, seq: int, batch_id: str) -> bool:
        """Advance the cursor to (value, seq). Returns True if advanced now, False if it was already advanced by this batch.
        Raises WatermarkConflictError if the precondition fails for any other reason. Idempotent."""
        cur = self.get(object_code, stream_key)
        prev_seq = cur.committed_seq if cur else 0
        if cur and cur.committed_seq >= seq:
            if cur.committed_batch_id == batch_id:
                return False
            raise WatermarkConflictError(f"cursor already at seq {cur.committed_seq} by batch {cur.committed_batch_id}",
                                         object=object_code, stream=stream_key)  # fmt: skip
        ts = now_utc()
        if cur is None:
            self.store.insert_if_absent(self.TABLE, [{"object_code": object_code, "stream_key": stream_key,
                                                      "committed_value": value, "committed_seq": seq, "committed_batch_id": batch_id,
                                                      "updated_ts": ts, "epoch": 0}], ["object_code", "stream_key"])  # fmt: skip
            now = self.get(object_code, stream_key)
            if now and now.committed_batch_id == batch_id and now.committed_seq == seq:
                return True
        else:
            n = self.store.update_where(self.TABLE, {"committed_value": value, "committed_seq": seq, "committed_batch_id": batch_id,
                                                     "updated_ts": ts}, {"object_code": object_code, "stream_key": stream_key,
                                                                         "committed_seq": prev_seq})  # fmt: skip
            if n == 1:
                return True
            now = self.get(object_code, stream_key)
            if now and now.committed_batch_id == batch_id and now.committed_seq == seq:
                return False  # a concurrent identical commit won
        raise WatermarkConflictError("watermark precondition failed (concurrent change)", object=object_code, stream=stream_key)

    def rewind(self, object_code: str, stream_key: str, value: str | None, seq: int, batch_id: str | None) -> None:
        """Audited admin operation (post-restore). Caller records the audit event; plain conditional-free update."""
        self.store.update_where(self.TABLE, {"committed_value": value, "committed_seq": seq, "committed_batch_id": batch_id,
                                             "updated_ts": now_utc()}, {"object_code": object_code, "stream_key": stream_key})  # fmt: skip


class LeaseRepo:
    TABLE = "run_batch_lease"

    def __init__(self, store: TableStore) -> None:
        self.store = store

    def acquire(self, object_code: str, stream_key: str, owner: str, now: datetime, ttl: timedelta) -> int | None:
        """Advisory lease. Returns the epoch if acquired/renewed, None if a live other owner holds it."""
        rows = self.store.select(self.TABLE, {"object_code": object_code, "stream_key": stream_key})
        expiry = now + ttl
        if not rows:
            self.store.insert_if_absent(self.TABLE, [{"object_code": object_code, "stream_key": stream_key, "owner_run_id": owner,
                                                      "epoch": 1, "lease_expiry": expiry}], ["object_code", "stream_key"])  # fmt: skip
            r = self.store.select(self.TABLE, {"object_code": object_code, "stream_key": stream_key})[0]
            return int(r["epoch"]) if r["owner_run_id"] == owner else None
        r = rows[0]
        if r["owner_run_id"] == owner:
            self.store.update_where(self.TABLE, {"lease_expiry": expiry}, {"object_code": object_code, "stream_key": stream_key,
                                                                         "owner_run_id": owner})  # fmt: skip
            return int(r["epoch"])
        if r["lease_expiry"] is None or _utc(r["lease_expiry"]) <= _utc(now):
            n = self.store.update_where(self.TABLE, {"owner_run_id": owner, "epoch": int(r["epoch"]) + 1, "lease_expiry": expiry},
                                        {"object_code": object_code, "stream_key": stream_key, "epoch": r["epoch"]})  # fmt: skip
            return int(r["epoch"]) + 1 if n == 1 else None
        return None

    def release(self, object_code: str, stream_key: str, owner: str, now: datetime) -> None:
        self.store.update_where(self.TABLE, {"lease_expiry": now - timedelta(seconds=1)},
                                {"object_code": object_code, "stream_key": stream_key, "owner_run_id": owner})  # fmt: skip


class RunLog:
    """run_pipeline / run_object / run_attempt / run_error / run_lineage / run_schema_snapshot writers (plain inserts)."""

    def __init__(self, store: TableStore) -> None:
        self.store = store

    def start_pipeline(self, run_id: str, execution: str | None, env: str, trigger: str, fw: str, release_id: str, snapshot_id: str,
                       parent: str | None = None) -> None:  # fmt: skip
        self.store.insert_if_absent("run_pipeline", [{"run_id": run_id, "execution_code": execution, "env_id": env, "trigger_type": trigger,
                                                      "framework_version": fw, "release_id": release_id, "snapshot_id": snapshot_id,
                                                      "status": "RUNNING", "start_ts": now_utc(), "end_ts": None,
                                                      "parent_run_id": parent}], ["run_id"])  # fmt: skip

    def end_pipeline(self, run_id: str, status: str) -> None:
        self.store.update_where("run_pipeline", {"status": status, "end_ts": now_utc()}, {"run_id": run_id})

    def object_result(self, run_id: str, object_code: str, status: str, attempt: int, rows_read: int | None, rows_written: int | None,
                      rows_rejected: int | None, duration_s: float, error_class: str | None) -> str:  # fmt: skip
        oid = str(uuid.uuid4())
        self.store.insert("run_object", [{"run_object_id": oid, "run_id": run_id, "object_code": object_code, "layer": "BRONZE",
                                          "status": status, "attempt": attempt, "rows_read": rows_read, "rows_written": rows_written,
                                          "rows_rejected": rows_rejected, "duration_s": duration_s, "error_class": error_class}])  # fmt: skip
        return oid

    def attempt(self, run_object_id: str, n: int, error: str | None, start: datetime, end: datetime, reason: str | None) -> None:
        self.store.insert("run_attempt", [{"run_object_id": run_object_id, "attempt_no": n, "error": error, "start_ts": start,
                                           "end_ts": end, "retry_reason": reason}])  # fmt: skip

    def error(self, run_id: str, object_code: str, error_class: str, code: str, message: str, context: dict[str, Any]) -> None:
        self.store.insert("run_error", [{"error_id": str(uuid.uuid4()), "run_id": run_id, "object_code": object_code,
                                         "error_class": error_class, "code": code, "message": message[:2000],
                                         "context": json.dumps(context, default=str, sort_keys=True), "error_ts": now_utc()}])  # fmt: skip

    def lineage(self, batch_id: str, source_object: str, target_object: str) -> None:
        self.store.insert_if_absent("run_lineage", [{"edge_id": str(uuid.uuid5(uuid.NAMESPACE_OID, f"{batch_id}|{target_object}")),
                                                     "batch_id": batch_id, "source_object": source_object,
                                                     "target_object": target_object, "source_column": None, "target_column": None,
                                                     "layer": "BRONZE"}], ["edge_id"])  # fmt: skip

    def schema_snapshot(self, object_code: str, batch_id: str, schema_hash: str, schema_json: str, change_type: str) -> None:
        self.store.insert_if_absent("run_schema_snapshot", [{"object_code": object_code, "batch_id": batch_id,
                                                             "schema_hash": schema_hash, "schema_json": schema_json,
                                                             "change_type": change_type}], ["object_code", "batch_id"])  # fmt: skip


def open_batch(manifests: ManifestRepo, events: EventLog, object_code: str, stream_key: str) -> Manifest | None:
    """The newest non-terminal batch of the stream, if any (recovery entry point)."""
    for m in reversed(manifests.list(object_code, stream_key)):
        if events.state(m.batch_id) not in (*TERMINAL, "UNKNOWN") or events.state(m.batch_id) == "UNKNOWN":
            return m
        return None
    return None


__all__ = ["ABANDONED", "BRONZE_COMMITTED", "DONE", "EventLog", "LeaseRepo", "ManifestRepo", "ReceiptRepo", "RunLog",
           "WatermarkRepo", "batch_id_for", "open_batch"]  # fmt: skip
