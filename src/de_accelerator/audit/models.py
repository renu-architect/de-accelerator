"""Plain data objects for the batch protocol (doc 04)."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from de_accelerator.metadata.packs.ids import NAMESPACE, canonical_json, sha256_hex

# derived batch states (fold of run_batch_event)
PLANNED, EXTRACTING, SEALED, BRONZE_WRITTEN, BRONZE_COMMITTED = "PLANNED", "EXTRACTING", "EXTRACTION_SEALED", "BRONZE_WRITTEN", "BRONZE_COMMITTED"
SRC_WM_COMMITTED, DONE, FAILED_RETRYABLE, FAILED_FATAL, ABANDONED = "SRC_WM_COMMITTED", "DONE", "FAILED_RETRYABLE", "FAILED_FATAL", "ABANDONED"
TERMINAL = {DONE, ABANDONED}
PROGRESS_EVENTS = {PLANNED, EXTRACTING, SEALED, BRONZE_WRITTEN, BRONZE_COMMITTED, SRC_WM_COMMITTED, DONE, ABANDONED}


def batch_id_for(object_code: str, stream_key: str, batch_seq: int) -> str:
    return str(uuid.uuid5(NAMESPACE, f"batch|{object_code}|{stream_key}|{batch_seq}"))


@dataclass(frozen=True)
class Manifest:
    batch_id: str
    object_code: str
    stream_key: str
    batch_seq: int
    batch_type: str
    snapshot_id: str
    release_id: str
    wm_from: str | None
    wm_to: str | None
    file_list: list[dict[str, Any]] | None
    expected_targets: list[dict[str, Any]]
    plan_json: dict[str, Any]
    planned_by_run_id: str
    planned_ts: datetime | None = None

    @property
    def manifest_hash(self) -> str:
        return sha256_hex(canonical_json({
            "object": self.object_code, "stream": self.stream_key, "seq": self.batch_seq, "type": self.batch_type,
            "wm_from": self.wm_from, "wm_to": self.wm_to, "files": self.file_list, "targets": self.expected_targets,
            "plan": self.plan_json,
        }))  # fmt: skip

    def to_row(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id, "object_code": self.object_code, "stream_key": self.stream_key,
            "batch_seq": self.batch_seq, "batch_type": self.batch_type, "snapshot_id": self.snapshot_id,
            "release_id": self.release_id, "wm_from": self.wm_from, "wm_to": self.wm_to,
            "file_list": canonical_json(self.file_list) if self.file_list is not None else None,
            "expected_targets": canonical_json(self.expected_targets), "plan_json": canonical_json(self.plan_json),
            "planned_by_run_id": self.planned_by_run_id, "manifest_hash": self.manifest_hash, "planned_ts": self.planned_ts,
        }  # fmt: skip

    @classmethod
    def from_row(cls, r: dict[str, Any]) -> Manifest:
        return cls(r["batch_id"], r["object_code"], r["stream_key"], int(r["batch_seq"]), r["batch_type"], r["snapshot_id"],
                   r["release_id"], r["wm_from"], r["wm_to"], json.loads(r["file_list"]) if r["file_list"] else None,
                   json.loads(r["expected_targets"] or "[]"), json.loads(r["plan_json"] or "{}"), r["planned_by_run_id"],
                   r["planned_ts"])  # fmt: skip


@dataclass(frozen=True)
class Receipt:
    batch_id: str
    layer: str
    target_table: str
    delta_version: int | None
    commit_tag: str | None
    rows_written: int
    rows_rejected: int = 0
    rows_read: int | None = None
    txn_app_id: str | None = None
    txn_version: int | None = None
    content_checksum: str | None = None


@dataclass(frozen=True)
class Watermark:
    object_code: str
    stream_key: str
    committed_value: str | None
    committed_seq: int
    committed_batch_id: str | None
    epoch: int = 0
