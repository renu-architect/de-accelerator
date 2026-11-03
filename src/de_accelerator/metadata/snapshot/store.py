"""Append-only persistence + integrity verification for run_config_snapshot."""

from __future__ import annotations

import gzip
import json

from de_accelerator.core import versioning
from de_accelerator.core.errors import NotFoundError, SnapshotIntegrityError
from de_accelerator.metadata.packs.ids import canonical_json, sha256_hex
from de_accelerator.metadata.snapshot.builder import SNAPSHOT_FORMAT, Snapshot, SnapshotRef
from de_accelerator.metadata.store.base import TableStore, now_utc

TABLE = "run_config_snapshot"


def _gz(text: str) -> bytes:
    return gzip.compress(text.encode("utf-8"), mtime=0)  # mtime=0 => deterministic bytes


class SnapshotStore:
    def __init__(self, store: TableStore) -> None:
        self.store = store

    def persist(self, snap: Snapshot, created_by: str = "system") -> SnapshotRef:
        """Idempotent: persisting the same snapshot twice is a no-op. Never updates an existing snapshot."""
        text = canonical_json(snap.content)
        if sha256_hex(text) != snap.snapshot_id:
            raise SnapshotIntegrityError("snapshot id does not match its content; refusing to persist")
        blob = _gz(text)
        self.store.insert_if_absent(TABLE, [{
            "snapshot_id": snap.snapshot_id, "release_id": snap.release_id, "env_id": snap.env,
            "scope_json": canonical_json(snap.scope), "content_sha256": snap.snapshot_id, "content_gz": blob,
            "size_bytes": len(text.encode("utf-8")), "entity_counts": json.dumps(snap.counts(), sort_keys=True),
            "framework_version": versioning.framework_version(), "created_by": created_by, "created_ts": now_utc(),
        }], ["snapshot_id"])  # fmt: skip
        self.load(snap.snapshot_id)  # read back + verify (also proves durability of what we just wrote)
        return snap.ref

    def load(self, snapshot_id: str) -> Snapshot:
        rows = self.store.select(TABLE, {"snapshot_id": snapshot_id})
        if not rows:
            raise NotFoundError(f"snapshot not found: {snapshot_id}")
        r = rows[0]
        try:
            text = gzip.decompress(bytes(r["content_gz"])).decode("utf-8")
        except (OSError, EOFError, UnicodeDecodeError) as exc:
            raise SnapshotIntegrityError(f"snapshot {snapshot_id}: corrupt content ({exc})") from exc
        digest = sha256_hex(text)
        if digest != r["content_sha256"] or digest != snapshot_id:
            raise SnapshotIntegrityError(f"snapshot {snapshot_id}: content hash mismatch (stored content was altered)",
                                         expected=snapshot_id, actual=digest)  # fmt: skip
        content = json.loads(text)
        if content.get("snapshot_format") != SNAPSHOT_FORMAT:
            raise SnapshotIntegrityError(f"unsupported snapshot_format {content.get('snapshot_format')}")
        if content["release_id"] != r["release_id"] or content["env"] != r["env_id"]:
            raise SnapshotIntegrityError(f"snapshot {snapshot_id}: release/env columns disagree with content")
        return Snapshot(snapshot_id, content["release_id"], content["env"], content["scope"], content)
