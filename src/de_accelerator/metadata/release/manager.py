"""Release manager: active-release pointer, activation, rollback, healing, compatibility gate.

Concurrency: the pointer (cfg_active_release) is changed only through a conditional update on `activation_seq`
(optimistic concurrency). A loser gets ActivationConflictError and changes nothing.
Crash safety: the pointer flip is the single commit point. Everything after it (status updates, history) is idempotent and
healed by `reconcile()`, which runs at the start of every mutating operation.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from typing import Any

from de_accelerator.core import versioning
from de_accelerator.core.errors import ActivationConflictError, CompatibilityError, NotFoundError, ReleaseError
from de_accelerator.core.logging import get_logger
from de_accelerator.metadata.release.common import (
    ACTIVATABLE,
    ACTIVE,
    ROLLED_BACK,
    SUPERSEDED,
)
from de_accelerator.metadata.release.history import ChangeDiffer
from de_accelerator.metadata.store.base import TableStore, now_utc

log = get_logger("release")
POINTER, RELEASES, EVENTS = "cfg_active_release", "cfg_pack_release", "cfg_release_event"


class ReleaseManager:
    def __init__(self, store: TableStore, env: str, actor: str = "system",
                 deployed_schema_version: Callable[[], int] | None = None) -> None:  # fmt: skip
        self.store = store
        self.env = env
        self.actor = actor
        self._deployed = deployed_schema_version or self._default_deployed_version
        self.differ = ChangeDiffer(store)

    # ---- reads -------------------------------------------------------------------------------
    def _default_deployed_version(self) -> int:
        from de_accelerator.metadata.migrations import MIGRATIONS, Migrator

        return Migrator(self.store, MIGRATIONS).current_version()

    def pointer(self) -> dict[str, Any] | None:
        rows = self.store.select(POINTER, {"env_id": self.env})
        return rows[0] if rows else None

    def active_release_id(self) -> str | None:
        p = self.pointer()
        return p["release_id"] if p else None

    def get_release(self, release_id: str) -> dict[str, Any]:
        rows = self.store.select(RELEASES, {"release_id": release_id})
        if not rows:
            raise NotFoundError(f"release not found: {release_id}")
        return rows[0]

    def list_releases(self) -> list[dict[str, Any]]:
        return sorted(self.store.select(RELEASES, {"env_id": self.env}), key=lambda r: r["staged_ts"] or now_utc())

    def events(self, release_id: str | None = None) -> list[dict[str, Any]]:
        where = {"env_id": self.env} | ({"release_id": release_id} if release_id else {})
        return sorted(self.store.select(EVENTS, where), key=lambda e: e["event_ts"])

    # ---- audit -------------------------------------------------------------------------------
    def record_event(self, release_id: str, event_type: str, **detail: Any) -> None:
        self.store.insert(EVENTS, [{
            "event_id": str(uuid.uuid4()), "release_id": release_id, "env_id": self.env, "event_type": event_type,
            "actor": self.actor, "detail": json.dumps(detail, sort_keys=True, default=str), "event_ts": now_utc(),
        }])  # fmt: skip

    # ---- compatibility gate ------------------------------------------------------------------
    def check_compat(self, rel: dict[str, Any]) -> None:
        spec = rel["requires_core"]
        core = versioning.framework_version()
        if spec and not versioning.satisfies(core, spec):
            raise CompatibilityError(f"release requires core '{spec}' but framework is {core}", release_id=rel["release_id"])
        need, have = rel["metadata_schema_version"], self._deployed()
        if need and need > have:
            raise CompatibilityError(f"release needs metadata schema {need}; deployed is {have} (run migrate)",
                                     release_id=rel["release_id"])  # fmt: skip

    # ---- activation / rollback ---------------------------------------------------------------
    def activate(self, release_id: str, expected_seq: int | None = None, ticket: str | None = None) -> dict[str, Any]:
        """Make `release_id` the active release. Idempotent if it is already active."""
        self.reconcile()
        rel = self.get_release(release_id)
        cur = self.pointer()
        if cur and cur["release_id"] == release_id:
            return cur
        if rel["env_id"] != self.env:
            raise ReleaseError(f"release belongs to env {rel['env_id']}, not {self.env}")
        if rel["status"] not in ACTIVATABLE:
            raise ReleaseError(f"release status {rel['status']} cannot be activated (must be one of {sorted(ACTIVATABLE)})")
        self.check_compat(rel)
        return self._flip(rel["release_id"], cur, expected_seq, "ACTIVATE", ticket)

    def rollback(self, to_release_id: str | None = None, reason: str = "", ticket: str | None = None,
                 expected_seq: int | None = None) -> dict[str, Any]:  # fmt: skip
        """Re-point to the previous (or a named) release. No configuration data is rewritten."""
        self.reconcile()
        cur = self.pointer()
        if cur is None:
            raise ReleaseError("nothing to roll back: no active release")
        target_id = to_release_id or cur["previous_release_id"]
        if not target_id:
            raise ReleaseError("no previous release to roll back to")
        if target_id == cur["release_id"]:
            raise ReleaseError("target release is already active")
        target = self.get_release(target_id)
        if target["env_id"] != self.env or target["status"] not in ACTIVATABLE:
            raise ReleaseError(f"cannot roll back to release in status {target['status']}")
        self.check_compat(target)
        return self._flip(target_id, cur, expected_seq, "ROLLBACK", ticket, reason=reason)

    def _flip(self, new_id: str, cur: dict[str, Any] | None, expected_seq: int | None, kind: str,
              ticket: str | None, reason: str = "") -> dict[str, Any]:  # fmt: skip
        cur_seq = cur["activation_seq"] if cur else 0
        if expected_seq is not None and expected_seq != cur_seq:
            self.record_event(new_id, f"{kind}_CONFLICT", expected_seq=expected_seq, actual_seq=cur_seq)
            raise ActivationConflictError(f"active release pointer is at seq {cur_seq}, expected {expected_seq}",
                                          expected=expected_seq, actual=cur_seq)  # fmt: skip
        new_seq = cur_seq + 1
        self.record_event(new_id, f"{kind}_REQUESTED", new_seq=new_seq, from_release=cur["release_id"] if cur else None,
                          reason=reason, ticket=ticket)  # fmt: skip
        ts = now_utc()
        if cur is None:
            first = {"env_id": self.env, "release_id": new_id, "previous_release_id": None, "activation_seq": 1,
                     "activated_by": self.actor, "activated_ts": ts}  # fmt: skip
            self.store.insert_if_absent(POINTER, [first], ["env_id"])
            won = (p := self.pointer()) is not None and p["release_id"] == new_id and p["activation_seq"] == 1
        else:
            n = self.store.update_where(POINTER, {"release_id": new_id, "previous_release_id": cur["release_id"],
                                                  "activation_seq": new_seq, "activated_by": self.actor, "activated_ts": ts},
                                        {"env_id": self.env, "activation_seq": cur_seq})  # fmt: skip
            won = n == 1
        if not won:
            self.record_event(new_id, f"{kind}_CONFLICT", expected_seq=cur_seq)
            raise ActivationConflictError("active release pointer was changed concurrently; re-read and retry", expected=cur_seq)
        log.info("env %s: %s -> release %s (seq %s)", self.env, kind, new_id, new_seq)
        self.reconcile(ticket=ticket)  # finalize statuses + history (idempotent; also what crash recovery runs)
        return self.pointer() or {}

    # ---- healing / finalisation --------------------------------------------------------------
    def reconcile(self, ticket: str | None = None) -> None:
        """Derive release statuses and history from the pointer (the single source of truth). Idempotent."""
        p = self.pointer()
        if p is None:
            return
        new_id, prev_id, seq = p["release_id"], p["previous_release_id"], p["activation_seq"]
        rels = {r["release_id"]: r for r in self.store.select(RELEASES, {"env_id": self.env})}
        new = rels.get(new_id)
        if new is None:
            return
        if new["status"] != ACTIVE or new["activated_ts"] is None:
            self.store.update_where(RELEASES, {"status": ACTIVE, "activated_ts": p["activated_ts"]}, {"release_id": new_id})
            self.record_event(new_id, "ACTIVATED", seq=seq)
        for rid, r in rels.items():
            if rid != new_id and r["status"] == ACTIVE:
                evs = self.store.select(EVENTS, {"release_id": new_id, "env_id": self.env})
                requested = [e for e in evs if e["event_type"] == "ROLLBACK_REQUESTED"
                             and json.loads(e["detail"]).get("new_seq") == seq]
                status = ROLLED_BACK if requested else SUPERSEDED
                self.store.update_where(RELEASES, {"status": status}, {"release_id": rid})
                self.record_event(rid, status, replaced_by=new_id, seq=seq)
        self.differ.record(self.env, prev_id, new_id, seq, p["activated_by"], ticket or new["ticket"])
