"""Pack loader: stage -> verify -> (activate via ReleaseManager). Idempotent, crash-safe, never mixes releases.

Config rows are insert-only and tagged with pack_release_id, so a half-staged release is invisible (the active pointer
does not reference it) and re-running stage completes it without deleting anything.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from de_accelerator.core import versioning
from de_accelerator.core.errors import CompatibilityError, PackError, ReleaseError, ValidationFailedError
from de_accelerator.core.logging import get_logger
from de_accelerator.metadata.packs.bundle import PackBundle
from de_accelerator.metadata.release.common import ACTIVE, FAILED, STAGED, STAGING, release_id_for, to_table_row
from de_accelerator.metadata.release.manager import RELEASES, ReleaseManager
from de_accelerator.metadata.schema.entities import ENTITIES
from de_accelerator.metadata.store.base import TableStore, now_utc
from de_accelerator.metadata.validation import ValidationReport, validate_bundle

log = get_logger("loader")


class PackLoader:
    def __init__(self, store: TableStore, env: str, actor: str = "system",
                 deployed_schema_version: Callable[[], int] | None = None,
                 validation_options: dict[str, Any] | None = None) -> None:  # fmt: skip
        self.store = store
        self.env = env
        self.actor = actor
        self.releases = ReleaseManager(store, env, actor, deployed_schema_version)
        self.validation_options = validation_options or {}

    def stage(
        self,
        bundle: PackBundle,
        *,
        git_sha: str | None = None,
        git_tag: str | None = None,
        approved_by: str | None = None,
        ci_run_url: str | None = None,
        ticket: str | None = None,
    ) -> dict[str, Any]:
        """Validate and stage a bundle as a new (inactive) release. Returns the release row. Re-running is safe."""
        if bundle.env != self.env:
            raise PackError(f"bundle is for env {bundle.env}, loader targets {self.env}")
        if bundle.compute_hash() != bundle.bundle_sha256:
            raise PackError("bundle hash mismatch: refusing to load a modified bundle")
        report: ValidationReport = validate_bundle(bundle, self.env, deployed_schema_version=self.releases._deployed(),
                                                   options=self.validation_options)  # fmt: skip
        if not report.ok:
            raise ValidationFailedError(f"bundle failed validation ({len(report.errors)} errors); nothing was loaded",
                                        codes=sorted({f.code for f in report.errors}))  # fmt: skip

        deployed = self.releases._deployed()
        if deployed < versioning.METADATA_SCHEMA_VERSION:
            raise CompatibilityError(
                f"metadata schema is v{deployed} but this framework writes v{versioning.METADATA_SCHEMA_VERSION}: "
                "run `de-accel migrate` first"
            )
        rid = release_id_for(self.env, bundle.bundle_sha256)
        existing = self.store.select(RELEASES, {"release_id": rid})
        if existing and existing[0]["status"] in (STAGED, ACTIVE, "SUPERSEDED", "ROLLED_BACK"):
            log.info("release %s already loaded (%s): idempotent no-op", rid, existing[0]["status"])
            return existing[0]
        m = bundle.manifest
        row = {"release_id": rid, "env_id": self.env, "client_code": m["client_code"], "pack_name": m["pack_name"],
               "pack_version": m["pack_version"], "bundle_sha256": bundle.bundle_sha256, "git_sha": git_sha,
               "git_tag": git_tag, "status": STAGING, "requires_core": m["requires_core"],
               "metadata_schema_version": m["metadata_schema_version"], "core_version_at_load": versioning.framework_version(),
               "entity_counts": _counts_json(bundle), "approved_by": approved_by, "ci_run_url": ci_run_url, "ticket": ticket,
               "loaded_by": self.actor, "staged_ts": now_utc(), "activated_ts": None}  # fmt: skip
        self.store.insert_if_absent(RELEASES, [row], ["release_id"])
        if existing and existing[0]["status"] == FAILED:  # retry of a failed staging
            self.store.update_where(RELEASES, {"status": STAGING}, {"release_id": rid})
        self.releases.record_event(rid, "STAGING_STARTED", bundle_sha256=bundle.bundle_sha256)
        try:
            self._write_rows(bundle, rid)
            self._verify(bundle, rid)
        except Exception as exc:
            self.store.update_where(RELEASES, {"status": FAILED}, {"release_id": rid})
            self.releases.record_event(rid, "STAGING_FAILED", error=str(exc)[:500])
            raise
        self.store.update_where(RELEASES, {"status": STAGED}, {"release_id": rid})
        self.releases.record_event(rid, "STAGED", entity_counts=bundle.counts())
        return self.releases.get_release(rid)

    # ---- internals ---------------------------------------------------------------------------
    def _previous_versions(self, table: str, id_col: str) -> dict[str, tuple[str, int]]:
        prev = self.releases.active_release_id()
        if not prev:
            return {}
        return {r[id_col]: (r["row_hash"], r["row_version"] or 1) for r in self.store.select(table, {"pack_release_id": prev})}

    def _write_rows(self, bundle: PackBundle, rid: str) -> None:
        ts = now_utc()
        for e in ENTITIES:
            rows = bundle.rows(e.kind)
            if not rows:
                continue
            prev = self._previous_versions(e.table, e.id_column)
            out = []
            for r in rows:
                old = prev.get(r[e.id_column])
                version = 1 if old is None else (old[1] if old[0] == r["row_hash"] else old[1] + 1)
                out.append(to_table_row(e.kind, r, rid, version, ts))
            self.store.insert_if_absent(e.table, out, ["pack_release_id", e.id_column])

    def _verify(self, bundle: PackBundle, rid: str) -> None:
        for e in ENTITIES:
            want = {r[e.id_column]: r["row_hash"] for r in bundle.rows(e.kind)}
            got = {r[e.id_column]: r["row_hash"] for r in self.store.select(e.table, {"pack_release_id": rid})}
            if want != got:
                missing, extra = set(want) - set(got), set(got) - set(want)
                differing = sum(1 for k in want.keys() & got.keys() if want[k] != got[k])
                raise ReleaseError(
                    f"verification failed for {e.table}: {len(missing)} missing, {len(extra)} unexpected, "
                    f"{differing} differing rows",
                    release_id=rid,
                )


def _counts_json(bundle: PackBundle) -> str:
    import json

    return json.dumps(bundle.counts(), sort_keys=True)
