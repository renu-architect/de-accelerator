# 08 — Strengthened Batch, Watermark & Recovery Protocol (supersedes ADR-0004 details)

> Design only. Implemented in M2 (ingestion/batch/watermark) and M4 (orchestration restart). M1 delivers only the DDL for these tables.

## 1. Principles
1. **Plan durably, then act.** A batch's identity and window are persisted before any source read.
2. **Deterministic replay.** Same batch ⇒ same window, same target tags, same idempotency keys.
3. **Confirm, don't assume.** A commit counts only when a *receipt* is verified against the target Delta log.
4. **Advance last.** Watermarks/cursors advance only after confirmed commits, via conditional update.
5. **Correctness from idempotency, not locks.** Leases reduce duplicate work; they are not relied on for safety.

## 2. Durable artifacts
| Artifact | Table | Mutability | Content |
|---|---|---|---|
| Batch manifest | `run_batch_manifest` | insert-once (immutable) | `batch_id`, `object_id`, `stream_key`, `batch_seq`, `batch_type` (LIVE/BACKFILL/REPROCESS), `config_snapshot_id`, `wm_from`, `wm_to`, `file_list` (or manifest file path), `expected_targets[]` (layer, table, write_mode), `planned_by_run_id`, `manifest_hash`, `planned_ts` |
| Batch event log | `run_batch_event` | append-only | `batch_id`, `event_seq`, `event_type`, `stage`, `run_id`, `attempt`, `payload`, `ts` — state is **derived** from the log (current-state view) |
| Target receipt | `run_target_receipt` | insert-once per (batch, layer, table) | `delta_version`, `commit_tag`, `rows_written`, `rows_rejected`, `content_checksum`, `verified_ts` |
| Watermark | `wm_watermark` | conditional update | `committed_value`, `committed_seq`, `committed_batch_id` |
| Layer cursor | `wm_layer_cursor` | conditional update | `object_id`, `layer`, `applied_seq`, `applied_batch_id` |
| Lease | `run_batch_lease` | conditional update | owner run, epoch, expiry (heartbeat) |
Manifest also mirrored as JSON in ADLS `_manifests/<object>/<batch_seq>.json` (tamper/recovery copy; checksum in table).

`batch_seq = committed_seq + 1` per `(object_id, stream_key)`; manifest creation is **insert-if-absent on (object_id, stream_key, batch_seq)** so a crashed planner never creates a second, different window — restart finds and reuses the open manifest.

## 3. State machine (derived from events)
```
PLANNED → EXTRACTING → BRONZE_COMMITTED → [SILVER_COMMITTED] → [GOLD_COMMITTED]
          source watermark commit happens after BRONZE_COMMITTED  → SRC_WM_COMMITTED
                                        layer cursors advance after each layer commit
 … → DONE            (all enabled layers committed, all cursors advanced, DQ/recon gates finished)
Failure edges: any → FAILED_RETRYABLE | FAILED_FATAL ;  FAILED_* → (recover) → resumes ; ABANDONED (operator only, audited)
```
Stage-level states per `(batch, layer)`: `NOT_STARTED, RUNNING, COMMITTED, CURSOR_ADVANCED, FAILED`.
Gate semantics: DQ `FAIL_BATCH` / recon FAIL prevents the layer commit receipt from being accepted as "advance-eligible"; the write is not visible to downstream because layer processing for the next layer only consumes batches whose upstream layer is `CURSOR_ADVANCED`.

## 4. Idempotent target writes
| Target pattern | Idempotency mechanism | Confirmation |
|---|---|---|
| Bronze append | `txnAppId=<object>:<layer>`, `txnVersion=batch_seq` (Delta idempotent write — replay of a committed version is skipped) + `_batch_id` column | receipt via table history `userMetadata`; fallback `count(_batch_id=…)` |
| Silver/Gold MERGE (SCD1/2, CDC) | deterministic MERGE keys + ordering guard (`_op_seq`/modified); re-apply yields same state | commit `userMetadata={object,layer,batch_seq,manifest_hash}` found in history |
| Overwrite partition/replaceWhere | `replaceWhere _batch_id = X` / partition predicate – idempotent | same |
| Files copied to raw (doc/file landing) | path derived from content hash; copy-if-absent | checksum |
**Spike S1 (M2, before implementation):** verify that MERGE commits can be reliably tagged/located via `userMetadata` in `DESCRIBE HISTORY` on target DBR, and the history-retention horizon; if not, adopt a `run_target_receipt`-first (write-intent) pattern with `_batch_id` verification. Result recorded as ADR addendum.

## 5. Watermark / cursor commit
```
UPDATE wm_watermark SET committed_value=:wm_to, committed_seq=:n, committed_batch_id=:b
WHERE object_id=:o AND stream_key=:s AND committed_seq = :n-1
```
- 1 row updated → advanced. 0 rows → read row: if `committed_seq >= n` and `committed_batch_id = :b` ⇒ already advanced (idempotent success); else ⇒ conflict error (alert, no change).
- Precondition before issuing: all `expected_targets` for the owning layer have **verified receipts** (`verified_ts` not null).
- Watermark value is never read from "max seen"; it is `manifest.wm_to` (fixed at plan time).
- Monotonic: attempts to decrease are rejected; rewind only via audited `rewind-watermark` (post-restore, see doc 11).

## 6. Recovery matrix (what restart does per observed state)
| Observed (derived) state | Evidence check | Action |
|---|---|---|
| No manifest | – | plan a new batch |
| PLANNED / EXTRACTING, no receipt | search target history for tag | tag found ⇒ adopt (write receipt); else re-extract same window |
| BRONZE write committed, no receipt | tag/`_batch_id` check | write receipt; continue |
| BRONZE_COMMITTED, wm not advanced (**crash between target commit and watermark commit**) | verify receipt | advance watermark only; **no re-read** |
| wm advanced, no DONE event | – | continue downstream layers / finalize |
| SILVER RUNNING crash | tag check on silver table | adopt or re-merge (idempotent) |
| Layer committed, cursor not advanced | verify receipt | advance cursor |
| Duplicate runner / expired lease | lease epoch | new owner proceeds; stale owner's later writes are idempotent; its watermark update fails the conditional check |
| FAILED_FATAL | – | operator decision: fix + `restart`, or `abandon` |
| Manifest hash mismatch / missing receipt target version | integrity check | halt, alert (data-integrity incident) |

## 7. Crash-injection and restart test specification (mandatory; M2 harness, extended M4)
**Mechanism:** `FaultInjector` hook (no-op in production, test-only activation via env flag + signed test config; cannot be enabled by metadata) with named crash points. A crash raises `InjectedCrash` that bypasses all `finally`/rollback/log-flush handlers to emulate process death; a fresh engine instance then runs `recover()`. A subset also runs as real subprocess `SIGKILL` / cluster-terminate on Databricks integration tier.

**Crash points (per layer where applicable):**
CP01 before manifest insert · CP02 after manifest, before lease · CP03 after lease, before read · CP04 mid-read · CP05 after read, before write · CP06 mid-write (Delta atomicity: expect all-or-nothing) · CP07 after target commit, before receipt · CP08 after receipt, before state event · CP09 after state event, before watermark/cursor update · CP10 after watermark update, before event · CP11 after cursor, before next layer · CP12 during DQ/recon logging · CP13 during quarantine write · CP14 during recovery itself (double fault) · CP15 retry-storm: crash at each point N times consecutively.

**Matrix:** every {load type: FULL, INCREMENTAL, CDC, SNAPSHOT, FILE_INCR, API} × {layer: BRONZE, SILVER(SCD1,SCD2), GOLD} × {crash point}. Each combination runs the scenario with ≥2 batches so batch N+1 correctness is verified.

**Invariants asserted after recovery (must all hold):**
- I1 No watermark/cursor is ahead of the last batch with verified receipts.
- I2 Target table content equals the content of a crash-free run (row-level, incl. SCD2 history) — no loss, no duplicates.
- I3 Exactly one terminal `DONE` per batch; event log is consistent (monotonic `event_seq`).
- I4 Re-running recovery or the whole run again is a no-op (idempotent).
- I5 Source is not re-read when only the watermark commit was outstanding (asserted via read-counter).
- I6 Audit rows (`rows_read/written`) reconcile with targets.
- I7 Windows are contiguous: `wm_from(n+1) == wm_to(n)`.
Additional: concurrent-runner test (two engines, same object), lease expiry, late data inside lookback, source mutation between attempts, Delta commit-conflict retries, clock skew, randomized (property-based) multi-batch crash sequences. A module is not `COMPLETE` without the full matrix green; results recorded in `TEST_EVIDENCE.md`.

## 8. Limits (honest)
Exactly-once is **effectively-once** (at-least-once read + idempotent write). A source that mutates already-served windows is handled by lookback + merge, and detected by reconciliation, not prevented. History-retention shorter than restart horizon degrades confirmation to `_batch_id` verification.
