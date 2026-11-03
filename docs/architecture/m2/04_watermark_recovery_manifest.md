# M2 — Watermark, Batch Manifest, Receipts and Recovery Design

Specialises doc 08 (ADR-0009) to the source→Bronze path. Refinements to doc 08 are listed in §9 (none changes an approved ADR).

## 1. State owned by M2 (all in the `audit` schema, activated from M1 DDL_ONLY; additive columns per CR-M2-01)
| Table | Mutability | Purpose |
|---|---|---|
| `run_batch_manifest` | insert-once | immutable identity + frozen plan of a batch |
| `run_batch_event` | append-only | state log; current state = fold of events |
| `run_target_receipt` | insert-once per (batch, layer, table) | verified proof of the Bronze commit |
| `wm_watermark` | conditional update only | **source→Bronze cursor** per `(object, stream)` |
| `run_batch_lease` | conditional update | advisory single-runner lease |
| `run_file_tracking` | insert / status update | per-file idempotency (key `(object, path, mtime, size)`) |
| `run_schema_snapshot`, `run_lineage` | insert-once | schema history, table-level lineage |
| `run_pipeline`, `run_object`, `run_attempt`, `run_error` | insert / status update | run, per-object result, retry history, errors |
`wm_layer_cursor` stays unused until M3. Silver/Gold never touch the source cursor.

## 2. Batch manifest
Identity: `batch_id = uuid5(object_code | stream_key | batch_seq)`; `batch_seq = max(manifest seq for object+stream) + 1`; insert-if-absent on `(object_code, stream_key, batch_seq)`.
```json
{
  "batch_id": "…", "object_code": "orders", "stream_key": "live", "batch_seq": 42, "batch_type": "LIVE",
  "snapshot_id": "<sha256>", "release_id": "<uuid>", "planned_by_run_id": "<job run id>",
  "wm_from": "2026-11-03T01:58:00.000000Z", "wm_to": "2026-11-03T02:55:00.000000Z",
  "file_list": null,
  "expected_targets": [{"layer": "BRONZE", "table": "bronze.erp_orders", "write_mode": "APPEND"}],
  "plan_json": {"connector": "jdbc/sqlserver", "query_sha256": "…", "lookback": "PT2H",
                "partition": {"column": "OrderID", "lo": 1, "hi": 880231, "n": 8}, "late_arrival_window": "PT5M"},
  "manifest_hash": "sha256(canonical(identity + wm_from + wm_to + file_list + plan_json + expected_targets))",
  "planned_ts": "…"
}
```
File batch: `file_list = [{"path": "abfss://…/products_2026-11-03.csv", "size": 18231, "mtime": "…", "etag": "…"}, …]`, `wm_from/wm_to` = mtime range of the list. CDC: `wm_from/wm_to` = LSN/seq (hex/text) plus `plan_json.capture_instance`, `min_lsn_at_plan`. No secrets, no credentials, no row data.

## 3. State machine (derived from events)
```
PLANNED ─▶ EXTRACTING ─▶ [EXTRACTION_SEALED (REST only)] ─▶ BRONZE_WRITTEN ─▶ BRONZE_COMMITTED ─▶ SRC_WM_COMMITTED ─▶ DONE
   │            │                                   │               │                  │
   └────────────┴──────────────── any failure ─────▶ FAILED_RETRYABLE | FAILED_FATAL ──(recover)──▶ resumes at last durable step
                                                                     ABANDONED (operator only, audited; releases the window for re-planning)
```
| Event | Payload (secret-free) |
|---|---|
| `PLANNED` | manifest_hash, run_id, attempt |
| `LEASE_ACQUIRED` / `LEASE_LOST` | epoch |
| `CDC_GAP_DETECTED` | min_available, committed, policy |
| `EXTRACTING` | attempt |
| `EXTRACTION_SEALED` | landed file count, records, sha256 of page checksums, `wm_to_observed` |
| `BRONZE_WRITTEN` | txn_app_id, txn_version |
| `BRONZE_COMMITTED` | delta_version, rows_written, receipt checksum |
| `SRC_WM_COMMITTED` | committed_value, committed_seq |
| `DONE` | duration_s, rows |
| `FAILED_RETRYABLE` / `FAILED_FATAL` | error_class, code, message (redacted), attempt |
| `ABANDONED` | actor, reason |

## 4. Receipt and confirmed commit
Receipt (`run_target_receipt`): `batch_id, layer=BRONZE, target_table, delta_version, commit_tag, rows_written, rows_rejected, content_checksum, verified_ts` (+ `txn_app_id, txn_version, rows_read`). A receipt is inserted **only after** all of:
1. Delta history contains a commit whose `userMetadata` has this `batch_id` (or, if history is unavailable/expired, the stats-skipped count query below succeeds);
2. `SELECT count(*) FROM bronze WHERE _stream_key=S AND _batch_seq=N` equals the rows observed by the write action;
3. `numOutputRows` from the commit equals the same number.
Receipt insert is `insert_if_absent` ⇒ idempotent. **No receipt ⇒ the watermark cannot advance.**

## 5. Watermark commit
```
UPDATE wm_watermark SET committed_value=:wm_to, committed_seq=:n, committed_batch_id=:b, updated_ts=now()
WHERE object_code=:o AND stream_key=:s AND committed_seq = :previous_committed_seq      -- precondition
```
1 row → advanced. 0 rows → re-read: if `committed_seq >= n` and `committed_batch_id = :b` ⇒ already advanced (idempotent success), else ⇒ conflict error (alert, no change). First batch: `insert_if_absent` then verify the row (not trusting counts). Values are monotonic; decreases are rejected except via the audited `rewind-watermark` (post-restore/DR, doc 11).
**Value representation:** source-native string (ISO-8601 UTC with microseconds; integer text; `rowversion`/LSN hex). Comparison is done by the connector in the source's own type, never by string order in Python.

## 6. Recovery matrix (first action of every run for an object = `recover()`)
| Derived state at start | Evidence checked | Action |
|---|---|---|
| no open batch | – | plan next batch |
| `PLANNED` / `EXTRACTING`, no `BRONZE_WRITTEN` | – | re-extract the **same frozen plan** (REST: resume/skip using landed pages) |
| REST: pages landed (`_SUCCESS`) but no `EXTRACTION_SEALED` | verify page checksums | write the sealed event, continue |
| `BRONZE_WRITTEN` (or crashed during/after write), no receipt | history/count verification | commit exists → create receipt, continue; not found → re-run write with same `txnVersion` (Delta skips if it had in fact committed) |
| `BRONZE_COMMITTED`, watermark not advanced (**crash between target commit and watermark commit**) | receipt present | **advance watermark only — no source re-read** |
| `SRC_WM_COMMITTED`, no `DONE` | – | finalise (file tracking DONE, run_object, lineage, DONE) |
| `FAILED_RETRYABLE` | – | resume per rows above (retry policy decides when) |
| `FAILED_FATAL` | – | stop; operator fixes cause → `ingest --restart` resumes same batch, or `abandon-batch` |
| lease held by live runner | lease expiry/heartbeat | exit `BUSY` (not an error) |
| lease expired | – | take over (epoch+1); a stale writer's late commit is harmless (idempotent) and its watermark update fails the precondition |
| receipt missing but target rows exist for a *different* manifest hash | integrity check | halt + alert (data-integrity incident) |

## 7. Late-arriving data, backfill, reprocess, retention
- **Late arrivals (incremental/API):** `lookback_window` re-reads an overlap each batch (duplicates land in Bronze by design); `late_arrival_window` holds back the upper bound so in-flight commits are not skipped; SQL Server `rowversion` uses `MIN_ACTIVE_ROWVERSION()`. Files: late-arriving files are caught by tracking regardless of mtime. CDC: ordering by source sequence; nothing is "late".
- **Historical backfill:** `backfill_from/to` (M1 fields) → `batch_type = BACKFILL`, `stream_key = backfill-<n>`, own cursor and `txnAppId`; the live cursor is untouched; windows chunked by `max_window`.
- **Reprocess:** `de-accel reprocess --object O --from X --to Y --reason R` → `REPROCESS` stream with an explicit window; Bronze rows carry `_batch_type`/`_stream_key` so Silver (M3) can re-merge idempotently.
- **CDC retention safeguards (all from M1 fields):** at plan time compare `min_available` (`fn_cdc_get_min_lsn` / `MIN(seq)` in the change table) with the committed cursor. If `committed < min_available` **or** `now − committed_ts > max_replay_age` → `CDC_GAP_DETECTED` and apply `on_retention_gap`:
  `FAIL` → batch `FAILED_FATAL`, alert, no data loss hidden · `FULL_RESYNC` → new `RESYNC` stream: record `hi` **first**, then take a full snapshot (`_op_type='S'`), then resume CDC from `hi` (duplicates are idempotent in Silver); `wm_watermark.epoch` increments · `ALERT` → continue from `min_available`, raise an alert that data may be missing.
- **Source data changing under a replay:** a replay reads the same window but the source may have changed; effects are bounded by idempotent Silver merges and by M4 reconciliation. Stated limitation: *effectively-once*, not exactly-once.

## 8. Crash-injection test design (mandatory; Bronze scope)
`FaultInjector` hook (test-only, enabled by env flag; never configurable from metadata). A crash raises `InjectedCrash(BaseException)` so no cleanup/`FAILED` marking runs (emulates process death); a **fresh** engine instance then runs `recover()`.
Crash points: CP01 before manifest insert · CP02 after manifest, before lease · CP03 after lease, before extract · CP04 mid-extract (REST: mid-landing, after N pages) · CP05 after extract, before sealed event (REST) · CP06 before Bronze write · CP07 mid-write (Delta atomic: all-or-nothing) · CP08 after Delta commit, before receipt · CP09 after receipt, before `BRONZE_COMMITTED` event · CP10 after event, before watermark update · CP11 after watermark update, before `SRC_WM_COMMITTED` event · CP12 before file-tracking DONE · CP13 before `DONE` · CP14 crash **during recovery** · CP15 repeated crashes at the same point (N times).
Matrix: {FULL, SNAPSHOT, INCREMENTAL, CDC (incl. deletes), FILE_INCR, API} × CP01–CP15, each with ≥ 3 consecutive batches. Invariants asserted after recovery:
I1 cursor never ahead of the last receipted batch · I2 Bronze row multiset equals a crash-free run (no loss, no duplicates, per `_batch_id`) · I3 exactly one `DONE` per batch, monotonic `event_seq` · I4 re-running recovery/the run is a no-op · I5 **source is not re-read** when only the watermark commit was outstanding (read-counter on the mock connector) · I6 `run_object` counts reconcile with Bronze · I7 committed windows are contiguous (`wm_from(n+1) == wm_to(n)`; abandoned seqs excluded).
Extra scenarios: two concurrent runners (lease + idempotency), lease expiry takeover, source mutated between attempts, Delta concurrent-commit conflict retry, CDC gap policies (FAIL / FULL_RESYNC / ALERT), file re-delivered with same name & new content, API cursor loop/max_pages exceeded, hypothesis-based random crash sequences over multi-batch runs. A subset runs as real subprocess `SIGKILL` against local Delta.

## 9. Refinements of doc 08 (for the record)
1. `batch_seq = max(manifest seq) + 1` (an abandoned batch consumes its number); contiguity (I7) is defined over committed batches.
2. REST: window upper bound is **sealed after landing** (immutable event) instead of fixed before read; all other connectors fix it in the manifest before reading.
3. Receipt confirmation may use the stats-skipped count query when history is unavailable (retention/permissions).
4. Source cursor has an `epoch` to represent CDC full resyncs.
