# ADR-0020 — M3 transformation engine decisions

Status: **ACCEPTED** by the user on 2026-11-04 (supersedes the earlier proposed text of this ADR). Amends, for MERGE-type targets only, the verification mechanism of ADR-0009 / doc 08 (see decision 3).

## Context
M1 (metadata) and M2 (source→Bronze) are closed as `LOCAL_VERIFIED`. M3 adds Bronze→Silver→Gold. Full design: `docs/architecture/m3/00_M3_design.md`; register D1–D12: `02_M3_decisions_and_estimate.md`.

## Decisions
1. Silver processes units of ordered, receipted Bronze `live` batches with an independent cursor in `wm_layer_cursor`; Gold consumes pinned Silver Delta versions. Layers share no cursor, so a Silver failure never causes re-extraction.
2. M3 reuses the M2 batch protocol through its **public API only** (manifest, events, receipts, lease, run log) with stream keys `SILVER:live` / `GOLD:main`. **No M2 source file is changed.** A ≈40-line retry wrapper and ≈25 lines of runner logic are deliberately duplicated; consolidation into a neutral package is tracked as `M4-CONSOLIDATE-01`.
3. **Recovery for MERGE-type targets** is data-idempotent merge plus content-level verification (per-strategy rules in doc 00 §8); no commit tagging, no session isolation. Rationale: the 2026-11-04 spike showed a replayed MERGE leaves identical data, while the commit-tag conf is not thread-safe in a shared session. Append writes keep Delta `txnAppId/txnVersion`. The invariant "cursor advances only after a verified commit" is preserved.
4. Business rules are metadata: allowlisted cleansing operations, SCD1/SCD2/APPEND, basic DQ. Gold SQL/PySpark are reviewed, checksum-verified files under `transform_root`; `inline_sql` is rejected.
5. Schema v3 is additive (six fields); migrations 0001/0002 stay frozen; v2 packs remain valid.
6. Deletes come from Bronze `_op_type='D'`; delete-by-absence is deferred to M4 because extract completeness cannot be proven for the current connectors.
7. Rejects hold pointers `(src_batch_id, _row_hash)` only and are classified restricted pseudonymous data.
8. Batch only. Streaming is not implemented; strategies are pure functions of a staged DataFrame.

## Consequences
Positive: no M2 regression risk, small metadata surface, recovery proven on real Delta. Negative: temporary duplication; JDBC source deletes do not reach Silver; no authenticated approvals; `_row_hash` is not a privacy guarantee; Databricks/Unity Catalog behaviour unvalidated (`DATABRICKS_INTEGRATION_PENDING`).
