# ADR-0020 — M3 transformation engine decisions

Status: **PROPOSED** (awaiting user approval; supersedes nothing). Date: 2026-11-04.

## Context
M1 (metadata) and M2 (source→Bronze) are closed as `LOCAL_VERIFIED`. M3 adds Bronze→Silver→Gold. Full design: `docs/architecture/m3/00_M3_design.md`.

## Decisions
1. Silver processes units of receipted Bronze `live` batches with an independent cursor (`wm_layer_cursor`); Gold consumes Silver Delta version ranges. Layers share no cursor, so a Silver failure never causes re-extraction.
2. Reuse the M2 batch protocol (manifest, events, receipts, conditional cursor update, lease); layer encoded in `stream_key`. Shared retry/commit helpers are extracted from the ingestion engine (behaviour-preserving) rather than duplicated.
3. MERGE commits are tagged and looked up on recovery; data-level idempotency (key + ordering guard + hash) is the safety net. Each concurrent unit runs in its own `spark.newSession()` (spike S1: the tag is session-level and not thread-safe; MERGE replays are not de-duplicated by Delta).
4. Cleansing is declarative (`cleanse_rules.ops`); SQL/PySpark transforms are files checksum-verified against metadata; no inline code, no LLM at runtime.
5. SCD2: version-chain MERGE; late arrivals fail by default; restatement out of scope. Surrogate keys are deterministic hashes.
6. Additive schema v3 (migration 0003); 0001/0002 stay frozen.
7. Streaming is not implemented; `InputSlice` and `MergeStrategy` are the extension points.

## Consequences
Positive: no duplicated protocol, recoverable per unit, client-independent. Negative: touches M2 engine code (mitigated by 398-test net); Gold CDF has a documented limit; Databricks behaviour unverified.
