# Known Limitations (design stage)
- Nothing implemented or tested; all capabilities are proposals.
- Streaming not covered (out of scope). Env overrides, DR/backup, GDPR erasure and UI-readiness are now designed in Rev 1 (docs 09–11) but not implemented.
- Performance targets unvalidated; sizing assumptions (500–5,000 objects, ≤10 TB) unconfirmed.
- Delta lacks multi-table transactions; mitigated by ADR-0009 (supersedes ADR-0004 details) (to be proven by crash-injection tests).
- "Minimal customization across clients" is an unproven goal until pilot validation.

## Added in Rev 1
- Sizing (500–5,000 objects, ≤10 TB/client) are design targets only; unmeasured.
- Exactly-once is *effectively-once*; relies on Spike S1 (MERGE commit tagging) – unverified.
- RPO/RTO values are proposals until DR drills measure them.
- Power BI deployment/DAX tests (T3–T5) need Fabric/Power BI capacity and tenant enablement; not available in design sandbox.
- Databricks-side integration tests need a DEV workspace; sandbox has no Databricks and no PySpark/Delta installed (Python 3.13, Java present).
- Erasure scope covers lineage-known datasets; external copies (e.g. Power BI imports, exports) need the consumer registry + manual/notified action; legal interpretation is the client's.
- Metadata UI not in MVP (Git workflow only).

## M1 limitations (metadata engine)
- No Databricks/Unity Catalog verification: UC grants, PK constraints, secret scopes, Workflows untested (`DATABRICKS_INTEGRATION_PENDING`).
- Azure DevOps pipeline file written but never executed. Runtime matrix profile (Python 3.11 / Spark 3.5) not yet verified against a specific Databricks LTS.
- Component (Delta) tests are slow (~5 min total); concurrency is tested with in-memory atomics, not concurrent Delta writers.
- Scale benchmark (500/2,500/5,000 objects) not run; sizing remains an unmeasured design target.
- Snapshot ids include the release id (no cross-release dedupe); per-layer cursors, CDC retention behaviour and crash-injection of ingestion are M2 scope.
- ADR-0018 items marked ⚠ await your acknowledgement.
