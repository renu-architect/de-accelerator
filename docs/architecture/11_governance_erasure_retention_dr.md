# 11 — Security Governance: Erasure, Retention, Backup & Disaster Recovery (Rev 1)

> Design only. Not legal advice: the client's DPO/legal defines lawful bases, deadlines and exceptions; the accelerator provides mechanisms and evidence. No personal data is stored in metadata or audit tables (only keyed/hashed tokens).

## 1. Identity & access baseline (recap + additions)
Unity Catalog is the single governance plane (catalog/schema/table grants, external locations, tags, masks, row filters, system-table audit). Key Vault holds all secrets (soft-delete + purge protection; RBAC mode; private endpoint). **Managed Identity** (Databricks Access Connector) for storage access; service principals (OAuth M2M) for jobs/CI with `run_as`; separate principals: `loader` (metadata), `engine` (runtime), `erasure` (deletion jobs – only DELETE/VACUUM on governed tables, nothing else), `ci`. RBAC via Entra groups (admin, engineer, steward, privacy-officer, consumer). Least privilege verified by negative-access tests.

## 2. Erasure (right-to-delete) propagation
**Metadata required**
- `cfg_subject_key_map`: for each object/column, the *subject identifier role* (e.g., `customer_id`), its normalisation rule, and `derived_from` links. Validator requires every object tagged `personal_data` to declare mappings or an explicit exemption with reason.
- `cfg_retention_policy` (see §3). Lineage: `run_lineage` + mapping metadata define which derived objects can contain the subject.
**Tables (governance schema `gov`)**
`gov_erasure_request` (request_id, subject_scope, requested_ts, due_ts, status, requester ref, legal_hold flag), `gov_erasure_task` (per object/layer/derived dataset: strategy, status, attempt), `gov_erasure_evidence` (rows affected, Delta version before/after, vacuum completion ts, verifier result — **counts and versions only**), `gov_suppression_list` (HMAC token of subject key, key-id; HMAC key held in Key Vault; no raw identifiers).
**Execution flow**
1. Intake (CLI/API from client's privacy workflow) → request with subject token(s) (HMAC computed at intake; raw identifier never persisted).
2. Resolve scope from `cfg_subject_key_map` + lineage → task list covering: Bronze, Silver, Gold, quarantine tables, rejected-record stores, document raw/extracted/page tables, embeddings/vector index entries, semantic extracts/metric-view backing tables, debug/sample tables.
3. Execute per task by strategy: `DELETE` (rows), `ANONYMIZE` (column overwrite where aggregates must stay), `REDACT` (docs), `REINDEX` (vector). SCD2 history rows are deleted/anonymised across all versions. Deletions run through Delta `DELETE` with deletion-vector awareness; **physical removal** requires `REORG … APPLY (PURGE)` / `VACUUM` honoring a *deletion-specific retention override* (documented interplay with time travel/DR).
4. Add token to `gov_suppression_list`; **ingestion/reprocess consults the list** (Bronze→Silver gate, backfills, restores) so deleted subjects are not re-materialised.
5. Verify: post-condition query per task (no matches for token on mapped columns); recorded as evidence.
6. Close request with an audit report; due-date monitoring & breach alerts.
**Gaps handled explicitly:** Power BI Import datasets/exports/external copies are outside lineage → `cfg_external_consumer` registry with notification task + refresh trigger; backups/clones hold data until expiry → documented max-lag (see §4) and **post-restore replay of the suppression list**; logs: audit tables never store PII (validated by DQ checks on log content patterns).
**Failure handling:** task-level retry; partial completion visible and blocks "closed"; legal hold pauses; idempotent (re-run safe). **Tests:** seeded synthetic subject across all layers incl. SCD2 and derived Gold → assert absent everywhere, assert suppression on reload/backfill, assert restore + replay, assert audit has no raw identifiers.

## 3. Retention
`cfg_retention_policy` (scope: object/layer/log-table/document class): `retain_days`, `basis` (legal/business), `action` (DELETE/ARCHIVE/ANONYMIZE), `legal_hold_exempt`, `delta_log_retention`, `deleted_file_retention`, `quarantine_retention`, `snapshot_retention`. A retention job (`gov_retention_run` evidence) applies policies on a schedule with dry-run, row counts and approvals for destructive new policies. Defaults proposed: quarantine 90 d; run/audit logs 13 months; config snapshots ≥ audit retention; Bronze per client policy (no default — must be set; validator warns if missing for personal-data objects). Retention changes are Git-controlled like any config.

## 4. Backup, Delta recovery, infrastructure recreation
| Tier | Assets | Mechanism | Proposed RPO | Proposed RTO |
|---|---|---|---|---|
| T0 Config & code | Git repos (core, client), Terraform code, packs | Git hosting redundancy + periodic mirror to separate region/account | ~0 (every merge) | 1–2 h to re-point |
| T1 Metadata (Delta `cfg_*`) | cfg tables, releases | **Rebuildable from packs** (re-run loader for each release) + scheduled deep clone / storage replication as accelerator | 0 (rebuild) / ≤ 24 h (clone) | ≤ 1 h |
| T2 Runtime state | `run_*`, `wm_*`, audit, gov evidence | Storage replication (GZRS/RA-GZRS or cross-region copy) + daily deep clone | ≤ 24 h (design target) | ≤ 4 h |
| T3 Data layers | Bronze/Silver/Gold | Delta time-travel/`RESTORE` within retention (operational errors); deep clone or storage replication for DR; Silver/Gold can be **rebuilt from Bronze**; Bronze re-extractable only if source retains history | per client tier (e.g., Bronze ≤ 24 h) | per client tier |
| T4 Documents raw | originals | ZRS/GZRS + immutability policy/soft delete | ≤ 24 h | ≤ 8 h |
| T5 Secrets | Key Vault | soft-delete + purge protection + vault backup/geo-pair; secrets re-creatable by client runbook | ≤ 24 h | 1–2 h |
| T6 Infra | workspace, UC, networking | **Terraform recreate** from remote (replicated) state; UC metastore/catalog recreate + external-location re-grants | n/a | 4–8 h (design target) |
**RPO/RTO are proposed design targets to agree per client; they are not commitments until DR tests demonstrate them.**
**Delta recovery:** `RESTORE TABLE … TO VERSION/TIMESTAMP` within retention; pre-migration table clones for destructive metadata migrations; log/deleted-file retention set by policy and reconciled with erasure (a deletion may require purging history earlier than default).
**Post-restore consistency protocol:** after any restore/failover run `cli verify-consistency`: (1) compare each `wm_watermark/wm_layer_cursor` with the target's max `_batch_seq`/receipts; watermark **ahead** of data ⇒ audited `rewind-watermark` to last verified receipt; behind ⇒ safe replay (idempotent); (2) reapply suppression list; (3) rerun reconciliation for last N batches.
**Infra recreation:** documented, scripted order: Terraform (network → storage → KV → workspace → UC → identities → grants) → bundle deploy → `migrate` → load active pack releases → `verify-consistency` → smoke pipeline.

## 5. DR testing strategy
| Test | Frequency | Pass criteria |
|---|---|---|
| Metadata rebuild from Git into empty catalog (automated in CI per release) | every release | state equals source of truth (hash compare) |
| Delta `RESTORE` drill on sample tables | quarterly (non-prod monthly automated) | recovered content correct; watermark protocol correct |
| Infra recreation in scratch subscription via Terraform | semi-annual | workspace usable, grants correct, within RTO target |
| Regional failover / restore from replica (tabletop + technical) | annual (PROD-like) | measured RPO/RTO recorded vs targets |
| Erasure-after-restore replay | with each drill | no deleted subject present |
| Secret/KV recovery drill | annual | pipelines run after recovery |
Results go to `TEST_EVIDENCE.md`; measured RPO/RTO replace the proposed values only after demonstrated.

## 6. Audit trails
Erasure/retention evidence (gov tables), config release/activation history, run/event logs, UC audit (system tables), Key Vault diagnostic logs, Terraform plan/apply logs — all retained per policy; tamper-resistance via append-only tables and restricted principals; audit log exports to Log Analytics optional.
