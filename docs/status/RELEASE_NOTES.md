# Release Notes

## 0.1.0 (unreleased — M1 metadata engine; LOCAL_VERIFIED, DATABRICKS_INTEGRATION_PENDING)
Added: package `de_accelerator` (core, metadata, cli); 53-table metadata schema v1 with migrations; YAML pack format with load-time
environment overrides; 60-rule validator; release-versioned insert-only loading with activate/rollback/optimistic concurrency/crash healing
and audit history; immutable content-hashed run snapshots; three readers + typed models; read-only `MetadataService`; `de-accel` CLI;
Azure DevOps pipeline definition (not yet executed on Azure DevOps).
Not included: any connector, ingestion, medallion, DQ/recon engine, orchestration, semantic generation, documents, Terraform.

## M2 (CLOSED 2026-11-04 as LOCAL_VERIFIED — source-to-Bronze ingestion foundation; SQL Server and Databricks validation PENDING; not production-ready)
Added: connector interface/registry, mock connector, JDBC connector (SQL Server, Azure SQL, PostgreSQL; FULL/SNAPSHOT/INCREMENTAL; SECRET auth), batch protocol (manifest, events, receipts, conditional watermark, lease, run log), Bronze writer (audit columns, idempotent transactional append, commit and row-count verification, schema evolution), ingestion engine + parallel runner with retry/recovery, CLI `ingest`/`batch-status`/`abandon-batch` (+ experimental `reprocess`), schema v2 migration, 65 validation rules. Store fixes: tz-aware UTC timestamps, bounded retry of Delta commit conflicts.
Not included: discovery, file connectors, REST, CDC/deletes, service-principal/managed-identity auth, Databricks job completion, real-environment validation.

## M3 (Bronze -> Silver -> Gold transformation) — LOCAL_VERIFIED on branch m3-design (611 tests, 95 % coverage; Databricks/Unity Catalog, real SQL Server and Azure DevOps validation PENDING; not production-ready)
Added: metadata schema v3 (migration 0003, additive), 16 transformation validation rules (MDV-7001..7016, schema >= 3 packs only), `LayerCursorRepo`, Silver engine (ordered receipted Bronze units, closed cleansing allowlist, casting with REJECT/FAIL, pointer-only reject tables, basic DQ, dedupe, APPEND, SCD1, SCD2 with logical-change identity, soft/hard/ignore deletes from Bronze `_op_type`), Gold engine (checksum-verified SQL/PySpark files, typed bound parameters, FULL and KEY_MERGE, pinned input versions, no-change skip), content-level verification before every receipt, dependency-aware runner, CLI `transform`, `transform-status`, `transform-replan`.
Not included (M4+): delete-by-absence, operator approval workflow and data-skip, HMAC reject references, Gold CDF, advanced schema evolution, built-in transforms, `expr`/regex/derived columns, automated rebuild, DQ scoring and reconciliation engine, planner/DAG, erasure automation, streaming. No M2 source file was modified.
