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
