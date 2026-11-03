# Release Notes

## 0.1.0 (unreleased — M1 metadata engine; LOCAL_VERIFIED, DATABRICKS_INTEGRATION_PENDING)
Added: package `de_accelerator` (core, metadata, cli); 53-table metadata schema v1 with migrations; YAML pack format with load-time
environment overrides; 60-rule validator; release-versioned insert-only loading with activate/rollback/optimistic concurrency/crash healing
and audit history; immutable content-hashed run snapshots; three readers + typed models; read-only `MetadataService`; `de-accel` CLI;
Azure DevOps pipeline definition (not yet executed on Azure DevOps).
Not included: any connector, ingestion, medallion, DQ/recon engine, orchestration, semantic generation, documents, Terraform.
