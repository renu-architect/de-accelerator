"""All metadata-schema table specs (schema version 1).

cfg_* tables are generated from entity definitions (insert-only, release-tagged). Runtime/governance tables that
belong to later modules are DDL_ONLY in M1 (schema defined, no logic). Doc runtime tables (doc_registry, ...) are
deferred to V1: the approved schema list has no `docs` schema, placement to be decided with the document module.
"""

from __future__ import annotations

from de_accelerator.metadata.schema.entities import ENTITIES, EntityDef
from de_accelerator.metadata.schema.tablespec import Col, Status, TableSpec

SCHEMA_VERSION = 1


def _cols(spec: str) -> tuple[Col, ...]:
    """'name:type! name2:type2' -> columns ('!' = NOT NULL)."""
    out = []
    for tok in spec.split():
        name, typ = tok.split(":")
        nn = typ.endswith("!")
        out.append(Col(name, typ.rstrip("!"), nullable=not nn))
    return tuple(out)


def _t(schema: str, name: str, cols: str, pk: str = "", status: Status = "ACTIVE", append_only: bool = False,
       comment: str = "") -> TableSpec:  # fmt: skip
    return TableSpec(schema, name, _cols(cols), tuple(pk.split(",")) if pk else (), comment, append_only, status)


def cfg_spec(e: EntityDef) -> TableSpec:
    cols = [Col(e.id_column, "str", False, "deterministic uuid5 of business key"), Col("client_code", "str", False)]
    if e.project_scoped:
        cols.append(Col("project_code", "str", False))
    for f in e.fields:
        cols.append(Col(f.name, f.type if f.type in ("int", "float", "bool", "list", "json") else "str"))
    cols += [Col("pack_release_id", "str", False), Col("row_hash", "str", False), Col("row_version", "int"),
             Col("created_ts", "ts")]  # fmt: skip
    return TableSpec("meta", e.table, tuple(cols), ("pack_release_id", e.id_column),
                     f"Config: {e.kind} (insert-only, release-versioned)", append_only=True)


def _cfg_tables() -> list[TableSpec]:
    return [cfg_spec(e) for e in ENTITIES]


_META_SYSTEM = [
    _t("meta", "cfg_framework_version",
       "framework_version:str! metadata_schema_version:int! min_compatible_schema:int status:str release_date:str wheel_ref:str registered_ts:ts",
       "framework_version", comment="Framework versions seen by this deployment"),
    _t("meta", "cfg_schema_migration",
       "version:int! name:str! checksum:str! applied_ts:ts applied_by:str framework_version:str status:str!", "version",
       comment="Applied metadata schema migrations"),
    _t("meta", "cfg_pack_release",
       "release_id:str! env_id:str! client_code:str! pack_name:str! pack_version:str! bundle_sha256:str! git_sha:str git_tag:str "
       "status:str! requires_core:str metadata_schema_version:int core_version_at_load:str entity_counts:json approved_by:str "
       "ci_run_url:str ticket:str loaded_by:str staged_ts:ts activated_ts:ts",
       "release_id", comment="Release registry (status is mutable; audit trail in cfg_release_event)"),
    _t("meta", "cfg_active_release",
       "env_id:str! release_id:str! previous_release_id:str activation_seq:int! activated_by:str activated_ts:ts", "env_id",
       comment="Active-release pointer per environment (conditional updates only)"),
    _t("meta", "cfg_release_event",
       "event_id:str! release_id:str! env_id:str! event_type:str! actor:str detail:json event_ts:ts!", "event_id",
       append_only=True, comment="Append-only release audit events"),
    _t("meta", "cfg_config_history",
       "history_id:str! env_id:str! table_name:str! entity_key:str! change_type:str! release_from:str release_to:str! "
       "activation_seq:int! before_json:json after_json:json actor:str ticket:str change_ts:ts!",
       "release_to,activation_seq,table_name,entity_key", append_only=True, comment="Row-level config change history derived from release diffs"),
    _t("meta", "run_config_snapshot",
       "snapshot_id:str! release_id:str! env_id:str! scope_json:json! content_sha256:str! content_gz:bin! size_bytes:int "
       "entity_counts:json framework_version:str created_by:str created_ts:ts!",
       "snapshot_id", append_only=True, comment="Immutable per-run configuration snapshots (content-hash id)"),
]

_D: Status = "DDL_ONLY"
_RUNTIME = [
    _t("audit", "run_pipeline", "run_id:str! execution_code:str env_id:str trigger_type:str framework_version:str release_id:str snapshot_id:str status:str start_ts:ts end_ts:ts parent_run_id:str", "run_id", _D),
    _t("audit", "run_object", "run_object_id:str! run_id:str! object_code:str! layer:str status:str attempt:int rows_read:int rows_written:int rows_rejected:int duration_s:float error_class:str", "run_object_id", _D),
    _t("audit", "run_batch_manifest", "batch_id:str! object_code:str! stream_key:str! batch_seq:int! batch_type:str! snapshot_id:str wm_from:str wm_to:str file_list:json expected_targets:json planned_by_run_id:str manifest_hash:str! planned_ts:ts", "object_code,stream_key,batch_seq", _D, append_only=True, comment="Immutable batch manifest (doc 08)"),
    _t("audit", "run_batch_event", "batch_id:str! event_seq:int! event_type:str! stage:str run_id:str attempt:int payload:json event_ts:ts", "batch_id,event_seq", _D, append_only=True),
    _t("audit", "run_target_receipt", "batch_id:str! layer:str! target_table:str! delta_version:int commit_tag:str rows_written:int rows_rejected:int content_checksum:str verified_ts:ts", "batch_id,layer,target_table", _D),
    _t("audit", "run_batch_lease", "object_code:str! stream_key:str! owner_run_id:str epoch:int! lease_expiry:ts", "object_code,stream_key", _D),
    _t("audit", "run_stage", "batch_id:str! stage:str! status:str delta_version:int start_ts:ts end_ts:ts", "batch_id,stage", _D),
    _t("audit", "run_attempt", "run_object_id:str! attempt_no:int! error:str start_ts:ts end_ts:ts retry_reason:str", "run_object_id,attempt_no", _D),
    _t("audit", "run_error", "error_id:str! run_id:str object_code:str error_class:str code:str message:str context:json error_ts:ts", "error_id", _D),
    _t("audit", "run_lineage", "edge_id:str! batch_id:str source_object:str target_object:str source_column:str target_column:str layer:str", "edge_id", _D),
    _t("audit", "run_schema_snapshot", "object_code:str! batch_id:str! schema_hash:str! schema_json:json change_type:str", "object_code,batch_id", _D),
    _t("audit", "run_file_tracking", "object_code:str! file_path:str! size:int mtime:ts file_hash:str batch_id:str status:str", "object_code,file_path", _D),
    _t("audit", "wm_watermark", "object_code:str! stream_key:str! committed_value:str committed_seq:int! committed_batch_id:str updated_ts:ts", "object_code,stream_key", _D, comment="Source watermark (advance only after confirmed Bronze commit)"),
    _t("audit", "wm_layer_cursor", "object_code:str! layer:str! applied_seq:int! applied_batch_id:str updated_ts:ts", "object_code,layer", _D, comment="Independent Silver/Gold cursors (ADR-0009)"),
    _t("dq", "dq_result", "dq_run_id:str! batch_id:str! rule_code:str! object_code:str status:str failed_count:int total_count:int failed_pct:float dq_score:float eval_ts:ts", "dq_run_id,rule_code", _D),
    _t("recon", "rc_result", "rc_run_id:str! batch_id:str! recon_code:str! object_code:str scope:str left_value:str right_value:str delta:str tolerance:str status:str eval_ts:ts", "rc_run_id,recon_code", _D),
    _t("recon", "rc_mismatch", "rc_run_id:str! recon_code:str! mismatch_key:str! left_payload:json right_payload:json", "rc_run_id,recon_code,mismatch_key", _D),
    _t("gov", "gov_erasure_request", "request_id:str! status:str! subject_scope:json requested_ts:ts due_ts:ts legal_hold:bool requester_ref:str closed_ts:ts", "request_id", _D),
    _t("gov", "gov_erasure_task", "task_id:str! request_id:str! object_code:str layer:str strategy:str status:str attempt:int", "task_id", _D),
    _t("gov", "gov_erasure_evidence", "evidence_id:str! task_id:str! rows_affected:int delta_version_before:int delta_version_after:int vacuum_completed_ts:ts verifier_result:str", "evidence_id", _D, append_only=True, comment="Counts/versions only; no personal data"),
    _t("gov", "gov_suppression_list", "token:str! key_id:str! added_request_id:str added_ts:ts", "token", _D, comment="HMAC tokens only"),
    _t("gov", "gov_retention_run", "run_id:str! policy_code:str! object_code:str action:str rows_affected:int dry_run:bool run_ts:ts", "run_id,policy_code", _D, append_only=True),
]


def all_tables() -> list[TableSpec]:
    return _cfg_tables() + _META_SYSTEM + _RUNTIME


def table_map() -> dict[str, TableSpec]:
    return {t.name: t for t in all_tables()}


def get_table(name: str) -> TableSpec:
    return table_map()[name]
