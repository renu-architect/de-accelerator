"""Entity definitions: the single source of truth for pack JSON-Schemas, cfg_* DDL and typed models.

Reference fields (`ref=<kind>`) hold the *code* of another entity in the same project and drive
referential validation. Business keys decide deterministic row ids (uuid5). Env overrides are resolved
by the loader; nothing here is environment-specific.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

LAYERS = ("BRONZE", "SILVER", "GOLD")
SENSITIVITY = ("public", "internal", "confidential", "restricted")
SOURCE_TYPES = (
    "sqlserver", "azuresql", "oracle", "mysql", "postgresql",
    "json", "xml", "parquet", "avro", "csv", "rest",
    "pdf", "word", "excel", "image", "text", "mock",
)  # fmt: skip
AUTH_MODES = ("MANAGED_IDENTITY", "SERVICE_PRINCIPAL", "SECRET", "NONE")
LOAD_TYPES = ("FULL", "INCREMENTAL", "CDC", "SNAPSHOT", "FILE_INCR", "API")
MERGE_TYPES = ("APPEND", "MERGE", "SCD1", "SCD2", "OVERWRITE")
DQ_RULE_TYPES = (
    "NOT_NULL", "DUPLICATE", "UNIQUE", "VALID_VALUES", "REF_INTEGRITY", "DATA_TYPE", "FORMAT", "RANGE",
    "BUSINESS_RULE", "FRESHNESS", "COMPLETENESS", "CUSTOM_SQL", "CUSTOM_PYSPARK",
)  # fmt: skip
SEVERITIES = ("INFO", "WARN", "ERROR", "CRITICAL")
DQ_ACTIONS = ("LOG", "QUARANTINE", "FAIL_BATCH")
RECON_SCOPES = ("SRC_BRZ", "BRZ_SLV", "SLV_GLD")
RECON_CHECKS = ("COUNT", "SUM", "DISTINCT_COUNT", "NULL_COUNT", "HASH", "PK", "FINANCIAL")


@dataclass(frozen=True)
class Field:
    name: str
    type: str  # str | int | float | bool | list | json
    required: bool = False
    enum: tuple[str, ...] | None = None
    ref: str | None = None
    default: Any = None
    description: str = ""


@dataclass(frozen=True)
class EntityDef:
    kind: str
    table: str
    key: tuple[str, ...]
    fields: tuple[Field, ...]
    project_scoped: bool = True
    origin: str = "items"  # items | manifest (client) | project_file (project)

    def field(self, name: str) -> Field:
        for f in self.fields:
            if f.name == name:
                return f
        raise KeyError(name)

    @property
    def id_column(self) -> str:
        return f"{self.kind}_id"


def _s(name: str, req: bool = False, enum: tuple[str, ...] | None = None, ref: str | None = None,
       default: Any = None, desc: str = "") -> Field:  # fmt: skip
    return Field(name, "str", req, enum, ref, default, desc)


def _i(name: str, req: bool = False, default: Any = None) -> Field:
    return Field(name, "int", req, default=default)


def _f(name: str, req: bool = False, default: Any = None) -> Field:
    return Field(name, "float", req, default=default)


def _b(name: str, default: bool = False) -> Field:
    return Field(name, "bool", False, default=default)


def _l(name: str, req: bool = False, ref: str | None = None, enum: tuple[str, ...] | None = None) -> Field:
    return Field(name, "list", req, enum, ref, default=[])


def _j(name: str, req: bool = False) -> Field:
    return Field(name, "json", req, default={})


_CODE = ("code",)
_D = "description"

ENTITIES: tuple[EntityDef, ...] = (
    EntityDef("client", "cfg_client", _CODE, (
        _s("code", True), _s("name", True), _s("region"), _s("data_residency"), _s("tenant_ref"),
    ), project_scoped=False, origin="manifest"),
    EntityDef("project", "cfg_project", _CODE, (
        _s("code", True), _s("name", True), _s("domain"), _s("owner"), _s("sla_class"), _s("cost_center"),
    ), origin="project_file"),
    EntityDef("environment", "cfg_environment", _CODE, (
        _s("code", True, enum=("DEV", "QA", "PROD")), _s("catalog_name"), _s("storage_root"),
        _s("secret_scope"), _s("cluster_policy_id"), _s("framework_version_range"),
        _i("default_concurrency", default=4), _s("topology", enum=("ISOLATED", "NONPROD_SHARED", "SINGLE_WORKSPACE")),
    )),  # fmt: skip
    EntityDef("source", "cfg_source", _CODE, (
        _s("code", True), _s("source_type", True, enum=SOURCE_TYPES), _s("connector_type"),
        _s("auth_mode", enum=AUTH_MODES, default="SECRET"), _j("connection"), _j("options"),
        _s("network_zone"), _s(_D),
    )),
    EntityDef("object", "cfg_object", _CODE, (
        _s("code", True), _s("source", True, ref="source"), _s("schema_name"), _s("object_name", True),
        _s("object_kind", enum=("table", "file", "api", "document", "derived"), default="table"),
        _l("business_keys"), _l("primary_keys"), _l("partition_cols"),
        _s("sensitivity", enum=SENSITIVITY, default="internal"), _i("priority", default=100),
        _l("enabled_layers", enum=LAYERS), _s("delete_mode", enum=("soft", "hard", "ignore"), default="soft"),
        _b("personal_data"), _j("tags"), _s("target_bronze"), _s("target_silver"), _s("target_gold"), _s(_D),
    )),  # fmt: skip
    EntityDef("column", "cfg_column", ("object", "name"), (
        _s("object", True, ref="object"), _s("name", True), _i("ordinal"), _s("source_type"), _s("target_type"),
        _b("is_pk"), _b("is_bk"), _b("is_nullable", True), _s("classification", enum=SENSITIVITY),
        _b("is_pii"), _s("mask_policy"), _s("default_value"), _j("cleanse_rules"),
        _s("fk_object", ref="object"), _s("fk_column"), _s(_D),
    )),  # fmt: skip
    EntityDef("load_strategy", "cfg_load_strategy", ("object",), (
        _s("object", True, ref="object"), _s("load_type", True, enum=LOAD_TYPES),
        _s("watermark_column"), _s("watermark_type"), _s("lookback_window"),
        _s("cdc_op_column"), _s("cdc_seq_column"), _s("late_arrival_window"),
        _s("backfill_from"), _s("backfill_to"),
        _s("schema_evolution_mode", enum=("FAIL", "ADD_COLUMNS", "ALLOW_WIDENING", "RESCUE"), default="ADD_COLUMNS"),
        _s("write_mode"), _j("page_config"), _b("is_complete_extract"), _s("partition_column"), _i("num_partitions"),
        # CDC retention / recovery safeguards (behaviour implemented in M2; validated here)
        _b("cdc_retention_check", True), _s("max_replay_age"),
        _s("on_retention_gap", enum=("FAIL", "FULL_RESYNC", "ALERT"), default="FAIL"),
    )),  # fmt: skip
    EntityDef("layer_mapping", "cfg_layer_mapping", ("object", "layer"), (
        _s("object", True, ref="object"), _s("layer", True, enum=LAYERS), _s("target_table"),
        _s("merge_type", enum=MERGE_TYPES, default="APPEND"), _l("scd_columns"), _l("dedupe_keys"),
        _s("dedupe_order"), _l("cluster_cols"), _j("maintenance"),
    )),
    EntityDef("transformation", "cfg_transformation", ("object", "layer", "step_no"), (
        _s("object", True, ref="object"), _s("layer", True, enum=LAYERS), _i("step_no", True),
        _s("type", True, enum=("SQL", "PYSPARK", "BUILTIN")), _s("body_ref"), _s("inline_sql"),
        _l("input_objects", ref="object"), _j("params"), _s("checksum"),
    )),
    EntityDef("dq_rule", "cfg_dq_rule", ("object", "code"), (
        _s("object", True, ref="object"), _s("code", True), _s("rule_type", True, enum=DQ_RULE_TYPES),
        _l("columns"), _j("params"), _s("severity", enum=SEVERITIES, default="ERROR"),
        _f("threshold_pct", default=0.0), _s("action", enum=DQ_ACTIONS, default="LOG"),
        _s("layer", enum=LAYERS, default="SILVER"), _s("expression"), _s("dimension"),
    )),
    EntityDef("recon_rule", "cfg_recon_rule", ("object", "code"), (
        _s("object", True, ref="object"), _s("code", True), _s("scope", True, enum=RECON_SCOPES),
        _s("check_type", True, enum=RECON_CHECKS), _s("left_ref"), _s("right_ref"), _l("columns"),
        _f("tolerance_abs", default=0.0), _f("tolerance_pct", default=0.0), _b("is_financial"),
        _b("incremental", True), _s("on_fail", enum=("WARN", "FAIL_BATCH"), default="WARN"),
    )),
    EntityDef("dependency", "cfg_pipeline_dependency", ("object", "depends_on"), (
        _s("object", True, ref="object"), _s("depends_on", True, ref="object"),
        _s("dependency_type", enum=("HARD", "SOFT"), default="HARD"), _s("stage_scope"), _s("condition"),
    )),
    EntityDef("retry_policy", "cfg_retry_policy", _CODE, (
        _s("code", True), _i("max_attempts", default=3), _i("backoff_sec", default=30), _f("backoff_mult", default=2.0),
        _l("retryable_error_classes"),
    )),  # fmt: skip
    EntityDef("execution", "cfg_execution", _CODE, (
        _s("code", True), _j("scope"), _s("schedule_cron"), _s("schedule_timezone"), _i("concurrency", default=4),
        _s("retry_policy", ref="retry_policy"), _i("timeout_min"),
        _s("failure_mode", enum=("ISOLATE", "FAIL_FAST"), default="ISOLATE"), _s("cluster_profile"),
        _s("provider", ref="orchestration_provider"),
    )),
    EntityDef("orchestration_provider", "cfg_orchestration_provider", _CODE, (
        _s("code", True), _s("provider_type", True), _j("options"),
    )),
    EntityDef("semantic_entity", "cfg_semantic_entity", _CODE, (
        _s("code", True), _s("object", True, ref="object"), _s("entity_type", True, enum=("FACT", "DIMENSION")),
        _l("grain"), _s("display_name"), _s(_D),
    )),
    EntityDef("semantic_measure", "cfg_semantic_measure", ("entity", "code"), (
        _s("entity", True, ref="semantic_entity"), _s("code", True), _s("expression", True), _s("agg_rule"),
        _s("format"), _b("is_kpi"), _f("kpi_target"), _s(_D),
    )),
    EntityDef("semantic_relationship", "cfg_semantic_relationship", _CODE, (
        _s("code", True), _s("from_entity", True, ref="semantic_entity"), _s("from_column", True),
        _s("to_entity", True, ref="semantic_entity"), _s("to_column", True),
        _s("cardinality", enum=("ONE_TO_ONE", "ONE_TO_MANY", "MANY_TO_ONE", "MANY_TO_MANY"), default="MANY_TO_ONE"),
        _s("cross_filter", enum=("SINGLE", "BOTH"), default="SINGLE"),
    )),
    EntityDef("semantic_glossary", "cfg_semantic_glossary", _CODE, (
        _s("code", True), _s("term", True), _s("definition", True), _s("owner"), _l("linked_columns"),
    )),
    EntityDef("semantic_target", "cfg_semantic_target", _CODE, (
        _s("code", True), _s("target_type", True, enum=("SQL_VIEW", "METRIC_VIEW", "TMDL")),
        _l("entities", ref="semantic_entity"), _j("options"),
    )),
    EntityDef("retention_policy", "cfg_retention_policy", _CODE, (
        _s("code", True), _s("scope_kind", True, enum=("OBJECT", "LAYER", "LOG", "DOCUMENT_CLASS")),
        _s("object", ref="object"), _s("layer", enum=LAYERS), _i("retain_days", True), _s("basis"),
        _s("action", enum=("DELETE", "ARCHIVE", "ANONYMIZE"), default="DELETE"), _b("legal_hold_exempt"),
        _i("delta_log_retention_days"), _i("deleted_file_retention_days"),
    )),
    EntityDef("subject_key", "cfg_subject_key_map", ("object", "column_name"), (
        _s("object", True, ref="object"), _s("column_name", True), _s("subject_role"), _s("normalization"),
        _b("exempt"), _s("exempt_reason"),
    )),
    EntityDef("external_consumer", "cfg_external_consumer", _CODE, (
        _s("code", True), _s("name", True), _s("kind"), _s("contact_ref"), _b("notify_on_erasure", True),
    )),
    EntityDef("doc_pipeline", "cfg_doc_pipeline", _CODE, (
        _s("code", True), _s("source", True, ref="source"), _s("ocr_provider", default="azure_document_intelligence"),
        _s("classifier"), _j("extract_options"),
    )),
)  # fmt: skip

BY_KIND: dict[str, EntityDef] = {e.kind: e for e in ENTITIES}
BY_TABLE: dict[str, EntityDef] = {e.table: e for e in ENTITIES}
ITEM_KINDS: tuple[str, ...] = tuple(e.kind for e in ENTITIES if e.origin == "items")
