# Databricks notebook source
# MAGIC %md
# MAGIC # INSIGHTS_02_LOAD_TAXONOMIES_TO_DIM_TABLES
# MAGIC
# MAGIC **Purpose**
# MAGIC - Load controlled taxonomy YAML files into Delta dimension tables for Insights.
# MAGIC
# MAGIC **Inputs**
# MAGIC - Parameters: `catalog`, `schema`, `run_id`, `run_mode`, `max_files_per_run`, `taxonomy_version`, `taxonomies_root`, `taxonomy_load_mode`, `enforce_unique_labels`, `require_examples`, `enable_dim_tables`
# MAGIC - Files: `taxonomies/contact_drivers.yml`, `taxonomies/issues.yml`, `taxonomies/intents.yml`, `taxonomies/emotions.yml`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.<schema>.dim_contact_driver`
# MAGIC - `<catalog>.<schema>.dim_issue`
# MAGIC - `<catalog>.<schema>.dim_intent`
# MAGIC - `<catalog>.<schema>.dim_emotion_catalog`
# MAGIC - `<catalog>.<schema>.ops_file_status` (stage: `insights_02_load_taxonomies_to_dim_tables`)
# MAGIC - `<catalog>.<schema>.ops_pipeline_runs` (workflow: `insights`)
# MAGIC
# MAGIC **Key rules**
# MAGIC - Supports `sample | incremental | full` with taxonomy-level eligibility and failure isolation.
# MAGIC - Idempotent load policy by `taxonomy_load_mode`:
# MAGIC   - `replace_all`: replace full table for taxonomy
# MAGIC   - `upsert_by_version`: overwrite only matching `taxonomy_version`
# MAGIC - Stage fails only when eligible taxonomies exist and zero complete successfully/skipped.

# COMMAND ----------

import json
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import functions as F
from pyspark.sql import types as T


WORKFLOW_NAME = "insights"
STAGE_NAME = "insights_02_load_taxonomies_to_dim_tables"
ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
DEFAULT_TAXONOMIES_ROOT = "taxonomies"
DEFAULT_TAXONOMY_LOAD_MODE = "upsert_by_version"
ALLOWED_TAXONOMY_LOAD_MODES = {"replace_all", "upsert_by_version"}
ALLOWED_SENTIMENT_GROUPS = {"Positive", "Neutral", "Negative"}


TAXONOMY_SPECS = [
    {
        "taxonomy_key": "contact_drivers",
        "taxonomy_status_key": "taxonomy:contact_drivers",
        "file_name": "contact_drivers.yml",
        "table_name": "dim_contact_driver",
        "table_kind": "label",
        "default_taxonomy_name": "contact_drivers",
    },
    {
        "taxonomy_key": "issues",
        "taxonomy_status_key": "taxonomy:issues",
        "file_name": "issues.yml",
        "table_name": "dim_issue",
        "table_kind": "label",
        "default_taxonomy_name": "issues",
    },
    {
        "taxonomy_key": "intents",
        "taxonomy_status_key": "taxonomy:intents",
        "file_name": "intents.yml",
        "table_name": "dim_intent",
        "table_kind": "label",
        "default_taxonomy_name": "intents",
    },
    {
        "taxonomy_key": "emotions",
        "taxonomy_status_key": "taxonomy:emotions",
        "file_name": "emotions.yml",
        "table_name": "dim_emotion_catalog",
        "table_kind": "emotion",
        "default_taxonomy_name": "emotion_catalog",
    },
]


def _is_databricks() -> bool:
    return "dbutils" in globals()


def _sql_literal(value: Optional[object]) -> str:
    if value is None:
        return "NULL"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _validate_identifier(name: str, value: str) -> str:
    if not value or not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(
            f"Invalid `{name}`: {value!r}. Allowed pattern is [A-Za-z0-9_]+."
        )
    return value


def _parse_bool(name: str, raw: str, default: bool = False) -> bool:
    value = (raw or "").strip().lower()
    if not value:
        return default
    if value in {"1", "true", "t", "yes", "y"}:
        return True
    if value in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"Invalid boolean value for `{name}`: {raw!r}")


def _parse_active_flag(raw: object, field_name: str = "active") -> bool:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        raise ValueError(f"Missing required boolean field `{field_name}`.")
    return _parse_bool(field_name, str(raw), default=False)


def _truncate_error(message: object, max_len: int = 1000) -> str:
    text = str(message).strip() or "Unknown error"
    return text[:max_len]


def _fq_table(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def _table_exists(catalog: str, schema: str, table: str) -> bool:
    return bool(spark.catalog.tableExists(f"{catalog}.{schema}.{table}"))


def _ensure_tables(
    dim_contact_driver_table: str,
    dim_issue_table: str,
    dim_intent_table: str,
    dim_emotion_catalog_table: str,
    ops_file_status_table: str,
    ops_pipeline_runs_table: str,
) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {dim_contact_driver_table} (
          taxonomy_name STRING,
          taxonomy_version STRING,
          label STRING,
          active_flag BOOLEAN,
          definition STRING,
          synonyms ARRAY<STRING>,
          examples ARRAY<STRING>,
          source_file STRING,
          run_id STRING,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        )
        USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {dim_issue_table} (
          taxonomy_name STRING,
          taxonomy_version STRING,
          label STRING,
          active_flag BOOLEAN,
          definition STRING,
          synonyms ARRAY<STRING>,
          examples ARRAY<STRING>,
          source_file STRING,
          run_id STRING,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        )
        USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {dim_intent_table} (
          taxonomy_name STRING,
          taxonomy_version STRING,
          label STRING,
          active_flag BOOLEAN,
          definition STRING,
          synonyms ARRAY<STRING>,
          examples ARRAY<STRING>,
          source_file STRING,
          run_id STRING,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        )
        USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {dim_emotion_catalog_table} (
          taxonomy_name STRING,
          taxonomy_version STRING,
          catalog_version STRING,
          emotion_name STRING,
          sentiment_group STRING,
          polarity_score DOUBLE,
          active_flag BOOLEAN,
          definition STRING,
          synonyms ARRAY<STRING>,
          examples ARRAY<STRING>,
          source_file STRING,
          run_id STRING,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        )
        USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ops_file_status_table} (
          call_id STRING,
          stage_name STRING,
          status STRING,
          error_message STRING,
          run_id STRING,
          updated_at TIMESTAMP
        )
        USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ops_pipeline_runs_table} (
          run_id STRING,
          workflow_name STRING,
          started_at TIMESTAMP,
          ended_at TIMESTAMP,
          status STRING,
          trigger_type STRING,
          parameters STRING,
          error_summary STRING,
          total_files BIGINT,
          success_count BIGINT,
          failed_count BIGINT,
          updated_at TIMESTAMP
        )
        USING DELTA
        """
    )


def _upsert_pipeline_run_running(
    ops_pipeline_runs_table: str, run_id: str, parameters_json: str
) -> None:
    spark.sql(
        f"""
        MERGE INTO {ops_pipeline_runs_table} AS t
        USING (
          SELECT
            {_sql_literal(run_id)} AS run_id,
            {_sql_literal(WORKFLOW_NAME)} AS workflow_name,
            current_timestamp() AS started_at,
            CAST(NULL AS TIMESTAMP) AS ended_at,
            'RUNNING' AS status,
            {_sql_literal(parameters_json)} AS parameters,
            CAST(NULL AS STRING) AS error_summary,
            CAST(0 AS BIGINT) AS total_files,
            CAST(0 AS BIGINT) AS success_count,
            CAST(0 AS BIGINT) AS failed_count,
            current_timestamp() AS updated_at
        ) AS s
        ON t.run_id = s.run_id AND t.workflow_name = s.workflow_name
        WHEN MATCHED THEN UPDATE SET
          t.started_at = COALESCE(t.started_at, s.started_at),
          t.ended_at = NULL,
          t.status = s.status,
          t.parameters = s.parameters,
          t.error_summary = s.error_summary,
          t.total_files = s.total_files,
          t.success_count = s.success_count,
          t.failed_count = s.failed_count,
          t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (
          run_id,
          workflow_name,
          started_at,
          ended_at,
          status,
          trigger_type,
          parameters,
          error_summary,
          total_files,
          success_count,
          failed_count,
          updated_at
        ) VALUES (
          s.run_id,
          s.workflow_name,
          s.started_at,
          s.ended_at,
          s.status,
          NULL,
          s.parameters,
          s.error_summary,
          s.total_files,
          s.success_count,
          s.failed_count,
          s.updated_at
        )
        """
    )


def _upsert_pipeline_run_final(
    ops_pipeline_runs_table: str,
    run_id: str,
    final_status: str,
    total_files: int,
    success_count: int,
    failed_count: int,
    error_summary: Optional[str],
) -> None:
    spark.sql(
        f"""
        MERGE INTO {ops_pipeline_runs_table} AS t
        USING (
          SELECT
            {_sql_literal(run_id)} AS run_id,
            {_sql_literal(WORKFLOW_NAME)} AS workflow_name,
            {_sql_literal(final_status)} AS status,
            CAST({total_files} AS BIGINT) AS total_files,
            CAST({success_count} AS BIGINT) AS success_count,
            CAST({failed_count} AS BIGINT) AS failed_count,
            {_sql_literal(error_summary)} AS error_summary,
            current_timestamp() AS ended_at,
            current_timestamp() AS updated_at
        ) AS s
        ON t.run_id = s.run_id AND t.workflow_name = s.workflow_name
        WHEN MATCHED THEN UPDATE SET
          t.status = s.status,
          t.total_files = s.total_files,
          t.success_count = s.success_count,
          t.failed_count = s.failed_count,
          t.error_summary = s.error_summary,
          t.ended_at = s.ended_at,
          t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (
          run_id,
          workflow_name,
          started_at,
          ended_at,
          status,
          trigger_type,
          parameters,
          error_summary,
          total_files,
          success_count,
          failed_count,
          updated_at
        ) VALUES (
          s.run_id,
          s.workflow_name,
          current_timestamp(),
          s.ended_at,
          s.status,
          NULL,
          NULL,
          s.error_summary,
          s.total_files,
          s.success_count,
          s.failed_count,
          s.updated_at
        )
        """
    )


def _normalize_string_list(value: object, field_name: str) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, list):
        raise ValueError(f"`{field_name}` must be a list or string.")

    normalized: List[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            normalized.append(text)
    return normalized


def _load_yaml_file(path: str):
    try:
        import yaml  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "PyYAML is required to load taxonomy files (`yaml.safe_load`)."
        ) from exc

    with open(path, "r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)

    if payload is None:
        raise ValueError(f"YAML is empty: {path}")
    return payload


def _extract_items(payload, file_name: str) -> List[object]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("items", "labels", "emotions", "values"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        for value in payload.values():
            if isinstance(value, list):
                return value
    raise ValueError(
        f"Unsupported YAML structure for {file_name}. Expected list or dict with list-valued items."
    )


def _extract_meta(payload) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not isinstance(payload, dict):
        return None, None, None

    taxonomy_obj = payload.get("taxonomy")
    if isinstance(taxonomy_obj, dict):
        return (
            str(taxonomy_obj.get("name") or "").strip() or None,
            str(taxonomy_obj.get("version") or "").strip() or None,
            "taxonomy",
        )

    catalog_obj = payload.get("catalog")
    if isinstance(catalog_obj, dict):
        return (
            str(catalog_obj.get("name") or "").strip() or None,
            str(catalog_obj.get("version") or "").strip() or None,
            "catalog",
        )

    return None, None, None


def _resolve_repo_taxonomy_file(taxonomies_root: str, file_name: str) -> str:
    root = (taxonomies_root or DEFAULT_TAXONOMIES_ROOT).strip()
    candidates: List[str] = []

    if os.path.isabs(root):
        candidates.append(os.path.join(root, file_name))
    else:
        cwd = os.getcwd()
        candidates.append(os.path.join(cwd, root, file_name))
        candidates.append(os.path.join(root, file_name))

        if "__file__" in globals():
            script_dir = os.path.dirname(os.path.abspath(__file__))
            repo_relative_root = os.path.abspath(
                os.path.join(script_dir, "..", "..", root)
            )
            candidates.append(os.path.join(repo_relative_root, file_name))

        if _is_databricks():
            try:
                notebook_path = (
                    dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
                    .notebook()
                    .getContext()
                    .notebookPath()
                    .get()
                )
                workspace_path = os.path.join("/Workspace", notebook_path.lstrip("/"))
                repo_root = os.path.abspath(os.path.join(workspace_path, "..", "..", ".."))
                candidates.append(os.path.join(repo_root, root, file_name))
            except Exception:
                pass

    deduped: List[str] = []
    seen = set()
    for candidate in candidates:
        candidate_norm = os.path.normpath(candidate)
        if candidate_norm in seen:
            continue
        seen.add(candidate_norm)
        deduped.append(candidate_norm)

    for candidate in deduped:
        if os.path.isfile(candidate):
            return candidate

    raise FileNotFoundError(
        f"Taxonomy file not found: {file_name}. Checked paths: {deduped}"
    )


def _build_label_rows(
    items: Sequence[object],
    taxonomy_name: str,
    taxonomy_version: str,
    source_file: str,
    run_id: str,
    require_examples: bool,
    enforce_unique_labels: bool,
    process_ts: datetime,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    seen_labels = set()
    duplicate_labels = set()

    for item in items:
        if not isinstance(item, dict):
            raise ValueError("Each taxonomy item must be an object.")

        label = str(item.get("label") or "").strip()
        if not label:
            raise ValueError("Missing required field `label`.")

        active_flag = _parse_active_flag(item.get("active"), "active")
        definition = str(item.get("definition") or "").strip()
        if not definition:
            raise ValueError(f"Missing required field `definition` for label `{label}`.")

        synonyms = _normalize_string_list(item.get("synonyms"), "synonyms")
        examples = _normalize_string_list(item.get("examples"), "examples")
        if require_examples and not examples:
            raise ValueError(f"`examples` is required and empty for label `{label}`.")

        normalized_key = label.lower()
        if normalized_key in seen_labels:
            duplicate_labels.add(label)
        seen_labels.add(normalized_key)

        rows.append(
            {
                "taxonomy_name": taxonomy_name,
                "taxonomy_version": taxonomy_version,
                "label": label,
                "active_flag": bool(active_flag),
                "definition": definition,
                "synonyms": synonyms,
                "examples": examples,
                "source_file": source_file,
                "run_id": run_id,
                "created_at": process_ts,
                "updated_at": process_ts,
            }
        )

    if enforce_unique_labels and duplicate_labels:
        raise ValueError(
            "Duplicate labels detected within taxonomy version: "
            + ", ".join(sorted(duplicate_labels))
        )

    return rows


def _build_emotion_rows(
    items: Sequence[object],
    taxonomy_name: str,
    taxonomy_version: str,
    source_file: str,
    run_id: str,
    require_examples: bool,
    enforce_unique_labels: bool,
    process_ts: datetime,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    seen_emotions = set()
    duplicate_emotions = set()

    for item in items:
        if not isinstance(item, dict):
            raise ValueError("Each emotion item must be an object.")

        emotion_name = str(item.get("emotion") or item.get("label") or "").strip()
        if not emotion_name:
            raise ValueError("Missing required field `emotion`.")

        active_flag = _parse_active_flag(item.get("active"), "active")
        definition = str(item.get("definition") or "").strip()
        if not definition:
            raise ValueError(
                f"Missing required field `definition` for emotion `{emotion_name}`."
            )

        sentiment_group = str(item.get("sentiment_group") or "").strip()
        if sentiment_group not in ALLOWED_SENTIMENT_GROUPS:
            raise ValueError(
                f"Invalid `sentiment_group` for emotion `{emotion_name}`: {sentiment_group!r}. "
                f"Allowed: {sorted(ALLOWED_SENTIMENT_GROUPS)}"
            )

        polarity_raw = item.get("polarity_score")
        if polarity_raw is None:
            raise ValueError(
                f"Missing required field `polarity_score` for emotion `{emotion_name}`."
            )
        try:
            polarity_score = float(polarity_raw)
        except Exception as exc:
            raise ValueError(
                f"`polarity_score` must be numeric for emotion `{emotion_name}`."
            ) from exc
        if polarity_score < -1.0 or polarity_score > 1.0:
            raise ValueError(
                f"`polarity_score` out of range [-1,1] for emotion `{emotion_name}`: "
                f"{polarity_score}"
            )

        synonyms = _normalize_string_list(item.get("synonyms"), "synonyms")
        examples = _normalize_string_list(item.get("examples"), "examples")
        if require_examples and not examples:
            raise ValueError(
                f"`examples` is required and empty for emotion `{emotion_name}`."
            )

        normalized_key = emotion_name.lower()
        if normalized_key in seen_emotions:
            duplicate_emotions.add(emotion_name)
        seen_emotions.add(normalized_key)

        rows.append(
            {
                "taxonomy_name": taxonomy_name,
                "taxonomy_version": taxonomy_version,
                "catalog_version": taxonomy_version,
                "emotion_name": emotion_name,
                "sentiment_group": sentiment_group,
                "polarity_score": float(polarity_score),
                "active_flag": bool(active_flag),
                "definition": definition,
                "synonyms": synonyms,
                "examples": examples,
                "source_file": source_file,
                "run_id": run_id,
                "created_at": process_ts,
                "updated_at": process_ts,
            }
        )

    if enforce_unique_labels and duplicate_emotions:
        raise ValueError(
            "Duplicate emotions detected within taxonomy version: "
            + ", ".join(sorted(duplicate_emotions))
        )

    return rows


def _write_label_rows(
    table_fq: str,
    rows: List[Dict[str, object]],
    taxonomy_version: str,
    taxonomy_load_mode: str,
) -> int:
    if taxonomy_load_mode == "replace_all":
        spark.sql(f"DELETE FROM {table_fq}")
    else:
        spark.sql(
            f"""
            DELETE FROM {table_fq}
            WHERE taxonomy_version = {_sql_literal(taxonomy_version)}
            """
        )

    schema_def = T.StructType(
        [
            T.StructField("taxonomy_name", T.StringType(), False),
            T.StructField("taxonomy_version", T.StringType(), False),
            T.StructField("label", T.StringType(), False),
            T.StructField("active_flag", T.BooleanType(), False),
            T.StructField("definition", T.StringType(), False),
            T.StructField("synonyms", T.ArrayType(T.StringType()), False),
            T.StructField("examples", T.ArrayType(T.StringType()), False),
            T.StructField("source_file", T.StringType(), False),
            T.StructField("run_id", T.StringType(), False),
            T.StructField("created_at", T.TimestampType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    )
    out_df = spark.createDataFrame(rows, schema=schema_def)
    out_df.write.format("delta").mode("append").saveAsTable(table_fq)
    return len(rows)


def _write_emotion_rows(
    table_fq: str,
    rows: List[Dict[str, object]],
    taxonomy_version: str,
    taxonomy_load_mode: str,
) -> int:
    if taxonomy_load_mode == "replace_all":
        spark.sql(f"DELETE FROM {table_fq}")
    else:
        spark.sql(
            f"""
            DELETE FROM {table_fq}
            WHERE taxonomy_version = {_sql_literal(taxonomy_version)}
            """
        )

    schema_def = T.StructType(
        [
            T.StructField("taxonomy_name", T.StringType(), False),
            T.StructField("taxonomy_version", T.StringType(), False),
            T.StructField("catalog_version", T.StringType(), False),
            T.StructField("emotion_name", T.StringType(), False),
            T.StructField("sentiment_group", T.StringType(), False),
            T.StructField("polarity_score", T.DoubleType(), False),
            T.StructField("active_flag", T.BooleanType(), False),
            T.StructField("definition", T.StringType(), False),
            T.StructField("synonyms", T.ArrayType(T.StringType()), False),
            T.StructField("examples", T.ArrayType(T.StringType()), False),
            T.StructField("source_file", T.StringType(), False),
            T.StructField("run_id", T.StringType(), False),
            T.StructField("created_at", T.TimestampType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    )
    out_df = spark.createDataFrame(rows, schema=schema_def)
    out_df.write.format("delta").mode("append").saveAsTable(table_fq)
    return len(rows)


if _is_databricks():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("taxonomy_version", "")
    dbutils.widgets.text("taxonomies_root", DEFAULT_TAXONOMIES_ROOT)
    dbutils.widgets.text("taxonomy_load_mode", DEFAULT_TAXONOMY_LOAD_MODE)
    dbutils.widgets.text("enforce_unique_labels", "true")
    dbutils.widgets.text("require_examples", "true")
    dbutils.widgets.text("enable_dim_tables", "true")

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    taxonomy_version_override = dbutils.widgets.get("taxonomy_version").strip()
    taxonomies_root = dbutils.widgets.get("taxonomies_root").strip() or DEFAULT_TAXONOMIES_ROOT
    taxonomy_load_mode = (
        dbutils.widgets.get("taxonomy_load_mode").strip().lower()
        or DEFAULT_TAXONOMY_LOAD_MODE
    )
    enforce_unique_labels_raw = dbutils.widgets.get("enforce_unique_labels").strip()
    require_examples_raw = dbutils.widgets.get("require_examples").strip()
    enable_dim_tables_raw = dbutils.widgets.get("enable_dim_tables").strip()
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    taxonomy_version_override = os.getenv("TAXONOMY_VERSION", "").strip()
    taxonomies_root = (
        os.getenv("TAXONOMIES_ROOT", DEFAULT_TAXONOMIES_ROOT).strip()
        or DEFAULT_TAXONOMIES_ROOT
    )
    taxonomy_load_mode = (
        os.getenv("TAXONOMY_LOAD_MODE", DEFAULT_TAXONOMY_LOAD_MODE).strip().lower()
        or DEFAULT_TAXONOMY_LOAD_MODE
    )
    enforce_unique_labels_raw = os.getenv("ENFORCE_UNIQUE_LABELS", "true").strip()
    require_examples_raw = os.getenv("REQUIRE_EXAMPLES", "true").strip()
    enable_dim_tables_raw = os.getenv("ENABLE_DIM_TABLES", "true").strip()

catalog = _validate_identifier("catalog", catalog)
schema = _validate_identifier("schema", schema)
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid `run_mode`: {run_mode!r}. Allowed: {sorted(ALLOWED_RUN_MODES)}")
if taxonomy_load_mode not in ALLOWED_TAXONOMY_LOAD_MODES:
    raise ValueError(
        f"Invalid `taxonomy_load_mode`: {taxonomy_load_mode!r}. "
        f"Allowed: {sorted(ALLOWED_TAXONOMY_LOAD_MODES)}"
    )

max_files_per_run: Optional[int] = None
if max_files_per_run_raw:
    max_files_per_run = int(max_files_per_run_raw)
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError("In sample mode, `max_files_per_run` must be provided as an integer > 0.")

enforce_unique_labels = _parse_bool(
    "enforce_unique_labels", enforce_unique_labels_raw, default=True
)
require_examples = _parse_bool("require_examples", require_examples_raw, default=True)
enable_dim_tables = _parse_bool("enable_dim_tables", enable_dim_tables_raw, default=True)

dim_contact_driver_table = _fq_table(catalog, schema, "dim_contact_driver")
dim_issue_table = _fq_table(catalog, schema, "dim_issue")
dim_intent_table = _fq_table(catalog, schema, "dim_intent")
dim_emotion_catalog_table = _fq_table(catalog, schema, "dim_emotion_catalog")
ops_file_status_table = _fq_table(catalog, schema, "ops_file_status")
ops_pipeline_runs_table = _fq_table(catalog, schema, "ops_pipeline_runs")

table_lookup = {
    "dim_contact_driver": dim_contact_driver_table,
    "dim_issue": dim_issue_table,
    "dim_intent": dim_intent_table,
    "dim_emotion_catalog": dim_emotion_catalog_table,
}

params_snapshot = {
    "catalog": catalog,
    "schema": schema,
    "run_id": run_id,
    "run_mode": run_mode,
    "max_files_per_run": max_files_per_run,
    "taxonomy_version": taxonomy_version_override or None,
    "taxonomies_root": taxonomies_root,
    "taxonomy_load_mode": taxonomy_load_mode,
    "enforce_unique_labels": enforce_unique_labels,
    "require_examples": require_examples,
    "enable_dim_tables": enable_dim_tables,
}
parameters_json = json.dumps(params_snapshot, sort_keys=True)

print(
    f"[{STAGE_NAME}] Starting with run_id={run_id}, run_mode={run_mode}, "
    f"taxonomy_load_mode={taxonomy_load_mode}"
)

_ensure_tables(
    dim_contact_driver_table=dim_contact_driver_table,
    dim_issue_table=dim_issue_table,
    dim_intent_table=dim_intent_table,
    dim_emotion_catalog_table=dim_emotion_catalog_table,
    ops_file_status_table=ops_file_status_table,
    ops_pipeline_runs_table=ops_pipeline_runs_table,
)
_upsert_pipeline_run_running(ops_pipeline_runs_table, run_id, parameters_json)

eligible_count = 0
ops_success_count = 0
ops_failed_count = 0
ops_skipped_count = 0
rows_written = 0
loaded_versions: Dict[str, str] = {}
final_status = "SUCCESS"
error_summary: Optional[str] = None

try:
    stage_status_map: Dict[str, str] = {}
    if _table_exists(catalog, schema, "ops_file_status"):
        stage_rows = (
            spark.table(ops_file_status_table)
            .where(F.col("stage_name") == STAGE_NAME)
            .select("call_id", "status")
            .collect()
        )
        for row in stage_rows:
            call_id = str(row["call_id"] or "")
            status = str(row["status"] or "").upper()
            if call_id:
                stage_status_map[call_id] = status

    selected_specs = []
    for spec in TAXONOMY_SPECS:
        status_key = spec["taxonomy_status_key"]
        previous_status = stage_status_map.get(status_key)
        should_run = True
        if run_mode in {"sample", "incremental"}:
            should_run = previous_status is None or previous_status == "FAILED"
        if should_run:
            selected_specs.append(spec)

    if run_mode == "sample" and max_files_per_run is not None:
        selected_specs = selected_specs[:max_files_per_run]

    eligible_count = len(selected_specs)
    print(f"[{STAGE_NAME}] Eligible taxonomies: {eligible_count}")

    status_records: List[Dict[str, object]] = []
    process_ts = datetime.utcnow()

    for spec in selected_specs:
        taxonomy_key = spec["taxonomy_key"]
        taxonomy_status_key = spec["taxonomy_status_key"]
        table_kind = spec["table_kind"]
        table_name = spec["table_name"]
        table_fq = table_lookup[table_name]
        file_name = spec["file_name"]

        if not enable_dim_tables:
            ops_skipped_count += 1
            status_records.append(
                {
                    "call_id": taxonomy_status_key,
                    "stage_name": STAGE_NAME,
                    "status": "SKIPPED",
                    "error_message": "Dim table loading disabled by `enable_dim_tables=false`.",
                    "run_id": run_id,
                    "updated_at": process_ts,
                }
            )
            continue

        try:
            source_path = _resolve_repo_taxonomy_file(taxonomies_root, file_name)
            payload = _load_yaml_file(source_path)
            items = _extract_items(payload, file_name=file_name)
            if not items:
                raise ValueError(f"No taxonomy items found in {file_name}.")

            meta_name, meta_version, _meta_type = _extract_meta(payload)
            taxonomy_name = meta_name or str(spec["default_taxonomy_name"])
            taxonomy_version = taxonomy_version_override or (meta_version or "")
            if not taxonomy_version:
                raise ValueError(
                    f"Taxonomy version is required for {file_name}. "
                    "Provide YAML version or `taxonomy_version` parameter."
                )

            source_file_ref = os.path.relpath(source_path, start=os.getcwd())
            if source_file_ref.startswith(".."):
                source_file_ref = source_path

            if table_kind == "label":
                rows = _build_label_rows(
                    items=items,
                    taxonomy_name=taxonomy_name,
                    taxonomy_version=taxonomy_version,
                    source_file=source_file_ref,
                    run_id=run_id,
                    require_examples=require_examples,
                    enforce_unique_labels=enforce_unique_labels,
                    process_ts=process_ts,
                )
                rows_written += _write_label_rows(
                    table_fq=table_fq,
                    rows=rows,
                    taxonomy_version=taxonomy_version,
                    taxonomy_load_mode=taxonomy_load_mode,
                )
            else:
                rows = _build_emotion_rows(
                    items=items,
                    taxonomy_name=taxonomy_name,
                    taxonomy_version=taxonomy_version,
                    source_file=source_file_ref,
                    run_id=run_id,
                    require_examples=require_examples,
                    enforce_unique_labels=enforce_unique_labels,
                    process_ts=process_ts,
                )
                rows_written += _write_emotion_rows(
                    table_fq=table_fq,
                    rows=rows,
                    taxonomy_version=taxonomy_version,
                    taxonomy_load_mode=taxonomy_load_mode,
                )

            ops_success_count += 1
            loaded_versions[taxonomy_key] = taxonomy_version
            status_records.append(
                {
                    "call_id": taxonomy_status_key,
                    "stage_name": STAGE_NAME,
                    "status": "SUCCESS",
                    "error_message": None,
                    "run_id": run_id,
                    "updated_at": process_ts,
                }
            )
        except Exception as exc:
            ops_failed_count += 1
            status_records.append(
                {
                    "call_id": taxonomy_status_key,
                    "stage_name": STAGE_NAME,
                    "status": "FAILED",
                    "error_message": _truncate_error(exc),
                    "run_id": run_id,
                    "updated_at": process_ts,
                }
            )

    if status_records:
        status_schema = T.StructType(
            [
                T.StructField("call_id", T.StringType(), False),
                T.StructField("stage_name", T.StringType(), False),
                T.StructField("status", T.StringType(), False),
                T.StructField("error_message", T.StringType(), True),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )
        status_df = spark.createDataFrame(status_records, schema=status_schema)
        status_df.createOrReplaceTempView("tmp_insights_02_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file_status_table} AS t
            USING tmp_insights_02_status AS s
            ON t.call_id = s.call_id AND t.stage_name = s.stage_name
            WHEN MATCHED THEN UPDATE SET
              t.status = s.status,
              t.error_message = s.error_message,
              t.run_id = s.run_id,
              t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (
              call_id,
              stage_name,
              status,
              error_message,
              run_id,
              updated_at
            ) VALUES (
              s.call_id,
              s.stage_name,
              s.status,
              s.error_message,
              s.run_id,
              s.updated_at
            )
            """
        )

    success_like_count = ops_success_count + ops_skipped_count
    if eligible_count > 0 and success_like_count == 0:
        final_status = "FAILED"
        error_summary = (
            "Zero taxonomy artifacts completed in insights_02_load_taxonomies_to_dim_tables "
            "while eligible artifacts existed."
        )
        raise RuntimeError(error_summary)

    if ops_failed_count > 0:
        final_status = "WARN"
        error_summary = (
            f"{ops_failed_count} taxonomy artifact(s) failed in {STAGE_NAME}; "
            f"loaded_versions={json.dumps(loaded_versions, sort_keys=True)}"
        )
    elif eligible_count == 0:
        final_status = "SUCCESS"
        error_summary = f"No eligible taxonomy artifacts for stage {STAGE_NAME}."
    else:
        final_status = "SUCCESS"
        error_summary = (
            "Taxonomy artifacts loaded successfully: "
            + json.dumps(loaded_versions, sort_keys=True)
        )

except Exception as exc:
    final_status = "FAILED"
    if error_summary is None:
        error_summary = _truncate_error(exc)
    raise
finally:
    _upsert_pipeline_run_final(
        ops_pipeline_runs_table=ops_pipeline_runs_table,
        run_id=run_id,
        final_status=final_status,
        total_files=eligible_count,
        success_count=ops_success_count + ops_skipped_count,
        failed_count=ops_failed_count,
        error_summary=error_summary,
    )
    print(
        f"[{STAGE_NAME}] eligible={eligible_count} success_items={ops_success_count} "
        f"skipped_items={ops_skipped_count} failed_items={ops_failed_count} "
        f"rows_written={rows_written} status={final_status}"
    )
