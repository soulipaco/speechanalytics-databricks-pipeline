# Databricks notebook source
# MAGIC %md
# MAGIC # FOUNDATION_06_REDACT_PII
# MAGIC
# MAGIC **Purpose**
# MAGIC - Convert aligned turns into compliance-safe text by redacting PII.
# MAGIC
# MAGIC **Inputs**
# MAGIC - Parameters: `catalog`, `schema`, `run_id`, `run_mode`, `max_files_per_run`, `enable_pii_redaction`, `presidio_language`, `redaction_version`, `redaction_placeholders`, `enable_residual_risk_scan`
# MAGIC - Table: `<catalog>.<schema>.silver_turns_aligned`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.<schema>.gold_turns_redacted`
# MAGIC - `<catalog>.<schema>.ops_file_status` (stage: `redact_pii`)
# MAGIC - `<catalog>.<schema>.ops_pipeline_runs`
# MAGIC
# MAGIC **Key rules**
# MAGIC - Redaction order: Presidio (if available) -> regex layer -> residual risk scan.
# MAGIC - No raw detected PII values are stored in output tables.
# MAGIC - Supports `sample | incremental | full` with idempotent per-call overwrite.

# COMMAND ----------

import json
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pyspark.sql import functions as F
from pyspark.sql import types as T


WORKFLOW_NAME = "foundation"
STAGE_NAME = "redact_pii"
ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
DEFAULT_PRESIDIO_LANGUAGE = "en"
DEFAULT_REDACTION_VERSION = "v1"
PII_ENTITY_STRUCT_FIELDS = ["entity_type", "start", "end", "source"]

DEFAULT_PLACEHOLDERS = {
    "PERSON": "[PERSON]",
    "PHONE_NUMBER": "[PHONE]",
    "EMAIL_ADDRESS": "[EMAIL]",
    "CREDIT_CARD": "[CARD]",
    "LOCATION": "[ADDRESS]",
    "URL": "[URL]",
    "IBAN_CODE": "[IBAN]",
    "DIGIT_SEQUENCE": "[NUMBER]",
    "DEFAULT": "[REDACTED]",
}

REGEX_RULES = [
    {
        "entity_type": "EMAIL_ADDRESS",
        "pattern": re.compile(r"(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b"),
    },
    {
        "entity_type": "URL",
        "pattern": re.compile(r"(?i)\b(?:https?://|www\.)\S+\b"),
    },
    {
        "entity_type": "IBAN_CODE",
        "pattern": re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b"),
    },
    {
        "entity_type": "CREDIT_CARD",
        "pattern": re.compile(r"\b(?:\d[ -]*?){13,19}\b"),
    },
    {
        "entity_type": "PHONE_NUMBER",
        "pattern": re.compile(r"(?<!\w)(?:\+?\d[\d\s().-]{7,}\d)(?!\w)"),
    },
    {
        "entity_type": "DIGIT_SEQUENCE",
        "pattern": re.compile(r"\b\d{6,}\b"),
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


def _truncate_error(message: object, max_len: int = 1000) -> str:
    text = str(message).strip() or "Unknown error"
    return text[:max_len]


def _fq_table(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def _table_exists(catalog: str, schema: str, table: str) -> bool:
    return bool(spark.catalog.tableExists(f"{catalog}.{schema}.{table}"))


def _parse_placeholders(raw: str) -> Dict[str, str]:
    parsed = DEFAULT_PLACEHOLDERS.copy()
    if not raw or not raw.strip():
        return parsed
    try:
        user_map = json.loads(raw)
    except Exception as exc:
        raise ValueError(
            "Invalid `redaction_placeholders` JSON. Expected object map."
        ) from exc
    if not isinstance(user_map, dict):
        raise ValueError("`redaction_placeholders` must be a JSON object.")
    for key, value in user_map.items():
        key_text = str(key).strip().upper()
        value_text = str(value).strip()
        if key_text and value_text:
            parsed[key_text] = value_text
    if not parsed.get("DEFAULT"):
        parsed["DEFAULT"] = "[REDACTED]"
    return parsed


def _ensure_tables(
    redacted_table: str, ops_file_status_table: str, ops_pipeline_runs_table: str
) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {redacted_table} (
          call_id STRING,
          turn_id STRING,
          role STRING,
          start_sec DOUBLE,
          end_sec DOUBLE,
          text_redacted STRING,
          pii_found_flag BOOLEAN,
          pii_entities ARRAY<STRUCT<entity_type:STRING,start:INT,end:INT,source:STRING>>,
          pii_entity_counts MAP<STRING,INT>,
          pii_residual_risk_flag BOOLEAN,
          redaction_method STRING,
          redaction_version STRING,
          run_id STRING,
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


def _merge_counts(*maps: Dict[str, int]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for mapping in maps:
        for key, value in mapping.items():
            out[key] = out.get(key, 0) + int(value)
    return out


def _apply_regex_redaction(
    text: str,
    placeholders: Dict[str, str],
) -> Tuple[str, List[Dict[str, object]], Dict[str, int]]:
    redacted = text
    entities: List[Dict[str, object]] = []
    counts: Dict[str, int] = {}

    for rule in REGEX_RULES:
        entity_type = rule["entity_type"]
        pattern = rule["pattern"]
        replacement = placeholders.get(entity_type, placeholders["DEFAULT"])

        matches = list(pattern.finditer(redacted))
        if not matches:
            continue

        counts[entity_type] = counts.get(entity_type, 0) + len(matches)
        for match in matches:
            entities.append(
                {
                    "entity_type": entity_type,
                    "start": int(match.start()),
                    "end": int(match.end()),
                    "source": "regex",
                }
            )
        redacted = pattern.sub(replacement, redacted)

    return redacted, entities, counts


def _build_presidio_backend(
    presidio_language: str,
):
    try:
        from presidio_analyzer import AnalyzerEngine  # type: ignore
        from presidio_anonymizer import AnonymizerEngine  # type: ignore
        from presidio_anonymizer.entities import OperatorConfig  # type: ignore
    except Exception as exc:
        return None, None, None, _truncate_error(exc, 300)

    try:
        analyzer = AnalyzerEngine()
        anonymizer = AnonymizerEngine()
        return analyzer, anonymizer, OperatorConfig, None
    except Exception as exc:
        return None, None, None, _truncate_error(exc, 300)


def _apply_presidio_redaction(
    text: str,
    presidio_language: str,
    analyzer,
    anonymizer,
    operator_config_class,
    placeholders: Dict[str, str],
) -> Tuple[str, List[Dict[str, object]], Dict[str, int]]:
    if analyzer is None or anonymizer is None or operator_config_class is None:
        return text, [], {}

    analysis_results = analyzer.analyze(text=text, language=presidio_language)
    if not analysis_results:
        return text, [], {}

    operators: Dict[str, object] = {}
    entities: List[Dict[str, object]] = []
    counts: Dict[str, int] = {}
    for item in analysis_results:
        entity_type = str(item.entity_type).upper()
        replacement = placeholders.get(entity_type, placeholders["DEFAULT"])
        operators[entity_type] = operator_config_class("replace", {"new_value": replacement})
        counts[entity_type] = counts.get(entity_type, 0) + 1
        entities.append(
            {
                "entity_type": entity_type,
                "start": int(item.start),
                "end": int(item.end),
                "source": "presidio",
            }
        )

    anonymized = anonymizer.anonymize(
        text=text,
        analyzer_results=analysis_results,
        operators=operators,
    )
    return anonymized.text, entities, counts


def _residual_risk_scan(text_redacted: str) -> bool:
    for rule in REGEX_RULES:
        if rule["pattern"].search(text_redacted):
            return True
    return False


if _is_databricks():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("enable_pii_redaction", "true")
    dbutils.widgets.text("presidio_language", DEFAULT_PRESIDIO_LANGUAGE)
    dbutils.widgets.text("redaction_version", DEFAULT_REDACTION_VERSION)
    dbutils.widgets.text("redaction_placeholders", "{}")
    dbutils.widgets.text("enable_residual_risk_scan", "true")

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    enable_pii_redaction_raw = dbutils.widgets.get("enable_pii_redaction").strip()
    presidio_language = dbutils.widgets.get("presidio_language").strip() or DEFAULT_PRESIDIO_LANGUAGE
    redaction_version = dbutils.widgets.get("redaction_version").strip() or DEFAULT_REDACTION_VERSION
    redaction_placeholders_raw = dbutils.widgets.get("redaction_placeholders").strip()
    enable_residual_risk_scan_raw = dbutils.widgets.get("enable_residual_risk_scan").strip()
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    enable_pii_redaction_raw = os.getenv("ENABLE_PII_REDACTION", "true").strip()
    presidio_language = os.getenv("PRESIDIO_LANGUAGE", DEFAULT_PRESIDIO_LANGUAGE).strip() or DEFAULT_PRESIDIO_LANGUAGE
    redaction_version = os.getenv("REDACTION_VERSION", DEFAULT_REDACTION_VERSION).strip() or DEFAULT_REDACTION_VERSION
    redaction_placeholders_raw = os.getenv("REDACTION_PLACEHOLDERS", "{}").strip()
    enable_residual_risk_scan_raw = os.getenv("ENABLE_RESIDUAL_RISK_SCAN", "true").strip()

catalog = _validate_identifier("catalog", catalog)
schema = _validate_identifier("schema", schema)
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid `run_mode`: {run_mode!r}. Allowed: {sorted(ALLOWED_RUN_MODES)}")

max_files_per_run: Optional[int] = None
if max_files_per_run_raw:
    max_files_per_run = int(max_files_per_run_raw)
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError("In sample mode, `max_files_per_run` must be provided as an integer > 0.")

enable_pii_redaction = _parse_bool("enable_pii_redaction", enable_pii_redaction_raw, default=True)
enable_residual_risk_scan = _parse_bool(
    "enable_residual_risk_scan", enable_residual_risk_scan_raw, default=True
)
redaction_placeholders = _parse_placeholders(redaction_placeholders_raw)

input_turns_table = _fq_table(catalog, schema, "silver_turns_aligned")
redacted_table = _fq_table(catalog, schema, "gold_turns_redacted")
ops_file_status_table = _fq_table(catalog, schema, "ops_file_status")
ops_pipeline_runs_table = _fq_table(catalog, schema, "ops_pipeline_runs")

params_snapshot = {
    "catalog": catalog,
    "schema": schema,
    "run_id": run_id,
    "run_mode": run_mode,
    "max_files_per_run": max_files_per_run,
    "enable_pii_redaction": enable_pii_redaction,
    "presidio_language": presidio_language,
    "redaction_version": redaction_version,
    "redaction_placeholders": redaction_placeholders,
    "enable_residual_risk_scan": enable_residual_risk_scan,
}
parameters_json = json.dumps(params_snapshot, sort_keys=True)

print(
    f"[{STAGE_NAME}] Starting with run_id={run_id}, run_mode={run_mode}, "
    f"redaction_version={redaction_version}"
)

_ensure_tables(redacted_table, ops_file_status_table, ops_pipeline_runs_table)
_upsert_pipeline_run_running(ops_pipeline_runs_table, run_id, parameters_json)

eligible_count = 0
ops_success_count = 0
ops_failed_count = 0
ops_skipped_count = 0
rows_written = 0
final_status = "SUCCESS"
error_summary: Optional[str] = None

try:
    if not _table_exists(catalog, schema, "silver_turns_aligned"):
        raise RuntimeError(f"Required input table is missing: {input_turns_table}")

    turns_df = (
        spark.table(input_turns_table)
        .select(
            "call_id",
            "turn_id",
            "role",
            "start_sec",
            "end_sec",
            "text_original",
        )
        .where("call_id IS NOT NULL AND turn_id IS NOT NULL")
    )

    stage_df = (
        spark.table(ops_file_status_table)
        .where(F.col("stage_name") == STAGE_NAME)
        .select("call_id", F.col("status").alias("stage_status"))
    )

    call_df = turns_df.select("call_id").distinct().orderBy("call_id")
    call_df = call_df.join(stage_df, on="call_id", how="left")
    if run_mode in {"sample", "incremental"}:
        call_df = call_df.where(
            F.col("stage_status").isNull() | (F.upper(F.col("stage_status")) == "FAILED")
        )
    if run_mode == "sample" and max_files_per_run is not None:
        call_df = call_df.limit(max_files_per_run)

    eligible_calls = [row["call_id"] for row in call_df.select("call_id").collect()]
    eligible_count = len(eligible_calls)
    print(f"[{STAGE_NAME}] Eligible calls: {eligible_count}")

    presidio_warning: Optional[str] = None
    analyzer = anonymizer = operator_config_class = None
    if enable_pii_redaction:
        analyzer, anonymizer, operator_config_class, presidio_warning = _build_presidio_backend(
            presidio_language=presidio_language
        )
        if presidio_warning:
            print(
                f"[{STAGE_NAME}] Presidio unavailable, regex fallback enabled: {presidio_warning}"
            )

    status_records: List[Dict[str, object]] = []
    redacted_rows: List[Dict[str, object]] = []
    successful_call_ids: List[str] = []
    skipped_call_ids: List[str] = []
    process_ts = datetime.utcnow()

    for call_id in eligible_calls:
        try:
            call_turns = (
                turns_df.where(F.col("call_id") == call_id)
                .orderBy("start_sec", "end_sec", "turn_id")
                .collect()
            )
            if not call_turns:
                raise ValueError("No turns found for call_id.")

            call_status = "SUCCESS"
            redaction_method = "presidio+regex"
            if not enable_pii_redaction:
                call_status = "SKIPPED"
                redaction_method = "disabled_passthrough"
            elif presidio_warning:
                redaction_method = "regex_only_fallback"

            call_output_rows: List[Dict[str, object]] = []
            for turn in call_turns:
                text_original = str(turn["text_original"] or "")
                if text_original is None:
                    text_original = ""

                text_after_presidio = text_original
                pres_entities: List[Dict[str, object]] = []
                pres_counts: Dict[str, int] = {}

                if enable_pii_redaction and analyzer is not None:
                    try:
                        text_after_presidio, pres_entities, pres_counts = _apply_presidio_redaction(
                            text=text_original,
                            presidio_language=presidio_language,
                            analyzer=analyzer,
                            anonymizer=anonymizer,
                            operator_config_class=operator_config_class,
                            placeholders=redaction_placeholders,
                        )
                    except Exception:
                        redaction_method = "regex_only_fallback"
                        text_after_presidio = text_original
                        pres_entities = []
                        pres_counts = {}

                if enable_pii_redaction:
                    text_after_regex, regex_entities, regex_counts = _apply_regex_redaction(
                        text=text_after_presidio,
                        placeholders=redaction_placeholders,
                    )
                else:
                    text_after_regex = text_original
                    regex_entities, regex_counts = [], {}

                pii_entities = pres_entities + regex_entities
                pii_entity_counts = _merge_counts(pres_counts, regex_counts)
                pii_found_flag = bool(sum(pii_entity_counts.values()) > 0)
                residual_flag = (
                    _residual_risk_scan(text_after_regex)
                    if enable_residual_risk_scan
                    else False
                )

                if text_after_regex is None:
                    raise ValueError("text_redacted is null.")
                if pii_found_flag and not pii_entity_counts:
                    raise ValueError(
                        "pii_found_flag is true but pii_entity_counts is empty."
                    )

                sanitized_entities = []
                for entity in pii_entities:
                    sanitized_entities.append(
                        {
                            "entity_type": str(entity.get("entity_type", "UNKNOWN")),
                            "start": int(entity.get("start", -1)),
                            "end": int(entity.get("end", -1)),
                            "source": str(entity.get("source", "unknown")),
                        }
                    )

                call_output_rows.append(
                    {
                        "call_id": call_id,
                        "turn_id": str(turn["turn_id"]),
                        "role": str(turn["role"] or "Unknown"),
                        "start_sec": float(turn["start_sec"]),
                        "end_sec": float(turn["end_sec"]),
                        "text_redacted": text_after_regex,
                        "pii_found_flag": pii_found_flag,
                        "pii_entities": sanitized_entities,
                        "pii_entity_counts": {k: int(v) for k, v in pii_entity_counts.items()},
                        "pii_residual_risk_flag": bool(residual_flag),
                        "redaction_method": redaction_method,
                        "redaction_version": redaction_version,
                        "run_id": run_id,
                        "updated_at": process_ts,
                    }
                )

            redacted_rows.extend(call_output_rows)
            if call_status == "SKIPPED":
                skipped_call_ids.append(call_id)
            else:
                successful_call_ids.append(call_id)

            status_records.append(
                {
                    "call_id": call_id,
                    "stage_name": STAGE_NAME,
                    "status": call_status,
                    "error_message": None,
                    "run_id": run_id,
                    "updated_at": process_ts,
                }
            )
        except Exception as exc:
            status_records.append(
                {
                    "call_id": call_id,
                    "stage_name": STAGE_NAME,
                    "status": "FAILED",
                    "error_message": _truncate_error(exc),
                    "run_id": run_id,
                    "updated_at": process_ts,
                }
            )

    ops_success_count = len(successful_call_ids)
    ops_skipped_count = len(skipped_call_ids)
    ops_failed_count = sum(1 for row in status_records if row["status"] == "FAILED")

    if redacted_rows:
        schema_def = T.StructType(
            [
                T.StructField("call_id", T.StringType(), False),
                T.StructField("turn_id", T.StringType(), False),
                T.StructField("role", T.StringType(), False),
                T.StructField("start_sec", T.DoubleType(), False),
                T.StructField("end_sec", T.DoubleType(), False),
                T.StructField("text_redacted", T.StringType(), False),
                T.StructField("pii_found_flag", T.BooleanType(), False),
                T.StructField(
                    "pii_entities",
                    T.ArrayType(
                        T.StructType(
                            [
                                T.StructField("entity_type", T.StringType(), False),
                                T.StructField("start", T.IntegerType(), False),
                                T.StructField("end", T.IntegerType(), False),
                                T.StructField("source", T.StringType(), False),
                            ]
                        )
                    ),
                    False,
                ),
                T.StructField("pii_entity_counts", T.MapType(T.StringType(), T.IntegerType()), False),
                T.StructField("pii_residual_risk_flag", T.BooleanType(), False),
                T.StructField("redaction_method", T.StringType(), False),
                T.StructField("redaction_version", T.StringType(), False),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )
        out_df = spark.createDataFrame(redacted_rows, schema=schema_def)
        successful_or_skipped_ids = sorted(set(successful_call_ids + skipped_call_ids))
        if successful_or_skipped_ids:
            ids_sql = ", ".join(_sql_literal(call_id) for call_id in successful_or_skipped_ids)
            spark.sql(f"DELETE FROM {redacted_table} WHERE call_id IN ({ids_sql})")
        out_df.write.format("delta").mode("append").saveAsTable(redacted_table)
        rows_written = len(redacted_rows)

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
        status_df.createOrReplaceTempView("tmp_foundation_06_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file_status_table} AS t
            USING tmp_foundation_06_status AS s
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

    if eligible_count > 0 and (ops_success_count + ops_skipped_count) == 0:
        final_status = "FAILED"
        error_summary = "Zero call_ids completed in redact_pii while eligible calls existed."
        raise RuntimeError(error_summary)

    if ops_failed_count > 0:
        error_summary = (
            f"{ops_failed_count} call(s) failed in {STAGE_NAME}; "
            f"see {ops_file_status_table} for details."
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
        f"[{STAGE_NAME}] eligible={eligible_count} success_calls={ops_success_count} "
        f"skipped_calls={ops_skipped_count} failed_calls={ops_failed_count} "
        f"rows_written={rows_written} status={final_status}"
    )
