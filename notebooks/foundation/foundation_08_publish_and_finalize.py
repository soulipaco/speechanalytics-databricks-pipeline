# Databricks notebook source
# MAGIC %md
# MAGIC # FOUNDATION_08_PUBLISH_AND_FINALIZE
# MAGIC
# MAGIC **Purpose**
# MAGIC - Finalize Foundation outputs by marking completed calls as `PROCESSED` and writing run summary/status.
# MAGIC
# MAGIC **Inputs**
# MAGIC - Parameters: `catalog`, `schema`, `run_id`, `run_mode`, `max_files_per_run`, `enable_translation`, `foundation_complete_policy`
# MAGIC - Tables: `<catalog>.<schema>.bronze_audio_files`, `<catalog>.<schema>.ops_file_status`, `<catalog>.<schema>.gold_turns_redacted`, optional `<catalog>.<schema>.gold_turns_translated`
# MAGIC
# MAGIC **Outputs**
# MAGIC - Updates `<catalog>.<schema>.bronze_audio_files.status`
# MAGIC - Updates `<catalog>.<schema>.ops_file_status` (stage: `publish_and_finalize`)
# MAGIC - Updates `<catalog>.<schema>.ops_pipeline_runs`
# MAGIC
# MAGIC **Key rules**
# MAGIC - Completion policy:
# MAGIC   - `redacted_required`: align + redact + (translate SUCCESS/SKIPPED when translation enabled)
# MAGIC   - `translation_optional`: align + redact only
# MAGIC - Safe re-run: deterministic status update and idempotent manifest overwrite to `PROCESSED` for completed call_ids.
# MAGIC - Logs only counts and identifiers.

# COMMAND ----------

import json
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Set

from pyspark.sql import functions as F
from pyspark.sql import types as T


WORKFLOW_NAME = "foundation"
STAGE_NAME = "publish_and_finalize"
ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
ALLOWED_COMPLETE_POLICIES = {"redacted_required", "translation_optional"}


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


def _ensure_ops_tables(
    ops_file_status_table: str, ops_pipeline_runs_table: str
) -> None:
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


if _is_databricks():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("enable_translation", "true")
    dbutils.widgets.text("foundation_complete_policy", "redacted_required")

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    enable_translation_raw = dbutils.widgets.get("enable_translation").strip()
    foundation_complete_policy = dbutils.widgets.get("foundation_complete_policy").strip().lower()
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    enable_translation_raw = os.getenv("ENABLE_TRANSLATION", "true").strip()
    foundation_complete_policy = os.getenv(
        "FOUNDATION_COMPLETE_POLICY", "redacted_required"
    ).strip().lower()

catalog = _validate_identifier("catalog", catalog)
schema = _validate_identifier("schema", schema)
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid `run_mode`: {run_mode!r}. Allowed: {sorted(ALLOWED_RUN_MODES)}")
if foundation_complete_policy not in ALLOWED_COMPLETE_POLICIES:
    raise ValueError(
        f"Invalid `foundation_complete_policy`: {foundation_complete_policy!r}. "
        f"Allowed: {sorted(ALLOWED_COMPLETE_POLICIES)}"
    )

max_files_per_run: Optional[int] = None
if max_files_per_run_raw:
    max_files_per_run = int(max_files_per_run_raw)
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError(
        "In sample mode, `max_files_per_run` must be provided as an integer > 0."
    )

enable_translation = _parse_bool("enable_translation", enable_translation_raw, default=True)

bronze_table = _fq_table(catalog, schema, "bronze_audio_files")
ops_file_status_table = _fq_table(catalog, schema, "ops_file_status")
ops_pipeline_runs_table = _fq_table(catalog, schema, "ops_pipeline_runs")
redacted_table = _fq_table(catalog, schema, "gold_turns_redacted")
translated_table = _fq_table(catalog, schema, "gold_turns_translated")

params_snapshot = {
    "catalog": catalog,
    "schema": schema,
    "run_id": run_id,
    "run_mode": run_mode,
    "max_files_per_run": max_files_per_run,
    "enable_translation": enable_translation,
    "foundation_complete_policy": foundation_complete_policy,
}
parameters_json = json.dumps(params_snapshot, sort_keys=True)

print(
    f"[{STAGE_NAME}] Starting with run_id={run_id}, run_mode={run_mode}, "
    f"policy={foundation_complete_policy}"
)

_ensure_ops_tables(ops_file_status_table, ops_pipeline_runs_table)
_upsert_pipeline_run_running(ops_pipeline_runs_table, run_id, parameters_json)

eligible_count = 0
completed_count = 0
incomplete_count = 0
manifest_updated_count = 0
qc_completed_left_new = 0
stage_counts: Dict[str, Dict[str, int]] = {}
final_status = "SUCCESS"
error_summary: Optional[str] = None

try:
    if not _table_exists(catalog, schema, "bronze_audio_files"):
        raise RuntimeError(f"Required input table is missing: {bronze_table}")
    if not _table_exists(catalog, schema, "gold_turns_redacted"):
        raise RuntimeError(f"Required input table is missing: {redacted_table}")
    if enable_translation and not _table_exists(catalog, schema, "gold_turns_translated"):
        raise RuntimeError(
            f"`enable_translation=true` but required input is missing: {translated_table}"
        )

    bronze_df = (
        spark.table(bronze_table)
        .select("call_id", F.col("status").alias("bronze_status"))
        .where("call_id IS NOT NULL")
    )
    finalize_stage_df = (
        spark.table(ops_file_status_table)
        .where(F.col("stage_name") == STAGE_NAME)
        .select("call_id", F.col("status").alias("finalize_status"))
    )

    eligible_df = bronze_df.join(finalize_stage_df, on="call_id", how="left")
    if run_mode in {"sample", "incremental"}:
        eligible_df = eligible_df.where(
            F.col("finalize_status").isNull()
            | (F.upper(F.col("finalize_status")) == "FAILED")
            | (F.upper(F.col("bronze_status")).isin("NEW", "FAILED"))
        )
    eligible_df = eligible_df.orderBy("call_id")
    if run_mode == "sample" and max_files_per_run is not None:
        eligible_df = eligible_df.limit(max_files_per_run)

    eligible_rows = eligible_df.select("call_id", "bronze_status").collect()
    eligible_call_ids = [str(r["call_id"]) for r in eligible_rows]
    eligible_count = len(eligible_call_ids)
    print(f"[{STAGE_NAME}] Eligible calls: {eligible_count}")

    redacted_call_set: Set[str] = {
        str(r["call_id"])
        for r in spark.table(redacted_table).select("call_id").distinct().collect()
    }
    translated_call_set: Set[str] = set()
    if enable_translation:
        translated_call_set = {
            str(r["call_id"])
            for r in spark.table(translated_table).select("call_id").distinct().collect()
        }

    tracked_stages = ["align_turns", "redact_pii", "translate_turns"]
    ops_rows = (
        spark.table(ops_file_status_table)
        .where(F.col("stage_name").isin(tracked_stages))
        .select("call_id", "stage_name", "status")
        .collect()
    )
    stage_status_map: Dict[str, Dict[str, str]] = {}
    for row in ops_rows:
        call_id = str(row["call_id"])
        stage_name = str(row["stage_name"])
        status = str(row["status"] or "").upper()
        stage_status_map.setdefault(call_id, {})[stage_name] = status

    for stage_name in tracked_stages:
        success = 0
        failed = 0
        for call_id in eligible_call_ids:
            value = stage_status_map.get(call_id, {}).get(stage_name, "")
            if value == "SUCCESS" or (stage_name == "translate_turns" and value == "SKIPPED"):
                success += 1
            else:
                failed += 1
        stage_counts[stage_name] = {"success_like": success, "not_success_like": failed}

    status_records: List[Dict[str, object]] = []
    completed_call_ids: List[str] = []
    incomplete_call_ids: List[str] = []
    process_ts = datetime.utcnow()

    for call_id in eligible_call_ids:
        reasons: List[str] = []
        stage_map = stage_status_map.get(call_id, {})

        if stage_map.get("align_turns") != "SUCCESS":
            reasons.append("align_turns not SUCCESS")
        if stage_map.get("redact_pii") != "SUCCESS":
            reasons.append("redact_pii not SUCCESS")
        if call_id not in redacted_call_set:
            reasons.append("gold_turns_redacted missing")

        if foundation_complete_policy == "redacted_required" and enable_translation:
            if stage_map.get("translate_turns") not in {"SUCCESS", "SKIPPED"}:
                reasons.append("translate_turns not SUCCESS/SKIPPED")
            if call_id not in translated_call_set:
                reasons.append("gold_turns_translated missing")

        if reasons:
            incomplete_call_ids.append(call_id)
            status_records.append(
                {
                    "call_id": call_id,
                    "stage_name": STAGE_NAME,
                    "status": "FAILED",
                    "error_message": _truncate_error("; ".join(reasons), 900),
                    "run_id": run_id,
                    "updated_at": process_ts,
                }
            )
        else:
            completed_call_ids.append(call_id)
            status_records.append(
                {
                    "call_id": call_id,
                    "stage_name": STAGE_NAME,
                    "status": "SUCCESS",
                    "error_message": None,
                    "run_id": run_id,
                    "updated_at": process_ts,
                }
            )

    completed_count = len(completed_call_ids)
    incomplete_count = len(incomplete_call_ids)

    if completed_call_ids:
        completed_schema = T.StructType([T.StructField("call_id", T.StringType(), False)])
        completed_df = spark.createDataFrame(
            [{"call_id": call_id} for call_id in sorted(set(completed_call_ids))],
            schema=completed_schema,
        )
        completed_df.createOrReplaceTempView("tmp_foundation_08_completed_calls")
        spark.sql(
            f"""
            MERGE INTO {bronze_table} AS t
            USING tmp_foundation_08_completed_calls AS s
            ON t.call_id = s.call_id
            WHEN MATCHED THEN UPDATE SET
              t.status = 'PROCESSED',
              t.updated_at = current_timestamp(),
              t.run_id = {_sql_literal(run_id)}
            """
        )

        completed_ids_sql = ", ".join(
            _sql_literal(call_id) for call_id in sorted(set(completed_call_ids))
        )
        manifest_updated_count = spark.sql(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {bronze_table}
            WHERE call_id IN ({completed_ids_sql})
              AND UPPER(status) = 'PROCESSED'
            """
        ).collect()[0]["cnt"]

        qc_completed_left_new = spark.sql(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {bronze_table}
            WHERE call_id IN ({completed_ids_sql})
              AND UPPER(status) = 'NEW'
            """
        ).collect()[0]["cnt"]

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
        status_df.createOrReplaceTempView("tmp_foundation_08_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file_status_table} AS t
            USING tmp_foundation_08_status AS s
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

    if qc_completed_left_new > 0:
        final_status = "FAILED"
        error_summary = (
            f"Quality check failed: {qc_completed_left_new} completed call(s) still NEW."
        )
        raise RuntimeError(error_summary)

    if eligible_count == 0:
        final_status = "SUCCESS"
        error_summary = "No eligible calls for publish_and_finalize."
    elif incomplete_count > 0:
        final_status = "WARN"
        error_summary = (
            f"{incomplete_count} of {eligible_count} eligible call(s) not foundation-complete."
        )
    else:
        final_status = "SUCCESS"
        error_summary = None

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
        success_count=completed_count,
        failed_count=incomplete_count,
        error_summary=error_summary,
    )
    print(
        f"[{STAGE_NAME}] objects_updated=bronze_audio_files,ops_file_status,ops_pipeline_runs"
    )
    print(
        f"[{STAGE_NAME}] quality_checks="
        f"eligible:{eligible_count},completed:{completed_count},incomplete:{incomplete_count},"
        f"completed_left_new:{qc_completed_left_new}"
    )
    print(
        f"[{STAGE_NAME}] stage_counts="
        + json.dumps(stage_counts, sort_keys=True)
    )
    print(f"[{STAGE_NAME}] final_status={final_status}")
