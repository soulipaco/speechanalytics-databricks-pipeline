# Databricks notebook source
# MAGIC %md
# MAGIC # INSIGHTS_01_COMPUTE_CONVERSATION_METRICS
# MAGIC
# MAGIC **Purpose**
# MAGIC - Compute deterministic, non-LLM conversation metrics at call grain.
# MAGIC
# MAGIC **Inputs**
# MAGIC - Parameters: `catalog`, `schema`, `run_id`, `run_mode`, `max_files_per_run`, `translation_enabled`, `metrics_version`, `silence_gap_threshold_sec`, `overlap_handling_policy`
# MAGIC - Preferred table: `<catalog>.<schema>.gold_turns_translated` when translation is enabled and table exists
# MAGIC - Fallback table: `<catalog>.<schema>.gold_turns_redacted`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.<schema>.gold_conversation_metrics`
# MAGIC - `<catalog>.<schema>.ops_file_status` (stage: `insights_01_compute_conversation_metrics`)
# MAGIC - `<catalog>.<schema>.ops_pipeline_runs` (workflow: `insights`)
# MAGIC
# MAGIC **Key rules**
# MAGIC - Supports `sample | incremental | full`.
# MAGIC - Incremental/sample eligibility: call_ids missing this stage or previously FAILED.
# MAGIC - Idempotent write per successful call_id + metrics_version (delete then append).
# MAGIC - Per-call failures are isolated; stage fails only if eligible calls exist and zero succeed.

# COMMAND ----------

import json
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pyspark.sql import functions as F
from pyspark.sql import types as T


WORKFLOW_NAME = "insights"
STAGE_NAME = "insights_01_compute_conversation_metrics"
ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
DEFAULT_METRICS_VERSION = "v1"
DEFAULT_SILENCE_GAP_THRESHOLD_SEC = 0.8
DEFAULT_OVERLAP_HANDLING_POLICY = "ignore_overlap_v1"
ALLOWED_OVERLAP_HANDLING_POLICIES = {DEFAULT_OVERLAP_HANDLING_POLICY}


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


def _normalize_role(role_value: Optional[object]) -> str:
    role = str(role_value or "").strip().lower()
    if role == "agent":
        return "Agent"
    if role == "customer":
        return "Customer"
    return "Unknown"


def _ensure_tables(
    metrics_table: str, ops_file_status_table: str, ops_pipeline_runs_table: str
) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {metrics_table} (
          call_id STRING,
          total_duration_sec DOUBLE,
          agent_talk_time_sec DOUBLE,
          customer_talk_time_sec DOUBLE,
          unknown_talk_time_sec DOUBLE,
          silence_time_sec DOUBLE,
          overlap_time_sec DOUBLE,
          turn_count_total BIGINT,
          turn_count_agent BIGINT,
          turn_count_customer BIGINT,
          turn_count_unknown BIGINT,
          num_turns_agent BIGINT,
          num_turns_customer BIGINT,
          avg_turn_length_sec DOUBLE,
          avg_turn_duration_agent_sec DOUBLE,
          avg_turn_duration_customer_sec DOUBLE,
          first_turn_ts_sec DOUBLE,
          last_turn_ts_sec DOUBLE,
          overlap_warning_flag BOOLEAN,
          metrics_version STRING,
          run_id STRING,
          source_turns_table STRING,
          overlap_handling_policy STRING,
          silence_gap_threshold_sec DOUBLE,
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


def _compute_call_metrics(
    call_turns: List[object],
    silence_gap_threshold_sec: float,
    overlap_handling_policy: str,
) -> Dict[str, object]:
    if not call_turns:
        raise ValueError("No turns found for call_id.")

    if overlap_handling_policy not in ALLOWED_OVERLAP_HANDLING_POLICIES:
        raise ValueError(
            f"Unsupported `overlap_handling_policy`: {overlap_handling_policy!r}. "
            f"Allowed: {sorted(ALLOWED_OVERLAP_HANDLING_POLICIES)}"
        )

    normalized_turns: List[Tuple[float, float, str, str]] = []
    for turn in call_turns:
        start_raw = turn["start_sec"]
        end_raw = turn["end_sec"]
        if start_raw is None or end_raw is None:
            raise ValueError("Turn start/end is null.")
        start_sec = float(start_raw)
        end_sec = float(end_raw)
        if end_sec <= start_sec:
            raise ValueError(
                f"Invalid turn timing: start_sec={start_sec}, end_sec={end_sec}."
            )
        normalized_turns.append(
            (
                start_sec,
                end_sec,
                _normalize_role(turn["role"]),
                str(turn["turn_id"] or ""),
            )
        )

    normalized_turns.sort(key=lambda row: (row[0], row[1], row[3]))
    starts = [row[0] for row in normalized_turns]
    ends = [row[1] for row in normalized_turns]
    durations = [row[1] - row[0] for row in normalized_turns]

    first_turn_ts_sec = min(starts)
    last_turn_ts_sec = max(ends)
    total_duration_sec = last_turn_ts_sec - first_turn_ts_sec
    if total_duration_sec <= 0:
        raise ValueError("Computed total_duration_sec <= 0.")

    talk_time = {"Agent": 0.0, "Customer": 0.0, "Unknown": 0.0}
    turn_counts = {"Agent": 0, "Customer": 0, "Unknown": 0}
    role_durations = {"Agent": [], "Customer": [], "Unknown": []}
    for start_sec, end_sec, role_bucket, _turn_id in normalized_turns:
        duration_sec = end_sec - start_sec
        talk_time[role_bucket] += duration_sec
        turn_counts[role_bucket] += 1
        role_durations[role_bucket].append(duration_sec)

    silence_time_sec = 0.0
    overlap_time_sec = 0.0
    for index in range(len(normalized_turns) - 1):
        current_end = normalized_turns[index][1]
        next_start = normalized_turns[index + 1][0]
        gap = next_start - current_end
        if gap > silence_gap_threshold_sec:
            silence_time_sec += gap
        if gap < 0:
            overlap_time_sec += abs(gap)

    avg_turn_length_sec = float(sum(durations) / len(durations))
    avg_turn_duration_agent_sec = (
        float(sum(role_durations["Agent"]) / len(role_durations["Agent"]))
        if role_durations["Agent"]
        else 0.0
    )
    avg_turn_duration_customer_sec = (
        float(sum(role_durations["Customer"]) / len(role_durations["Customer"]))
        if role_durations["Customer"]
        else 0.0
    )
    overlap_warning_flag = (
        talk_time["Agent"] + talk_time["Customer"]
    ) > (total_duration_sec * 1.2)

    numeric_checks = [
        ("total_duration_sec", total_duration_sec),
        ("agent_talk_time_sec", talk_time["Agent"]),
        ("customer_talk_time_sec", talk_time["Customer"]),
        ("unknown_talk_time_sec", talk_time["Unknown"]),
        ("silence_time_sec", silence_time_sec),
        ("overlap_time_sec", overlap_time_sec),
        ("avg_turn_length_sec", avg_turn_length_sec),
        ("avg_turn_duration_agent_sec", avg_turn_duration_agent_sec),
        ("avg_turn_duration_customer_sec", avg_turn_duration_customer_sec),
    ]
    for metric_name, metric_value in numeric_checks:
        if metric_value < 0:
            raise ValueError(f"{metric_name} is negative: {metric_value}")

    return {
        "total_duration_sec": total_duration_sec,
        "agent_talk_time_sec": talk_time["Agent"],
        "customer_talk_time_sec": talk_time["Customer"],
        "unknown_talk_time_sec": talk_time["Unknown"],
        "silence_time_sec": silence_time_sec,
        "overlap_time_sec": overlap_time_sec,
        "turn_count_total": int(len(normalized_turns)),
        "turn_count_agent": int(turn_counts["Agent"]),
        "turn_count_customer": int(turn_counts["Customer"]),
        "turn_count_unknown": int(turn_counts["Unknown"]),
        "num_turns_agent": int(turn_counts["Agent"]),
        "num_turns_customer": int(turn_counts["Customer"]),
        "avg_turn_length_sec": avg_turn_length_sec,
        "avg_turn_duration_agent_sec": avg_turn_duration_agent_sec,
        "avg_turn_duration_customer_sec": avg_turn_duration_customer_sec,
        "first_turn_ts_sec": first_turn_ts_sec,
        "last_turn_ts_sec": last_turn_ts_sec,
        "overlap_warning_flag": bool(overlap_warning_flag),
    }


if _is_databricks():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("translation_enabled", "true")
    dbutils.widgets.text("metrics_version", DEFAULT_METRICS_VERSION)
    dbutils.widgets.text(
        "silence_gap_threshold_sec", str(DEFAULT_SILENCE_GAP_THRESHOLD_SEC)
    )
    dbutils.widgets.text("overlap_handling_policy", DEFAULT_OVERLAP_HANDLING_POLICY)

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    translation_enabled_raw = dbutils.widgets.get("translation_enabled").strip()
    metrics_version = (
        dbutils.widgets.get("metrics_version").strip() or DEFAULT_METRICS_VERSION
    )
    silence_gap_threshold_raw = dbutils.widgets.get("silence_gap_threshold_sec").strip()
    overlap_handling_policy = (
        dbutils.widgets.get("overlap_handling_policy").strip()
        or DEFAULT_OVERLAP_HANDLING_POLICY
    )
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    translation_enabled_raw = os.getenv("TRANSLATION_ENABLED", "true").strip()
    metrics_version = (
        os.getenv("METRICS_VERSION", DEFAULT_METRICS_VERSION).strip()
        or DEFAULT_METRICS_VERSION
    )
    silence_gap_threshold_raw = os.getenv(
        "SILENCE_GAP_THRESHOLD_SEC", str(DEFAULT_SILENCE_GAP_THRESHOLD_SEC)
    ).strip()
    overlap_handling_policy = (
        os.getenv("OVERLAP_HANDLING_POLICY", DEFAULT_OVERLAP_HANDLING_POLICY).strip()
        or DEFAULT_OVERLAP_HANDLING_POLICY
    )

catalog = _validate_identifier("catalog", catalog)
schema = _validate_identifier("schema", schema)
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid `run_mode`: {run_mode!r}. Allowed: {sorted(ALLOWED_RUN_MODES)}")
if overlap_handling_policy not in ALLOWED_OVERLAP_HANDLING_POLICIES:
    raise ValueError(
        f"Invalid `overlap_handling_policy`: {overlap_handling_policy!r}. "
        f"Allowed: {sorted(ALLOWED_OVERLAP_HANDLING_POLICIES)}"
    )
if not metrics_version:
    raise ValueError("Parameter `metrics_version` must not be empty.")

max_files_per_run: Optional[int] = None
if max_files_per_run_raw:
    max_files_per_run = int(max_files_per_run_raw)
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError("In sample mode, `max_files_per_run` must be provided as an integer > 0.")

translation_enabled = _parse_bool(
    "translation_enabled", translation_enabled_raw, default=True
)
silence_gap_threshold_sec = float(
    silence_gap_threshold_raw or DEFAULT_SILENCE_GAP_THRESHOLD_SEC
)
if silence_gap_threshold_sec < 0:
    raise ValueError("`silence_gap_threshold_sec` must be >= 0.")

translated_turns_table = _fq_table(catalog, schema, "gold_turns_translated")
redacted_turns_table = _fq_table(catalog, schema, "gold_turns_redacted")
metrics_table = _fq_table(catalog, schema, "gold_conversation_metrics")
ops_file_status_table = _fq_table(catalog, schema, "ops_file_status")
ops_pipeline_runs_table = _fq_table(catalog, schema, "ops_pipeline_runs")

if translation_enabled and _table_exists(catalog, schema, "gold_turns_translated"):
    source_table_name = "gold_turns_translated"
    source_turns_table = translated_turns_table
else:
    source_table_name = "gold_turns_redacted"
    source_turns_table = redacted_turns_table

params_snapshot = {
    "catalog": catalog,
    "schema": schema,
    "run_id": run_id,
    "run_mode": run_mode,
    "max_files_per_run": max_files_per_run,
    "translation_enabled": translation_enabled,
    "metrics_version": metrics_version,
    "silence_gap_threshold_sec": silence_gap_threshold_sec,
    "overlap_handling_policy": overlap_handling_policy,
    "source_table_name": source_table_name,
}
parameters_json = json.dumps(params_snapshot, sort_keys=True)

print(
    f"[{STAGE_NAME}] Starting with run_id={run_id}, run_mode={run_mode}, "
    f"source_table={source_table_name}, metrics_version={metrics_version}"
)

_ensure_tables(metrics_table, ops_file_status_table, ops_pipeline_runs_table)
_upsert_pipeline_run_running(ops_pipeline_runs_table, run_id, parameters_json)

eligible_count = 0
ops_success_count = 0
ops_failed_count = 0
overlap_warning_count = 0
rows_written = 0
final_status = "SUCCESS"
error_summary: Optional[str] = None

try:
    if not _table_exists(catalog, schema, source_table_name):
        if source_table_name == "gold_turns_redacted":
            raise RuntimeError(f"Required input table is missing: {redacted_turns_table}")
        raise RuntimeError(
            "Preferred input table is missing and no fallback is available: "
            f"{source_turns_table}"
        )

    input_df = spark.table(source_turns_table)
    required_columns = {"call_id", "role", "start_sec", "end_sec"}
    missing_columns = sorted(required_columns - set(input_df.columns))
    if missing_columns:
        raise RuntimeError(
            f"Input table {source_turns_table} is missing required columns: {missing_columns}"
        )

    turns_df = input_df.select(
        "call_id",
        F.col("role").alias("role"),
        F.col("start_sec").cast("double").alias("start_sec"),
        F.col("end_sec").cast("double").alias("end_sec"),
        (
            F.col("turn_id").cast("string")
            if "turn_id" in input_df.columns
            else F.lit(None).cast("string")
        ).alias("turn_id"),
    ).where("call_id IS NOT NULL")

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

    status_records: List[Dict[str, object]] = []
    metrics_rows: List[Dict[str, object]] = []
    process_ts = datetime.utcnow()

    for call_id in eligible_calls:
        try:
            call_turns = (
                turns_df.where(F.col("call_id") == call_id)
                .orderBy("start_sec", "end_sec", "turn_id")
                .collect()
            )

            computed = _compute_call_metrics(
                call_turns=call_turns,
                silence_gap_threshold_sec=silence_gap_threshold_sec,
                overlap_handling_policy=overlap_handling_policy,
            )
            if bool(computed["overlap_warning_flag"]):
                overlap_warning_count += 1

            metrics_rows.append(
                {
                    "call_id": str(call_id),
                    "total_duration_sec": float(computed["total_duration_sec"]),
                    "agent_talk_time_sec": float(computed["agent_talk_time_sec"]),
                    "customer_talk_time_sec": float(computed["customer_talk_time_sec"]),
                    "unknown_talk_time_sec": float(computed["unknown_talk_time_sec"]),
                    "silence_time_sec": float(computed["silence_time_sec"]),
                    "overlap_time_sec": float(computed["overlap_time_sec"]),
                    "turn_count_total": int(computed["turn_count_total"]),
                    "turn_count_agent": int(computed["turn_count_agent"]),
                    "turn_count_customer": int(computed["turn_count_customer"]),
                    "turn_count_unknown": int(computed["turn_count_unknown"]),
                    "num_turns_agent": int(computed["num_turns_agent"]),
                    "num_turns_customer": int(computed["num_turns_customer"]),
                    "avg_turn_length_sec": float(computed["avg_turn_length_sec"]),
                    "avg_turn_duration_agent_sec": float(
                        computed["avg_turn_duration_agent_sec"]
                    ),
                    "avg_turn_duration_customer_sec": float(
                        computed["avg_turn_duration_customer_sec"]
                    ),
                    "first_turn_ts_sec": float(computed["first_turn_ts_sec"]),
                    "last_turn_ts_sec": float(computed["last_turn_ts_sec"]),
                    "overlap_warning_flag": bool(computed["overlap_warning_flag"]),
                    "metrics_version": metrics_version,
                    "run_id": run_id,
                    "source_turns_table": source_table_name,
                    "overlap_handling_policy": overlap_handling_policy,
                    "silence_gap_threshold_sec": silence_gap_threshold_sec,
                    "updated_at": process_ts,
                }
            )
            status_records.append(
                {
                    "call_id": str(call_id),
                    "stage_name": STAGE_NAME,
                    "status": "SUCCESS",
                    "error_message": None,
                    "run_id": run_id,
                    "updated_at": process_ts,
                }
            )
        except Exception as exc:
            status_records.append(
                {
                    "call_id": str(call_id),
                    "stage_name": STAGE_NAME,
                    "status": "FAILED",
                    "error_message": _truncate_error(exc),
                    "run_id": run_id,
                    "updated_at": process_ts,
                }
            )

    ops_success_count = len(metrics_rows)
    ops_failed_count = sum(1 for row in status_records if row["status"] == "FAILED")

    if metrics_rows:
        metrics_schema = T.StructType(
            [
                T.StructField("call_id", T.StringType(), False),
                T.StructField("total_duration_sec", T.DoubleType(), False),
                T.StructField("agent_talk_time_sec", T.DoubleType(), False),
                T.StructField("customer_talk_time_sec", T.DoubleType(), False),
                T.StructField("unknown_talk_time_sec", T.DoubleType(), False),
                T.StructField("silence_time_sec", T.DoubleType(), False),
                T.StructField("overlap_time_sec", T.DoubleType(), False),
                T.StructField("turn_count_total", T.LongType(), False),
                T.StructField("turn_count_agent", T.LongType(), False),
                T.StructField("turn_count_customer", T.LongType(), False),
                T.StructField("turn_count_unknown", T.LongType(), False),
                T.StructField("num_turns_agent", T.LongType(), False),
                T.StructField("num_turns_customer", T.LongType(), False),
                T.StructField("avg_turn_length_sec", T.DoubleType(), False),
                T.StructField("avg_turn_duration_agent_sec", T.DoubleType(), False),
                T.StructField("avg_turn_duration_customer_sec", T.DoubleType(), False),
                T.StructField("first_turn_ts_sec", T.DoubleType(), False),
                T.StructField("last_turn_ts_sec", T.DoubleType(), False),
                T.StructField("overlap_warning_flag", T.BooleanType(), False),
                T.StructField("metrics_version", T.StringType(), False),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("source_turns_table", T.StringType(), False),
                T.StructField("overlap_handling_policy", T.StringType(), False),
                T.StructField("silence_gap_threshold_sec", T.DoubleType(), False),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )
        out_df = spark.createDataFrame(metrics_rows, schema=metrics_schema)
        successful_call_ids = sorted({row["call_id"] for row in metrics_rows})
        if successful_call_ids:
            call_ids_sql = ", ".join(_sql_literal(call_id) for call_id in successful_call_ids)
            spark.sql(
                f"""
                DELETE FROM {metrics_table}
                WHERE call_id IN ({call_ids_sql})
                  AND metrics_version = {_sql_literal(metrics_version)}
                """
            )
        out_df.write.format("delta").mode("append").saveAsTable(metrics_table)
        rows_written = len(metrics_rows)

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
        status_df.createOrReplaceTempView("tmp_insights_01_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file_status_table} AS t
            USING tmp_insights_01_status AS s
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

    if eligible_count > 0 and ops_success_count == 0:
        final_status = "FAILED"
        error_summary = (
            "Zero call_ids completed in insights_01_compute_conversation_metrics "
            "while eligible calls existed."
        )
        raise RuntimeError(error_summary)

    if ops_failed_count > 0:
        final_status = "WARN"
        error_summary = (
            f"{ops_failed_count} call(s) failed in {STAGE_NAME}; "
            f"see {ops_file_status_table} for details."
        )
    elif overlap_warning_count > 0:
        final_status = "WARN"
        error_summary = (
            f"{overlap_warning_count} call(s) exceeded overlap warning threshold "
            "(agent_talk_time_sec + customer_talk_time_sec > total_duration_sec * 1.2)."
        )
    elif eligible_count == 0:
        final_status = "SUCCESS"
        error_summary = f"No eligible calls for stage {STAGE_NAME}."

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
        success_count=ops_success_count,
        failed_count=ops_failed_count,
        error_summary=error_summary,
    )
    print(
        f"[{STAGE_NAME}] eligible={eligible_count} success_calls={ops_success_count} "
        f"failed_calls={ops_failed_count} overlap_warnings={overlap_warning_count} "
        f"rows_written={rows_written} status={final_status}"
    )
