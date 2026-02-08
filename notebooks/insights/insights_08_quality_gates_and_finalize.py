# Databricks notebook source
# MAGIC %md
# MAGIC # INSIGHTS_08_QUALITY_GATES_AND_FINALIZE
# MAGIC
# MAGIC Finalize Insights workflow by applying quality gates and writing terminal ops statuses.

# COMMAND ----------

import json
import os
import re
from collections import defaultdict
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T


WORKFLOW_NAME = "insights"
STAGE_NAME = "insights_08_quality_gates_and_finalize"

ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
ALLOWED_RESOLUTION = {"Resolved", "Not resolved"}
ALLOWED_EFFORT = {"High", "Low"}
ALLOWED_SENTIMENT = {"Positive", "Neutral", "Negative"}

DEFAULT_METRICS_VERSION = "v1"
DEFAULT_CONSOLIDATION_VERSION = "v1"
DEFAULT_FINALIZE_POLICY = "standard"


def _is_dbx():
    return "dbutils" in globals()


def _lit(v):
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


def _truncate(msg, n=1000):
    return (str(msg).strip() or "Unknown error")[:n]


def _valid_ident(name, value):
    if not value or not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(f"Invalid `{name}`: {value!r}")
    return value


def _parse_bool(name, raw, default=False):
    token = (raw or "").strip().lower()
    if not token:
        return default
    if token in {"1", "true", "t", "yes", "y"}:
        return True
    if token in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"Invalid boolean for `{name}`: {raw!r}")


def _safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value):
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _s(value):
    return str(value or "").strip()


def _fq(catalog, schema, table):
    return f"`{catalog}`.`{schema}`.`{table}`"


def _exists(catalog, schema, table):
    return bool(spark.catalog.tableExists(f"{catalog}.{schema}.{table}"))


def _ensure_ops_tables(ops_file, ops_runs):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ops_file} (
          call_id STRING,
          stage_name STRING,
          status STRING,
          error_message STRING,
          run_id STRING,
          updated_at TIMESTAMP
        ) USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ops_runs} (
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
        ) USING DELTA
        """
    )


def _upsert_run_running(ops_runs, run_id, params_json):
    spark.sql(
        f"""
        MERGE INTO {ops_runs} AS t
        USING (
          SELECT
            {_lit(run_id)} AS run_id,
            {_lit(WORKFLOW_NAME)} AS workflow_name,
            current_timestamp() AS started_at,
            CAST(NULL AS TIMESTAMP) AS ended_at,
            'RUNNING' AS status,
            {_lit(params_json)} AS parameters,
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
          run_id, workflow_name, started_at, ended_at, status, trigger_type, parameters,
          error_summary, total_files, success_count, failed_count, updated_at
        ) VALUES (
          s.run_id, s.workflow_name, s.started_at, s.ended_at, s.status, NULL, s.parameters,
          s.error_summary, s.total_files, s.success_count, s.failed_count, s.updated_at
        )
        """
    )


def _upsert_run_final(ops_runs, run_id, status, total_files, success_count, failed_count, error_summary):
    spark.sql(
        f"""
        MERGE INTO {ops_runs} AS t
        USING (
          SELECT
            {_lit(run_id)} AS run_id,
            {_lit(WORKFLOW_NAME)} AS workflow_name,
            {_lit(status)} AS status,
            CAST({int(total_files)} AS BIGINT) AS total_files,
            CAST({int(success_count)} AS BIGINT) AS success_count,
            CAST({int(failed_count)} AS BIGINT) AS failed_count,
            {_lit(error_summary)} AS error_summary,
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
          run_id, workflow_name, started_at, ended_at, status, trigger_type, parameters,
          error_summary, total_files, success_count, failed_count, updated_at
        ) VALUES (
          s.run_id, s.workflow_name, current_timestamp(), s.ended_at, s.status, NULL, NULL,
          s.error_summary, s.total_files, s.success_count, s.failed_count, s.updated_at
        )
        """
    )


def _load_taxonomy_sets(catalog, schema):
    needed = [
        "dim_contact_driver",
        "dim_issue",
        "dim_intent",
        "dim_emotion_catalog",
    ]
    missing = [t for t in needed if not _exists(catalog, schema, t)]
    if missing:
        return None, missing

    def _table_map_label(table_name, label_col):
        out = defaultdict(set)
        for row in (
            spark.table(_fq(catalog, schema, table_name))
            .where(F.col("active_flag") == F.lit(True))
            .select(
                F.col("taxonomy_version").cast("string").alias("taxonomy_version"),
                F.col(label_col).cast("string").alias("label"),
            )
            .collect()
        ):
            tv = _s(row["taxonomy_version"])
            lbl = _s(row["label"])
            if tv and lbl:
                out[tv].add(lbl)
        return out

    drivers = _table_map_label("dim_contact_driver", "label")
    issues = _table_map_label("dim_issue", "label")
    intents = _table_map_label("dim_intent", "label")

    emotions = defaultdict(set)
    for row in (
        spark.table(_fq(catalog, schema, "dim_emotion_catalog"))
        .where(F.col("active_flag") == F.lit(True))
        .select(
            F.col("taxonomy_version").cast("string").alias("taxonomy_version"),
            F.col("catalog_version").cast("string").alias("catalog_version"),
            F.col("emotion_name").cast("string").alias("emotion_name"),
        )
        .collect()
    ):
        emo = _s(row["emotion_name"])
        tv = _s(row["taxonomy_version"])
        cv = _s(row["catalog_version"])
        if emo and tv:
            emotions[tv].add(emo)
        if emo and cv:
            emotions[cv].add(emo)

    return {
        "driver": drivers,
        "issue": issues,
        "intent": intents,
        "emotion": emotions,
    }, []


def _validate_membership(taxonomy_sets, taxonomy_version, key, label):
    if taxonomy_sets is None:
        return False
    by_version = taxonomy_sets.get(key, {})
    allowed = by_version.get(taxonomy_version, set())
    return bool(label in allowed)


def _is_in_range_01(value):
    if value is None:
        return False
    try:
        v = float(value)
    except Exception:
        return False
    return 0.0 <= v <= 1.0


def _is_int_range(value, lo, hi):
    try:
        v = int(value)
    except Exception:
        return False
    return lo <= v <= hi


def _evaluate_call_quality(
    call_id,
    metrics_rows,
    insights_rows,
    taxonomy_sets,
    taxonomy_sets_available,
    taxonomy_version_filter,
    allow_warn,
):
    critical = []
    warns = []

    if len(metrics_rows) != 1:
        critical.append(
            f"Expected exactly one metrics row for call_id={call_id}; found={len(metrics_rows)}."
        )
    if len(insights_rows) != 1:
        critical.append(
            f"Expected exactly one insights row for call_id={call_id}; found={len(insights_rows)}."
        )

    if critical:
        return "FAILED", critical, warns

    metric = metrics_rows[0]
    insight = insights_rows[0]

    total_duration = _safe_float(metric.get("total_duration_sec"))
    if total_duration is None or total_duration <= 0:
        critical.append("total_duration_sec must be > 0.")

    non_negative_metric_fields = [
        "agent_talk_time_sec",
        "customer_talk_time_sec",
        "unknown_talk_time_sec",
        "silence_time_sec",
        "overlap_time_sec",
        "avg_turn_length_sec",
        "first_turn_ts_sec",
        "last_turn_ts_sec",
        "turn_count_total",
        "turn_count_agent",
        "turn_count_customer",
        "turn_count_unknown",
    ]
    for field in non_negative_metric_fields:
        if field in metric:
            val = _safe_float(metric.get(field))
            if val is not None and val < 0:
                critical.append(f"{field} must be >= 0.")

    required_labels = ["contact_driver_label", "issue_label", "intent_label"]
    for field in required_labels:
        if not _s(insight.get(field)):
            critical.append(f"{field} is required and cannot be empty.")

    if not _s(insight.get("summary_text")):
        critical.append("summary_text is required and cannot be empty.")

    enum_checks = [
        ("resolution", ALLOWED_RESOLUTION),
        ("effort", ALLOWED_EFFORT),
        ("sentiment", ALLOWED_SENTIMENT),
    ]
    for field, allowed in enum_checks:
        val = _s(insight.get(field))
        if val not in allowed:
            critical.append(f"{field} must be one of {sorted(allowed)}.")

    confidence_fields = [
        "contact_driver_confidence",
        "issue_confidence",
        "intent_confidence",
        "resolution_confidence",
        "effort_confidence",
        "sentiment_confidence",
    ]
    confidence_values = []
    for field in confidence_fields:
        val = insight.get(field)
        if not _is_in_range_01(val):
            critical.append(f"{field} must be in [0,1].")
        else:
            confidence_values.append(float(val))

    for score_field in ["agent_love_score_1_10", "brand_love_score_1_10"]:
        if not _is_int_range(insight.get(score_field), 1, 10):
            critical.append(f"{score_field} must be integer in [1,10].")

    taxonomy_version = _s(taxonomy_version_filter) or _s(insight.get("taxonomy_version"))
    if not taxonomy_version:
        critical.append("taxonomy_version is required for taxonomy label validation.")
    elif taxonomy_sets_available:
        label_map = [
            ("contact_driver_label", "driver"),
            ("issue_label", "issue"),
            ("intent_label", "intent"),
            ("customer_emotion_start", "emotion"),
            ("customer_emotion_end", "emotion"),
            ("agent_emotion_start", "emotion"),
            ("agent_emotion_end", "emotion"),
        ]
        for field, key in label_map:
            val = _s(insight.get(field))
            if not val:
                critical.append(f"{field} is required for taxonomy validation.")
            elif not _validate_membership(taxonomy_sets, taxonomy_version, key, val):
                critical.append(
                    f"{field}={val!r} not found in active dim values for taxonomy_version={taxonomy_version!r}."
                )
    else:
        warns.append("taxonomy dim tables unavailable; taxonomy membership checks skipped.")

    rag_used = bool(insight.get("rag_used_flag"))
    if rag_used:
        if not _s(insight.get("rag_backend")):
            warns.append("rag_used_flag=true but rag_backend is empty.")
        rag_top_k = _safe_int(insight.get("rag_top_k"))
        if rag_top_k is None or rag_top_k <= 0:
            warns.append("rag_used_flag=true but rag_top_k is missing/invalid.")

    if confidence_values:
        if min(confidence_values) < 0.4:
            warns.append("One or more confidence values are below 0.40.")

    if critical:
        return "FAILED", critical, warns
    if warns:
        if allow_warn:
            return "WARN", critical, warns
        return "FAILED", critical + warns, []
    return "SUCCESS", critical, warns


if _is_dbx():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("enable_quality_gates", "true")
    dbutils.widgets.text("finalize_policy", DEFAULT_FINALIZE_POLICY)
    dbutils.widgets.text("allow_warn", "true")
    dbutils.widgets.text("metrics_version", DEFAULT_METRICS_VERSION)
    dbutils.widgets.text("consolidation_version", DEFAULT_CONSOLIDATION_VERSION)
    dbutils.widgets.text("taxonomy_version", "")
    dbutils.widgets.text("min_calls_required", "0")

    # Task-doc compatibility aliases.
    dbutils.widgets.text("fail_run_if_quality_checks_fail", "")
    dbutils.widgets.text("quality_version", "v1")
    dbutils.widgets.text("publish_latest_views", "false")

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    enable_quality_gates_raw = dbutils.widgets.get("enable_quality_gates").strip()
    finalize_policy = dbutils.widgets.get("finalize_policy").strip().lower() or DEFAULT_FINALIZE_POLICY
    allow_warn_raw = dbutils.widgets.get("allow_warn").strip()
    metrics_version = dbutils.widgets.get("metrics_version").strip() or DEFAULT_METRICS_VERSION
    consolidation_version = (
        dbutils.widgets.get("consolidation_version").strip() or DEFAULT_CONSOLIDATION_VERSION
    )
    taxonomy_version = dbutils.widgets.get("taxonomy_version").strip()
    min_calls_required_raw = dbutils.widgets.get("min_calls_required").strip()

    fail_run_if_quality_checks_fail_raw = dbutils.widgets.get("fail_run_if_quality_checks_fail").strip()
    quality_version = dbutils.widgets.get("quality_version").strip() or "v1"
    publish_latest_views_raw = dbutils.widgets.get("publish_latest_views").strip()
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    enable_quality_gates_raw = os.getenv("ENABLE_QUALITY_GATES", "true").strip()
    finalize_policy = os.getenv("FINALIZE_POLICY", DEFAULT_FINALIZE_POLICY).strip().lower() or DEFAULT_FINALIZE_POLICY
    allow_warn_raw = os.getenv("ALLOW_WARN", "true").strip()
    metrics_version = os.getenv("METRICS_VERSION", DEFAULT_METRICS_VERSION).strip() or DEFAULT_METRICS_VERSION
    consolidation_version = (
        os.getenv("CONSOLIDATION_VERSION", DEFAULT_CONSOLIDATION_VERSION).strip()
        or DEFAULT_CONSOLIDATION_VERSION
    )
    taxonomy_version = os.getenv("TAXONOMY_VERSION", "").strip()
    min_calls_required_raw = os.getenv("MIN_CALLS_REQUIRED", "0").strip()

    fail_run_if_quality_checks_fail_raw = os.getenv("FAIL_RUN_IF_QUALITY_CHECKS_FAIL", "").strip()
    quality_version = os.getenv("QUALITY_VERSION", "v1").strip() or "v1"
    publish_latest_views_raw = os.getenv("PUBLISH_LATEST_VIEWS", "false").strip()

catalog = _valid_ident("catalog", catalog)
schema = _valid_ident("schema", schema)
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid `run_mode`: {run_mode!r}")
if not consolidation_version:
    raise ValueError("`consolidation_version` must not be empty.")
if not metrics_version:
    raise ValueError("`metrics_version` must not be empty.")

enable_quality_gates = _parse_bool("enable_quality_gates", enable_quality_gates_raw, default=True)
allow_warn = _parse_bool("allow_warn", allow_warn_raw or "true", default=True)
publish_latest_views = _parse_bool("publish_latest_views", publish_latest_views_raw or "false", default=False)
min_calls_required = int(min_calls_required_raw or "0")
if min_calls_required < 0:
    raise ValueError("`min_calls_required` must be >= 0.")

if fail_run_if_quality_checks_fail_raw:
    fail_hard = _parse_bool(
        "fail_run_if_quality_checks_fail", fail_run_if_quality_checks_fail_raw, default=False
    )
    if fail_hard:
        allow_warn = False

max_files_per_run = int(max_files_per_run_raw) if max_files_per_run_raw else None
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError("In sample mode, `max_files_per_run` must be > 0.")

ops_file = _fq(catalog, schema, "ops_file_status")
ops_runs = _fq(catalog, schema, "ops_pipeline_runs")
metrics_table = _fq(catalog, schema, "gold_conversation_metrics")
insights_table = _fq(catalog, schema, "gold_speech_insights")

params_json = json.dumps(
    {
        "catalog": catalog,
        "schema": schema,
        "run_id": run_id,
        "run_mode": run_mode,
        "max_files_per_run": max_files_per_run,
        "enable_quality_gates": enable_quality_gates,
        "finalize_policy": finalize_policy,
        "allow_warn": allow_warn,
        "metrics_version": metrics_version,
        "consolidation_version": consolidation_version,
        "taxonomy_version": taxonomy_version or None,
        "min_calls_required": min_calls_required,
        "quality_version": quality_version,
        "publish_latest_views": publish_latest_views,
    },
    sort_keys=True,
)

print(
    f"[{STAGE_NAME}] start run_id={run_id} mode={run_mode} "
    f"enable_quality_gates={enable_quality_gates} "
    f"metrics_version={metrics_version} consolidation_version={consolidation_version}"
)

_ensure_ops_tables(ops_file, ops_runs)
_upsert_run_running(ops_runs, run_id, params_json)

eligible_count = 0
success_calls = 0
warn_calls = 0
failed_calls = 0
skipped_calls = 0
final_status = "SUCCESS"
error_summary = None

try:
    hard_missing = []
    for tbl in ["ops_file_status", "ops_pipeline_runs", "gold_conversation_metrics", "gold_speech_insights"]:
        if not _exists(catalog, schema, tbl):
            hard_missing.append(f"{catalog}.{schema}.{tbl}")
    if hard_missing:
        raise RuntimeError("Missing required table(s): " + ", ".join(hard_missing))

    metrics_df = spark.table(metrics_table)
    insights_df = spark.table(insights_table)

    for required_col in ["call_id"]:
        if required_col not in metrics_df.columns:
            raise RuntimeError(f"{metrics_table} missing required column `{required_col}`.")
        if required_col not in insights_df.columns:
            raise RuntimeError(f"{insights_table} missing required column `{required_col}`.")
    if "metrics_version" not in metrics_df.columns:
        raise RuntimeError(f"{metrics_table} missing required column `metrics_version`.")

    if "consolidation_version" not in insights_df.columns:
        raise RuntimeError(f"{insights_table} missing required column `consolidation_version`.")

    global_schema_errors = []
    forbidden_text_columns = {
        "chunk_text",
        "chunk_text_redacted",
        "chunk_text_translated",
        "turn_text",
        "turn_text_redacted",
        "turn_text_translated",
        "transcript_text",
        "transcript",
        "raw_transcript",
        "text_redacted",
        "text_translated",
        "text_raw",
    }
    found_forbidden_cols = sorted(set(insights_df.columns).intersection(forbidden_text_columns))
    if found_forbidden_cols:
        global_schema_errors.append(
            "Raw transcript-like columns detected in gold_speech_insights: "
            + ", ".join(found_forbidden_cols)
        )

    metrics_filtered = metrics_df.where(F.col("metrics_version") == metrics_version)
    insights_filtered = insights_df.where(F.col("consolidation_version") == consolidation_version)
    if "metrics_version" in insights_df.columns:
        insights_filtered = insights_filtered.where(F.col("metrics_version") == metrics_version)
    if taxonomy_version and "taxonomy_version" in insights_df.columns:
        insights_filtered = insights_filtered.where(F.col("taxonomy_version") == taxonomy_version)

    base_calls_df = (
        insights_filtered.select(F.col("call_id").cast("string").alias("call_id"))
        .where("call_id IS NOT NULL")
        .distinct()
        .orderBy("call_id")
    )

    stage_df = (
        spark.table(ops_file)
        .where(F.col("stage_name") == STAGE_NAME)
        .select(
            F.col("call_id").cast("string").alias("call_id"),
            F.upper(F.col("status")).alias("stage_status"),
        )
    )

    eligible_df = base_calls_df.join(stage_df, on="call_id", how="left")
    if run_mode in {"sample", "incremental"}:
        eligible_df = eligible_df.where(
            F.col("stage_status").isNull() | (F.col("stage_status") == "FAILED")
        )
    if run_mode == "sample" and max_files_per_run is not None:
        eligible_df = eligible_df.limit(max_files_per_run)

    eligible_calls = [str(r["call_id"]) for r in eligible_df.select("call_id").collect()]
    eligible_count = len(eligible_calls)
    print(f"[{STAGE_NAME}] eligible_calls={eligible_count}")

    if min_calls_required > 0 and eligible_count < min_calls_required:
        global_schema_errors.append(
            f"Eligible call count {eligible_count} is below min_calls_required={min_calls_required}."
        )

    taxonomy_sets = None
    taxonomy_sets_available = False
    taxonomy_missing = []
    if enable_quality_gates:
        taxonomy_sets, taxonomy_missing = _load_taxonomy_sets(catalog, schema)
        taxonomy_sets_available = taxonomy_sets is not None
        if taxonomy_missing:
            msg = "Missing taxonomy dim tables: " + ", ".join(taxonomy_missing)
            if finalize_policy in {"strict", "hard_fail"}:
                global_schema_errors.append(msg)
            else:
                error_summary = msg

    metrics_rows = (
        metrics_filtered.select(*metrics_filtered.columns)
        .where(F.col("call_id").cast("string").isin(eligible_calls))
        .collect()
        if eligible_calls
        else []
    )
    insights_rows = (
        insights_filtered.select(*insights_filtered.columns)
        .where(F.col("call_id").cast("string").isin(eligible_calls))
        .collect()
        if eligible_calls
        else []
    )

    metrics_by_call = defaultdict(list)
    for row in metrics_rows:
        metrics_by_call[_s(row["call_id"])].append(row.asDict(recursive=True))

    insights_by_call = defaultdict(list)
    for row in insights_rows:
        insights_by_call[_s(row["call_id"])].append(row.asDict(recursive=True))

    status_rows = []
    ts = datetime.utcnow()

    for call_id in eligible_calls:
        if not enable_quality_gates:
            skipped_calls += 1
            status_rows.append(
                {
                    "call_id": call_id,
                    "stage_name": STAGE_NAME,
                    "status": "SKIPPED",
                    "error_message": "Quality gates disabled by `enable_quality_gates=false`.",
                    "run_id": run_id,
                    "updated_at": ts,
                }
            )
            continue

        if global_schema_errors:
            failed_calls += 1
            status_rows.append(
                {
                    "call_id": call_id,
                    "stage_name": STAGE_NAME,
                    "status": "FAILED",
                    "error_message": "; ".join(global_schema_errors)[:1000],
                    "run_id": run_id,
                    "updated_at": ts,
                }
            )
            continue

        status, critical, warns = _evaluate_call_quality(
            call_id=call_id,
            metrics_rows=metrics_by_call.get(call_id, []),
            insights_rows=insights_by_call.get(call_id, []),
            taxonomy_sets=taxonomy_sets,
            taxonomy_sets_available=taxonomy_sets_available,
            taxonomy_version_filter=taxonomy_version,
            allow_warn=allow_warn,
        )

        if status == "SUCCESS":
            success_calls += 1
        elif status == "WARN":
            warn_calls += 1
        elif status == "FAILED":
            failed_calls += 1
        else:
            failed_calls += 1
            status = "FAILED"
            critical = [f"Unsupported status generated: {status!r}"]

        message_parts = []
        if critical:
            message_parts.append("critical=" + "; ".join(critical[:5]))
        if warns:
            message_parts.append("warn=" + "; ".join(warns[:5]))
        error_message = " | ".join(message_parts)[:1000] if message_parts else None

        status_rows.append(
            {
                "call_id": call_id,
                "stage_name": STAGE_NAME,
                "status": status,
                "error_message": error_message,
                "run_id": run_id,
                "updated_at": ts,
            }
        )

    if status_rows:
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
        status_df = spark.createDataFrame(status_rows, schema=status_schema)
        status_df.createOrReplaceTempView("tmp_insights_08_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file} AS t
            USING tmp_insights_08_status AS s
            ON t.call_id = s.call_id AND t.stage_name = s.stage_name
            WHEN MATCHED THEN UPDATE SET
              t.status = s.status,
              t.error_message = s.error_message,
              t.run_id = s.run_id,
              t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (
              call_id, stage_name, status, error_message, run_id, updated_at
            ) VALUES (
              s.call_id, s.stage_name, s.status, s.error_message, s.run_id, s.updated_at
            )
            """
        )

    if publish_latest_views:
        spark.sql(
            f"""
            CREATE OR REPLACE VIEW {_fq(catalog, schema, "vw_gold_conversation_metrics_latest")} AS
            SELECT *
            FROM {metrics_table}
            WHERE metrics_version = {_lit(metrics_version)}
            """
        )
        insights_where = [
            f"consolidation_version = {_lit(consolidation_version)}",
        ]
        if "metrics_version" in insights_df.columns:
            insights_where.append(f"metrics_version = {_lit(metrics_version)}")
        if taxonomy_version and "taxonomy_version" in insights_df.columns:
            insights_where.append(f"taxonomy_version = {_lit(taxonomy_version)}")
        spark.sql(
            f"""
            CREATE OR REPLACE VIEW {_fq(catalog, schema, "vw_gold_speech_insights_latest")} AS
            SELECT *
            FROM {insights_table}
            WHERE {" AND ".join(insights_where)}
            """
        )

    success_warn_calls = success_calls + warn_calls
    success_like = success_warn_calls + skipped_calls

    if global_schema_errors:
        final_status = "FAILED"
        error_summary = "; ".join(global_schema_errors)[:1000]
    elif eligible_count > 0 and success_like == 0:
        final_status = "FAILED"
        error_summary = "Zero eligible calls completed with SUCCESS/WARN/SKIPPED."
        raise RuntimeError(error_summary)
    elif failed_calls > 0 and success_warn_calls > 0:
        final_status = "WARN"
        error_summary = f"{failed_calls} call(s) failed; {warn_calls} call(s) WARN."
    elif failed_calls > 0 and success_warn_calls == 0:
        final_status = "FAILED"
        error_summary = f"{failed_calls} call(s) failed; zero calls succeeded/warned."
    elif failed_calls > 0:
        final_status = "FAILED"
        error_summary = f"{failed_calls} call(s) failed."
    elif eligible_count == 0:
        final_status = "SUCCESS"
        error_summary = f"No eligible calls for stage {STAGE_NAME}."
    else:
        final_status = "SUCCESS"
        if warn_calls > 0:
            error_summary = f"{warn_calls} call(s) completed with WARN."
        elif skipped_calls > 0:
            error_summary = f"{skipped_calls} call(s) skipped."
        else:
            error_summary = "Quality gates completed successfully."

except Exception as exc:
    final_status = "FAILED"
    if error_summary is None:
        error_summary = _truncate(exc)
    raise
finally:
    _upsert_run_final(
        ops_runs=ops_runs,
        run_id=run_id,
        status=final_status,
        total_files=eligible_count,
        success_count=success_calls + warn_calls + skipped_calls,
        failed_count=failed_calls,
        error_summary=error_summary,
    )
    print(
        f"[{STAGE_NAME}] eligible={eligible_count} success={success_calls} warn={warn_calls} "
        f"skipped={skipped_calls} failed={failed_calls} status={final_status}"
    )
