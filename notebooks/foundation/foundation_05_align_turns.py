# Databricks notebook source
# MAGIC %md
# MAGIC # FOUNDATION_05_ALIGN_TURNS
# MAGIC
# MAGIC **Purpose**
# MAGIC - Align ASR segments with optional diarization segments to produce ordered turn-level rows.
# MAGIC
# MAGIC **Inputs**
# MAGIC - Parameters: `catalog`, `schema`, `run_id`, `run_mode`, `max_files_per_run`, `alignment_version`, `enable_diarization`, `alignment_policy`, `role_assignment_policy`, `language_resolution_policy`
# MAGIC - Tables: `<catalog>.<schema>.bronze_audio_files`, `<catalog>.<schema>.silver_asr_segments`, optional `<catalog>.<schema>.silver_diarization_segments`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.<schema>.silver_turns_aligned`
# MAGIC - `<catalog>.<schema>.ops_file_status` (stage: `align_turns`)
# MAGIC - `<catalog>.<schema>.ops_pipeline_runs`
# MAGIC
# MAGIC **Key rules**
# MAGIC - Supports `sample | incremental | full`.
# MAGIC - Diarization is optional: if missing/unavailable/empty, falls back to `SPEAKER_UNKNOWN` and `Unknown`.
# MAGIC - Per-call failures are isolated; task fails only when all eligible calls fail.

# COMMAND ----------

import json
import os
import re
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import functions as F
from pyspark.sql import types as T


WORKFLOW_NAME = "foundation"
STAGE_NAME = "align_turns"
ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
ALLOWED_ALIGNMENT_POLICIES = {"max_overlap", "midpoint"}
ALLOWED_LANGUAGE_POLICIES = {"prefer_detected_else_hint", "prefer_hint_else_detected"}
UNKNOWN_SPEAKER = "SPEAKER_UNKNOWN"
UNKNOWN_ROLE = "Unknown"
UNKNOWN_LANGUAGE = "unknown"
MERGE_GAP_SEC = 0.75


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


def _ensure_tables(
    turns_table: str, ops_file_status_table: str, ops_pipeline_runs_table: str
) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {turns_table} (
          call_id STRING,
          turn_id STRING,
          speaker_label STRING,
          role STRING,
          start_sec DOUBLE,
          end_sec DOUBLE,
          text_original STRING,
          language_hint STRING,
          language_detected STRING,
          language_final STRING,
          alignment_version STRING,
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


def _resolve_language(
    language_hint: Optional[str],
    language_detected: Optional[str],
    policy: str,
) -> str:
    hint = (language_hint or "").strip().lower() or None
    detected = (language_detected or "").strip().lower() or None
    if policy == "prefer_hint_else_detected":
        return hint or detected or UNKNOWN_LANGUAGE
    return detected or hint or UNKNOWN_LANGUAGE


def _overlap(start_a: float, end_a: float, start_b: float, end_b: float) -> float:
    return max(0.0, min(end_a, end_b) - max(start_a, start_b))


def _assign_speaker_label(
    asr_start: float,
    asr_end: float,
    diar_segments: List[Dict[str, object]],
    alignment_policy: str,
) -> str:
    if not diar_segments:
        return UNKNOWN_SPEAKER

    midpoint = (asr_start + asr_end) / 2.0

    if alignment_policy == "max_overlap":
        best_overlap = 0.0
        best_label: Optional[str] = None
        for seg in diar_segments:
            ov = _overlap(asr_start, asr_end, float(seg["start_sec"]), float(seg["end_sec"]))
            if ov > best_overlap:
                best_overlap = ov
                best_label = str(seg["speaker_label"])
        if best_label:
            return best_label

    containing: List[Dict[str, object]] = []
    for seg in diar_segments:
        start_sec = float(seg["start_sec"])
        end_sec = float(seg["end_sec"])
        if start_sec <= midpoint <= end_sec:
            containing.append(seg)
    if containing:
        containing.sort(key=lambda seg: float(seg["end_sec"]) - float(seg["start_sec"]))
        return str(containing[0]["speaker_label"])

    diar_segments_sorted = sorted(
        diar_segments, key=lambda seg: abs(((float(seg["start_sec"]) + float(seg["end_sec"])) / 2.0) - midpoint)
    )
    return str(diar_segments_sorted[0]["speaker_label"]) if diar_segments_sorted else UNKNOWN_SPEAKER


def _assign_role_map(
    role_assignment_policy: str, turns: List[Dict[str, object]]
) -> Dict[str, str]:
    speaker_labels = {str(turn["speaker_label"]) for turn in turns if turn["speaker_label"] != UNKNOWN_SPEAKER}
    if role_assignment_policy == "dominant_two_speakers_v1" and len(speaker_labels) == 2:
        totals: Dict[str, float] = {label: 0.0 for label in speaker_labels}
        for turn in turns:
            label = str(turn["speaker_label"])
            if label in totals:
                totals[label] += float(turn["end_sec"]) - float(turn["start_sec"])
        ordered = sorted(totals.items(), key=lambda x: x[1], reverse=True)
        return {
            ordered[0][0]: "Agent",
            ordered[1][0]: "Customer",
        }
    return {}


if _is_databricks():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("alignment_version", "v1")
    dbutils.widgets.text("enable_diarization", "true")
    dbutils.widgets.text("alignment_policy", "max_overlap")
    dbutils.widgets.text("role_assignment_policy", "unknown_roles_v1")
    dbutils.widgets.text("language_resolution_policy", "prefer_detected_else_hint")

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    alignment_version = dbutils.widgets.get("alignment_version").strip() or "v1"
    enable_diarization_raw = dbutils.widgets.get("enable_diarization").strip()
    alignment_policy = dbutils.widgets.get("alignment_policy").strip().lower()
    role_assignment_policy = dbutils.widgets.get("role_assignment_policy").strip() or "unknown_roles_v1"
    language_resolution_policy = dbutils.widgets.get("language_resolution_policy").strip().lower()
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    alignment_version = os.getenv("ALIGNMENT_VERSION", "v1").strip() or "v1"
    enable_diarization_raw = os.getenv("ENABLE_DIARIZATION", "true").strip()
    alignment_policy = os.getenv("ALIGNMENT_POLICY", "max_overlap").strip().lower()
    role_assignment_policy = os.getenv("ROLE_ASSIGNMENT_POLICY", "unknown_roles_v1").strip() or "unknown_roles_v1"
    language_resolution_policy = os.getenv(
        "LANGUAGE_RESOLUTION_POLICY", "prefer_detected_else_hint"
    ).strip().lower()

catalog = _validate_identifier("catalog", catalog)
schema = _validate_identifier("schema", schema)
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid `run_mode`: {run_mode!r}. Allowed: {sorted(ALLOWED_RUN_MODES)}")
if alignment_policy not in ALLOWED_ALIGNMENT_POLICIES:
    raise ValueError(
        f"Invalid `alignment_policy`: {alignment_policy!r}. Allowed: {sorted(ALLOWED_ALIGNMENT_POLICIES)}"
    )
if language_resolution_policy not in ALLOWED_LANGUAGE_POLICIES:
    raise ValueError(
        f"Invalid `language_resolution_policy`: {language_resolution_policy!r}. "
        f"Allowed: {sorted(ALLOWED_LANGUAGE_POLICIES)}"
    )

enable_diarization = _parse_bool("enable_diarization", enable_diarization_raw, default=True)
max_files_per_run: Optional[int] = None
if max_files_per_run_raw:
    max_files_per_run = int(max_files_per_run_raw)
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError(
        "In sample mode, `max_files_per_run` must be provided as an integer > 0."
    )

bronze_table = _fq_table(catalog, schema, "bronze_audio_files")
asr_table = _fq_table(catalog, schema, "silver_asr_segments")
diar_table = _fq_table(catalog, schema, "silver_diarization_segments")
turns_table = _fq_table(catalog, schema, "silver_turns_aligned")
ops_file_status_table = _fq_table(catalog, schema, "ops_file_status")
ops_pipeline_runs_table = _fq_table(catalog, schema, "ops_pipeline_runs")

params_snapshot = {
    "catalog": catalog,
    "schema": schema,
    "run_id": run_id,
    "run_mode": run_mode,
    "max_files_per_run": max_files_per_run,
    "alignment_version": alignment_version,
    "enable_diarization": enable_diarization,
    "alignment_policy": alignment_policy,
    "role_assignment_policy": role_assignment_policy,
    "language_resolution_policy": language_resolution_policy,
}
parameters_json = json.dumps(params_snapshot, sort_keys=True)

print(
    f"[{STAGE_NAME}] Starting with run_id={run_id}, run_mode={run_mode}, "
    f"alignment_version={alignment_version}"
)

_ensure_tables(turns_table, ops_file_status_table, ops_pipeline_runs_table)
_upsert_pipeline_run_running(ops_pipeline_runs_table, run_id, parameters_json)

eligible_count = 0
ops_success_count = 0
ops_failed_count = 0
turns_written = 0
final_status = "SUCCESS"
error_summary: Optional[str] = None

try:
    if not _table_exists(catalog, schema, "bronze_audio_files"):
        raise RuntimeError(f"Required input table is missing: {bronze_table}")
    if not _table_exists(catalog, schema, "silver_asr_segments"):
        raise RuntimeError(f"Required input table is missing: {asr_table}")

    bronze_df = (
        spark.table(bronze_table)
        .select("call_id", "language_hint", "status")
        .where("call_id IS NOT NULL")
    )
    if run_mode in {"sample", "incremental"}:
        bronze_df = bronze_df.where(F.upper(F.col("status")).isin("NEW", "PROCESSED"))

    align_status_df = (
        spark.table(ops_file_status_table)
        .where(F.col("stage_name") == STAGE_NAME)
        .select("call_id", F.col("status").alias("stage_status"))
    )

    eligible_df = bronze_df.join(align_status_df, on="call_id", how="left")
    if run_mode in {"sample", "incremental"}:
        eligible_df = eligible_df.where(
            F.col("stage_status").isNull() | (F.upper(F.col("stage_status")) == "FAILED")
        )

    eligible_df = eligible_df.select("call_id", "language_hint").orderBy("call_id")
    if run_mode == "sample" and max_files_per_run is not None:
        eligible_df = eligible_df.limit(max_files_per_run)

    eligible_rows = eligible_df.collect()
    eligible_count = len(eligible_rows)
    print(f"[{STAGE_NAME}] Eligible calls: {eligible_count}")

    diarization_available = False
    if enable_diarization and _table_exists(catalog, schema, "silver_diarization_segments"):
        diarization_available = spark.table(diar_table).limit(1).count() > 0
    print(
        f"[{STAGE_NAME}] enable_diarization={enable_diarization}, "
        f"diarization_available={diarization_available}"
    )

    status_records: List[Dict[str, object]] = []
    turn_records: List[Dict[str, object]] = []
    successful_call_ids: List[str] = []
    warning_empty_turn_calls = 0

    process_ts = datetime.utcnow()
    for row in eligible_rows:
        call_id = str(row["call_id"])
        language_hint = row["language_hint"]
        try:
            asr_rows = (
                spark.table(asr_table)
                .where(F.col("call_id") == call_id)
                .select(
                    "asr_segment_id",
                    "start_sec",
                    "end_sec",
                    "text",
                    "language_detected",
                )
                .orderBy("start_sec", "end_sec", "asr_segment_id")
                .collect()
            )
            if not asr_rows:
                raise ValueError("ASR segments not found for call_id.")

            diar_segments: List[Dict[str, object]] = []
            if diarization_available:
                diar_rows = (
                    spark.table(diar_table)
                    .where(F.col("call_id") == call_id)
                    .select("speaker_label", "start_sec", "end_sec")
                    .orderBy("start_sec", "end_sec")
                    .collect()
                )
                for d in diar_rows:
                    d_start = float(d["start_sec"])
                    d_end = float(d["end_sec"])
                    if d_end <= d_start:
                        continue
                    diar_segments.append(
                        {
                            "speaker_label": str(d["speaker_label"] or UNKNOWN_SPEAKER),
                            "start_sec": d_start,
                            "end_sec": d_end,
                        }
                    )

            aligned_segments: List[Dict[str, object]] = []
            for a in asr_rows:
                start_sec = float(a["start_sec"])
                end_sec = float(a["end_sec"])
                text = str(a["text"] or "").strip()
                language_detected = (a["language_detected"] or "").strip() or None

                if end_sec <= start_sec:
                    raise ValueError(
                        f"Invalid ASR segment timing: start_sec={start_sec}, end_sec={end_sec}"
                    )
                if not text:
                    continue

                speaker_label = _assign_speaker_label(
                    asr_start=start_sec,
                    asr_end=end_sec,
                    diar_segments=diar_segments,
                    alignment_policy=alignment_policy,
                )
                aligned_segments.append(
                    {
                        "speaker_label": speaker_label or UNKNOWN_SPEAKER,
                        "start_sec": start_sec,
                        "end_sec": end_sec,
                        "text": text,
                        "language_detected": language_detected,
                    }
                )

            if not aligned_segments:
                warning_empty_turn_calls += 1
                raise ValueError("No non-empty ASR text segments available for alignment.")

            turns: List[Dict[str, object]] = []
            for seg in aligned_segments:
                if not turns:
                    turns.append(
                        {
                            "speaker_label": seg["speaker_label"],
                            "start_sec": seg["start_sec"],
                            "end_sec": seg["end_sec"],
                            "text_original": seg["text"],
                            "language_detected": seg["language_detected"],
                        }
                    )
                    continue

                prev = turns[-1]
                gap = float(seg["start_sec"]) - float(prev["end_sec"])
                if (
                    seg["speaker_label"] == prev["speaker_label"]
                    and gap <= MERGE_GAP_SEC
                ):
                    prev["end_sec"] = max(float(prev["end_sec"]), float(seg["end_sec"]))
                    prev["text_original"] = f"{prev['text_original']} {seg['text']}".strip()
                    if prev["language_detected"] is None:
                        prev["language_detected"] = seg["language_detected"]
                else:
                    turns.append(
                        {
                            "speaker_label": seg["speaker_label"],
                            "start_sec": seg["start_sec"],
                            "end_sec": seg["end_sec"],
                            "text_original": seg["text"],
                            "language_detected": seg["language_detected"],
                        }
                    )

            role_map = _assign_role_map(role_assignment_policy=role_assignment_policy, turns=turns)

            for idx, turn in enumerate(turns, start=1):
                start_sec = float(turn["start_sec"])
                end_sec = float(turn["end_sec"])
                text_original = str(turn["text_original"]).strip()
                if end_sec <= start_sec:
                    raise ValueError(
                        f"Invalid turn timing after alignment: start_sec={start_sec}, end_sec={end_sec}"
                    )
                if not text_original:
                    continue

                language_detected = turn["language_detected"]
                language_final = _resolve_language(
                    language_hint=language_hint,
                    language_detected=language_detected,
                    policy=language_resolution_policy,
                )
                if not language_final:
                    raise ValueError("language_final resolved to null.")

                speaker_label = str(turn["speaker_label"] or UNKNOWN_SPEAKER)
                turn_records.append(
                    {
                        "call_id": call_id,
                        "turn_id": f"{call_id}_turn_{idx:05d}",
                        "speaker_label": speaker_label,
                        "role": role_map.get(speaker_label, UNKNOWN_ROLE),
                        "start_sec": start_sec,
                        "end_sec": end_sec,
                        "text_original": text_original,
                        "language_hint": language_hint,
                        "language_detected": language_detected,
                        "language_final": language_final,
                        "alignment_version": alignment_version,
                        "run_id": run_id,
                        "updated_at": process_ts,
                    }
                )

            successful_call_ids.append(call_id)
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
    ops_failed_count = sum(1 for row in status_records if row["status"] == "FAILED")

    if turn_records:
        turns_schema = T.StructType(
            [
                T.StructField("call_id", T.StringType(), False),
                T.StructField("turn_id", T.StringType(), False),
                T.StructField("speaker_label", T.StringType(), False),
                T.StructField("role", T.StringType(), False),
                T.StructField("start_sec", T.DoubleType(), False),
                T.StructField("end_sec", T.DoubleType(), False),
                T.StructField("text_original", T.StringType(), False),
                T.StructField("language_hint", T.StringType(), True),
                T.StructField("language_detected", T.StringType(), True),
                T.StructField("language_final", T.StringType(), False),
                T.StructField("alignment_version", T.StringType(), False),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )
        turns_df = spark.createDataFrame(turn_records, schema=turns_schema)
        successful_call_ids_sql = ", ".join(
            _sql_literal(call_id) for call_id in sorted(set(successful_call_ids))
        )
        if successful_call_ids_sql:
            spark.sql(
                f"DELETE FROM {turns_table} WHERE call_id IN ({successful_call_ids_sql})"
            )
        turns_df.write.format("delta").mode("append").saveAsTable(turns_table)
        turns_written = len(turn_records)

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
        status_df.createOrReplaceTempView("tmp_foundation_05_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file_status_table} AS t
            USING tmp_foundation_05_status AS s
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

    if warning_empty_turn_calls > 0:
        print(
            f"[{STAGE_NAME}] WARNING calls with empty-text turn outputs: {warning_empty_turn_calls}"
        )

    if eligible_count > 0 and ops_success_count == 0:
        final_status = "FAILED"
        error_summary = "All eligible calls failed during align_turns."
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
        success_count=ops_success_count,
        failed_count=ops_failed_count,
        error_summary=error_summary,
    )
    print(
        f"[{STAGE_NAME}] eligible={eligible_count} success_calls={ops_success_count} "
        f"failed_calls={ops_failed_count} turns_written={turns_written} status={final_status}"
    )
