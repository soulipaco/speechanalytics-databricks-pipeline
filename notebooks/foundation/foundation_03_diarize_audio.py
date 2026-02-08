# Databricks notebook source
# MAGIC %md
# MAGIC # FOUNDATION_03_DIARIZE_AUDIO
# MAGIC
# MAGIC **Purpose**
# MAGIC - Generate speaker diarization segments per call using primary diarization or fallback segmentation.
# MAGIC
# MAGIC **Inputs**
# MAGIC - Parameters: `catalog`, `schema`, `volume_root`, `run_id`, `run_mode`, `max_files_per_run`, `enable_diarization`, `use_preprocessed_audio`, `diarization_model_name`, `diarization_version`, `enable_fallback_segmentation`, `fallback_method`, `min_segment_sec`, `merge_gap_sec`, `hf_token`
# MAGIC - Tables: `<catalog>.<schema>.bronze_audio_files`, optional `<catalog>.<schema>.silver_audio_preprocessed`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.<schema>.silver_diarization_segments`
# MAGIC - `<catalog>.<schema>.ops_file_status` (stage: `diarize_audio`)
# MAGIC - `<catalog>.<schema>.ops_pipeline_runs`
# MAGIC
# MAGIC **Key rules**
# MAGIC - Supports `sample | incremental | full`.
# MAGIC - Prefer preprocessed audio path when available; fallback to bronze path.
# MAGIC - Idempotent writes for successful calls: delete by `call_id` then append.

# COMMAND ----------

import json
import os
import re
import wave
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import functions as F
from pyspark.sql import types as T


WORKFLOW_NAME = "foundation"
STAGE_NAME = "diarize_audio"
ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
ALLOWED_FALLBACK_METHODS = {"vad_fallback", "single_segment_fallback"}


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


def _canonical_volume_path(path: str) -> str:
    raw = (path or "").strip()
    if not raw:
        return raw
    if raw.startswith("dbfs:/Volumes/"):
        return "/" + raw[len("dbfs:/") :].rstrip("/")
    if raw.startswith("Volumes/"):
        return "/" + raw.rstrip("/")
    if raw.startswith("/Volumes/"):
        return raw.rstrip("/")
    return raw.rstrip("/")


def _to_local_read_path(path: str) -> str:
    if path.startswith("dbfs:/Volumes/"):
        return "/" + path[len("dbfs:/") :]
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path[len("dbfs:/") :]
    if path.startswith("Volumes/"):
        return "/" + path
    return path


def _table_exists(catalog: str, schema: str, table: str) -> bool:
    return bool(spark.catalog.tableExists(f"{catalog}.{schema}.{table}"))


def _ensure_tables(
    diar_table: str, ops_file_status_table: str, ops_pipeline_runs_table: str
) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {diar_table} (
          call_id STRING,
          segment_id STRING,
          speaker_label STRING,
          start_sec DOUBLE,
          end_sec DOUBLE,
          method STRING,
          diarization_method STRING,
          diarization_model STRING,
          diarization_version STRING,
          confidence DOUBLE,
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


def _merge_adjacent_segments(
    segments: Sequence[Dict[str, object]],
    merge_gap_sec: float,
    min_segment_sec: float,
    duration_sec: Optional[float],
) -> List[Dict[str, object]]:
    if not segments:
        return []

    ordered = sorted(segments, key=lambda x: (float(x["start_sec"]), float(x["end_sec"])))
    merged: List[Dict[str, object]] = []

    for seg in ordered:
        start_sec = float(seg["start_sec"])
        end_sec = float(seg["end_sec"])
        speaker_label = str(seg.get("speaker_label") or "SPEAKER_00")
        confidence = seg.get("confidence")

        if duration_sec is not None:
            start_sec = max(0.0, min(start_sec, duration_sec))
            end_sec = max(0.0, min(end_sec, duration_sec))

        if end_sec <= start_sec:
            continue

        if merged and speaker_label == str(merged[-1]["speaker_label"]):
            gap = start_sec - float(merged[-1]["end_sec"])
            if gap <= merge_gap_sec:
                merged[-1]["end_sec"] = max(float(merged[-1]["end_sec"]), end_sec)
                if merged[-1].get("confidence") is None and confidence is not None:
                    merged[-1]["confidence"] = confidence
                continue

        merged.append(
            {
                "speaker_label": speaker_label,
                "start_sec": start_sec,
                "end_sec": end_sec,
                "confidence": confidence,
            }
        )

    filtered: List[Dict[str, object]] = []
    for seg in merged:
        seg_len = float(seg["end_sec"]) - float(seg["start_sec"])
        if seg_len < min_segment_sec:
            continue
        filtered.append(seg)

    return filtered


def _load_duration_sec(audio_local_path: str) -> float:
    with wave.open(audio_local_path, "rb") as reader:
        frame_rate = int(reader.getframerate())
        frame_count = int(reader.getnframes())
        if frame_rate <= 0:
            raise ValueError("Invalid WAV sample rate in input audio.")
        return frame_count / float(frame_rate)


def _single_segment_fallback(duration_sec: float) -> List[Dict[str, object]]:
    if duration_sec <= 0:
        return []
    return [
        {
            "speaker_label": "SPEAKER_00",
            "start_sec": 0.0,
            "end_sec": float(duration_sec),
            "confidence": None,
        }
    ]

def _vad_fallback_segments(
    audio_local_path: str,
    min_segment_sec: float,
    merge_gap_sec: float,
    duration_sec: Optional[float],
) -> List[Dict[str, object]]:
    try:
        import audioop  # type: ignore
    except Exception as exc:
        raise RuntimeError("Fallback VAD requires Python `audioop`, but it is unavailable.") from exc

    with wave.open(audio_local_path, "rb") as reader:
        channels = int(reader.getnchannels())
        sampwidth = int(reader.getsampwidth())
        sample_rate = int(reader.getframerate())
        frames = reader.readframes(reader.getnframes())

    if sample_rate <= 0:
        raise ValueError("Invalid sample rate for fallback VAD.")
    if channels not in {1, 2}:
        raise ValueError(f"Unsupported channel count={channels} for fallback VAD.")

    mono = frames
    if channels == 2:
        mono = audioop.tomono(frames, sampwidth, 0.5, 0.5)

    frame_samples = max(1, int(sample_rate * 0.03))
    frame_bytes = frame_samples * sampwidth

    if len(mono) < frame_bytes:
        return _single_segment_fallback(duration_sec or _load_duration_sec(audio_local_path))

    rms_values: List[int] = []
    for i in range(0, len(mono), frame_bytes):
        chunk = mono[i : i + frame_bytes]
        if len(chunk) < frame_bytes:
            break
        rms_values.append(int(audioop.rms(chunk, sampwidth)))

    if not rms_values:
        return _single_segment_fallback(duration_sec or _load_duration_sec(audio_local_path))

    avg_rms = sum(rms_values) / float(len(rms_values))
    max_rms = max(rms_values)
    threshold = max(250.0, min(max_rms * 0.8, avg_rms * 1.5 + 120.0))

    raw_segments: List[Tuple[float, float]] = []
    active_start: Optional[float] = None

    for idx, rms in enumerate(rms_values):
        frame_start = idx * frame_samples / float(sample_rate)
        frame_end = (idx + 1) * frame_samples / float(sample_rate)
        is_speech = rms >= threshold

        if is_speech and active_start is None:
            active_start = frame_start
        elif (not is_speech) and active_start is not None:
            raw_segments.append((active_start, frame_end))
            active_start = None

    if active_start is not None:
        last_end = len(rms_values) * frame_samples / float(sample_rate)
        raw_segments.append((active_start, last_end))

    if not raw_segments:
        return _single_segment_fallback(duration_sec or _load_duration_sec(audio_local_path))

    segs = [
        {
            "speaker_label": "SPEAKER_00",
            "start_sec": float(start_sec),
            "end_sec": float(end_sec),
            "confidence": None,
        }
        for start_sec, end_sec in raw_segments
    ]

    merged = _merge_adjacent_segments(
        segments=segs,
        merge_gap_sec=merge_gap_sec,
        min_segment_sec=min_segment_sec,
        duration_sec=duration_sec,
    )
    if merged:
        return merged

    return _single_segment_fallback(duration_sec or _load_duration_sec(audio_local_path))


def _run_pyannote(
    audio_local_path: str,
    diarization_model_name: str,
    hf_token: str,
    min_segment_sec: float,
    merge_gap_sec: float,
    duration_sec: Optional[float],
) -> List[Dict[str, object]]:
    try:
        from pyannote.audio import Pipeline  # type: ignore
    except Exception as exc:
        raise RuntimeError("pyannote is unavailable in this runtime.") from exc

    try:
        pipeline = Pipeline.from_pretrained(diarization_model_name, use_auth_token=hf_token)
    except Exception as exc:
        raise RuntimeError("Failed to initialize pyannote pipeline.") from exc

    try:
        diarization = pipeline(audio_local_path)
    except Exception as exc:
        raise RuntimeError("pyannote diarization inference failed.") from exc

    output: List[Dict[str, object]] = []
    for turn, _, label in diarization.itertracks(yield_label=True):
        start_sec = float(getattr(turn, "start", 0.0))
        end_sec = float(getattr(turn, "end", 0.0))
        if end_sec <= start_sec:
            continue
        output.append(
            {
                "speaker_label": str(label or "SPEAKER_00"),
                "start_sec": start_sec,
                "end_sec": end_sec,
                "confidence": None,
            }
        )

    merged = _merge_adjacent_segments(
        segments=output,
        merge_gap_sec=merge_gap_sec,
        min_segment_sec=min_segment_sec,
        duration_sec=duration_sec,
    )
    if not merged:
        raise RuntimeError("pyannote returned no valid segments.")
    return merged

if _is_databricks():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("volume_root", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("enable_diarization", "true")
    dbutils.widgets.text("use_preprocessed_audio", "false")
    dbutils.widgets.text("diarization_model_name", "pyannote/speaker-diarization-3.1")
    dbutils.widgets.text("diarization_version", "v1")
    dbutils.widgets.text("enable_fallback_segmentation", "true")
    dbutils.widgets.text("fallback_method", "vad_fallback")
    dbutils.widgets.text("min_segment_sec", "0.5")
    dbutils.widgets.text("merge_gap_sec", "0.2")
    dbutils.widgets.text("hf_token", "")

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    volume_root = dbutils.widgets.get("volume_root").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    enable_diarization_raw = dbutils.widgets.get("enable_diarization").strip()
    use_preprocessed_audio_raw = dbutils.widgets.get("use_preprocessed_audio").strip()
    diarization_model_name = dbutils.widgets.get("diarization_model_name").strip() or "pyannote/speaker-diarization-3.1"
    diarization_version = dbutils.widgets.get("diarization_version").strip() or "v1"
    enable_fallback_segmentation_raw = dbutils.widgets.get("enable_fallback_segmentation").strip()
    fallback_method = dbutils.widgets.get("fallback_method").strip().lower() or "vad_fallback"
    min_segment_sec_raw = dbutils.widgets.get("min_segment_sec").strip()
    merge_gap_sec_raw = dbutils.widgets.get("merge_gap_sec").strip()
    hf_token_widget = dbutils.widgets.get("hf_token").strip()
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    volume_root = os.getenv("VOLUME_ROOT", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    enable_diarization_raw = os.getenv("ENABLE_DIARIZATION", "true").strip()
    use_preprocessed_audio_raw = os.getenv("USE_PREPROCESSED_AUDIO", "false").strip()
    diarization_model_name = os.getenv("DIARIZATION_MODEL_NAME", "pyannote/speaker-diarization-3.1").strip() or "pyannote/speaker-diarization-3.1"
    diarization_version = os.getenv("DIARIZATION_VERSION", "v1").strip() or "v1"
    enable_fallback_segmentation_raw = os.getenv("ENABLE_FALLBACK_SEGMENTATION", "true").strip()
    fallback_method = os.getenv("FALLBACK_METHOD", "vad_fallback").strip().lower() or "vad_fallback"
    min_segment_sec_raw = os.getenv("MIN_SEGMENT_SEC", "0.5").strip()
    merge_gap_sec_raw = os.getenv("MERGE_GAP_SEC", "0.2").strip()
    hf_token_widget = os.getenv("HF_TOKEN", "").strip()

catalog = _validate_identifier("catalog", catalog)
schema = _validate_identifier("schema", schema)
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid `run_mode`: {run_mode!r}. Allowed: {sorted(ALLOWED_RUN_MODES)}")
if fallback_method not in ALLOWED_FALLBACK_METHODS:
    raise ValueError(
        f"Invalid `fallback_method`: {fallback_method!r}. Allowed: {sorted(ALLOWED_FALLBACK_METHODS)}"
    )

max_files_per_run: Optional[int] = None
if max_files_per_run_raw:
    max_files_per_run = int(max_files_per_run_raw)
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError(
        "In sample mode, `max_files_per_run` must be provided as an integer > 0."
    )

enable_diarization = _parse_bool("enable_diarization", enable_diarization_raw, default=True)
use_preprocessed_audio = _parse_bool(
    "use_preprocessed_audio", use_preprocessed_audio_raw, default=False
)
enable_fallback_segmentation = _parse_bool(
    "enable_fallback_segmentation", enable_fallback_segmentation_raw, default=True
)
min_segment_sec = float(min_segment_sec_raw or "0.5")
merge_gap_sec = float(merge_gap_sec_raw or "0.2")
if min_segment_sec <= 0:
    raise ValueError("`min_segment_sec` must be > 0.")
if merge_gap_sec < 0:
    raise ValueError("`merge_gap_sec` must be >= 0.")

hf_token = (hf_token_widget or os.getenv("HF_TOKEN", "").strip()).strip()
if hf_token and len(hf_token) < 8:
    raise ValueError("Provided `hf_token` appears invalid (too short).")

bronze_table = _fq_table(catalog, schema, "bronze_audio_files")
preprocessed_table = _fq_table(catalog, schema, "silver_audio_preprocessed")
diar_table = _fq_table(catalog, schema, "silver_diarization_segments")
ops_file_status_table = _fq_table(catalog, schema, "ops_file_status")
ops_pipeline_runs_table = _fq_table(catalog, schema, "ops_pipeline_runs")

params_snapshot = {
    "catalog": catalog,
    "schema": schema,
    "volume_root": volume_root,
    "run_id": run_id,
    "run_mode": run_mode,
    "max_files_per_run": max_files_per_run,
    "enable_diarization": enable_diarization,
    "use_preprocessed_audio": use_preprocessed_audio,
    "diarization_model_name": diarization_model_name,
    "diarization_version": diarization_version,
    "enable_fallback_segmentation": enable_fallback_segmentation,
    "fallback_method": fallback_method,
    "min_segment_sec": min_segment_sec,
    "merge_gap_sec": merge_gap_sec,
    "hf_token_provided": bool(hf_token),
}
parameters_json = json.dumps(params_snapshot, sort_keys=True)

print(
    f"[{STAGE_NAME}] Starting with run_id={run_id}, run_mode={run_mode}, "
    f"enable_diarization={enable_diarization}, fallback={enable_fallback_segmentation}"
)

_ensure_tables(diar_table, ops_file_status_table, ops_pipeline_runs_table)
_upsert_pipeline_run_running(ops_pipeline_runs_table, run_id, parameters_json)

eligible_count = 0
ops_success_count = 0
ops_failed_count = 0
ops_skipped_count = 0
segments_written = 0
pyannote_success_calls = 0
fallback_success_calls = 0
warning_low_segment_calls = 0
final_status = "SUCCESS"
error_summary: Optional[str] = None

try:
    if not _table_exists(catalog, schema, "bronze_audio_files"):
        raise RuntimeError(f"Required input table is missing: {bronze_table}")

    bronze_df = (
        spark.table(bronze_table)
        .select("call_id", "file_path", "duration_sec")
        .where("call_id IS NOT NULL AND file_path IS NOT NULL")
    )

    stage_df = (
        spark.table(ops_file_status_table)
        .where(F.col("stage_name") == STAGE_NAME)
        .select("call_id", F.col("status").alias("stage_status"))
    )

    eligible_df = bronze_df.join(stage_df, on="call_id", how="left")
    if run_mode in {"sample", "incremental"}:
        eligible_df = eligible_df.where(
            F.col("stage_status").isNull() | (F.upper(F.col("stage_status")) == "FAILED")
        )

    preprocessed_available = _table_exists(catalog, schema, "silver_audio_preprocessed")
    if preprocessed_available:
        pre_df = spark.table(preprocessed_table)
        pre_cols = set(pre_df.columns)
        if {"call_id", "output_path"}.issubset(pre_cols):
            if "status" in pre_cols:
                pre_df = pre_df.where(F.upper(F.col("status")) == "SUCCESS")
            pre_df = pre_df.select(
                "call_id",
                F.col("output_path").alias("preprocessed_path"),
            ).dropDuplicates(["call_id"])
            eligible_df = (
                eligible_df.join(pre_df, on="call_id", how="left")
                .withColumn(
                    "resolved_audio_path",
                    F.when(
                        F.col("preprocessed_path").isNotNull()
                        & (F.length(F.trim(F.col("preprocessed_path"))) > 0),
                        F.col("preprocessed_path"),
                    ).otherwise(F.col("file_path")),
                )
                .drop("preprocessed_path")
                .drop("file_path")
                .withColumnRenamed("resolved_audio_path", "audio_path")
            )
        else:
            eligible_df = eligible_df.withColumnRenamed("file_path", "audio_path")
    else:
        eligible_df = eligible_df.withColumnRenamed("file_path", "audio_path")

    eligible_df = eligible_df.select("call_id", "audio_path", "duration_sec").orderBy("call_id")

    if run_mode == "sample" and max_files_per_run is not None:
        eligible_df = eligible_df.limit(max_files_per_run)

    eligible_rows = eligible_df.collect()
    eligible_count = len(eligible_rows)
    print(f"[{STAGE_NAME}] Eligible calls: {eligible_count}")

    pyannote_enabled = enable_diarization and bool(hf_token)
    if enable_diarization and not hf_token:
        print(f"[{STAGE_NAME}] HF token not provided; skipping pyannote and using fallback when possible.")

    if use_preprocessed_audio:
        print(f"[{STAGE_NAME}] use_preprocessed_audio=true (preprocessed path is still preferred when available).")
    else:
        print(f"[{STAGE_NAME}] use_preprocessed_audio=false (preprocessed path is still preferred when available).")

    status_records: List[Dict[str, object]] = []
    diar_records: List[Dict[str, object]] = []
    successful_call_ids: List[str] = []

    process_ts = datetime.utcnow()
    for row in eligible_rows:
        call_id = str(row["call_id"])
        audio_path = _canonical_volume_path(str(row["audio_path"]))
        duration_sec = (
            float(row["duration_sec"])
            if row["duration_sec"] is not None
            else None
        )

        try:
            if not enable_diarization and not enable_fallback_segmentation:
                status_records.append(
                    {
                        "call_id": call_id,
                        "stage_name": STAGE_NAME,
                        "status": "SKIPPED",
                        "error_message": "Both diarization and fallback are disabled.",
                        "run_id": run_id,
                        "updated_at": process_ts,
                    }
                )
                continue

            local_audio_path = _to_local_read_path(audio_path)
            if duration_sec is None:
                duration_sec = _load_duration_sec(local_audio_path)

            method_used: Optional[str] = None
            segments: List[Dict[str, object]] = []
            primary_error: Optional[str] = None

            if pyannote_enabled:
                try:
                    segments = _run_pyannote(
                        audio_local_path=local_audio_path,
                        diarization_model_name=diarization_model_name,
                        hf_token=hf_token,
                        min_segment_sec=min_segment_sec,
                        merge_gap_sec=merge_gap_sec,
                        duration_sec=duration_sec,
                    )
                    method_used = "pyannote"
                    pyannote_success_calls += 1
                except Exception as exc:
                    primary_error = _truncate_error(exc, 400)

            if not segments and enable_fallback_segmentation:
                try:
                    if fallback_method == "single_segment_fallback":
                        segments = _single_segment_fallback(duration_sec or 0.0)
                    else:
                        segments = _vad_fallback_segments(
                            audio_local_path=local_audio_path,
                            min_segment_sec=min_segment_sec,
                            merge_gap_sec=merge_gap_sec,
                            duration_sec=duration_sec,
                        )
                    if segments:
                        method_used = fallback_method
                        fallback_success_calls += 1
                except Exception as exc:
                    fallback_err = _truncate_error(exc, 400)
                    if primary_error:
                        raise RuntimeError(
                            f"Primary diarization failed: {primary_error}; fallback failed: {fallback_err}"
                        )
                    raise RuntimeError(f"Fallback segmentation failed: {fallback_err}") from exc

            if not segments:
                if primary_error and not enable_fallback_segmentation:
                    raise RuntimeError(
                        f"Primary diarization failed and fallback is disabled: {primary_error}"
                    )
                raise RuntimeError("No diarization segments produced.")

            ordered_segments = sorted(
                segments,
                key=lambda x: (float(x["start_sec"]), float(x["end_sec"])),
            )

            validated_segments: List[Dict[str, object]] = []
            for idx, seg in enumerate(ordered_segments, start=1):
                start_sec = float(seg["start_sec"])
                end_sec = float(seg["end_sec"])
                speaker_label = str(seg.get("speaker_label") or "SPEAKER_00")
                confidence = seg.get("confidence")

                if duration_sec is not None:
                    start_sec = max(0.0, min(start_sec, duration_sec))
                    end_sec = max(0.0, min(end_sec, duration_sec))

                if end_sec <= start_sec:
                    continue

                validated_segments.append(
                    {
                        "call_id": call_id,
                        "segment_id": f"{call_id}_diar_{idx:05d}",
                        "speaker_label": speaker_label,
                        "start_sec": start_sec,
                        "end_sec": end_sec,
                        "method": method_used,
                        "diarization_method": method_used,
                        "diarization_model": (
                            diarization_model_name if method_used == "pyannote" else None
                        ),
                        "diarization_version": diarization_version,
                        "confidence": (
                            float(confidence) if confidence is not None else None
                        ),
                        "run_id": run_id,
                        "updated_at": process_ts,
                    }
                )

            if not validated_segments:
                raise RuntimeError("No valid segments remained after quality filtering.")

            if duration_sec is not None and duration_sec >= 120.0 and len(validated_segments) <= 1:
                warning_low_segment_calls += 1

            diar_records.extend(validated_segments)
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
    ops_skipped_count = sum(1 for row in status_records if row["status"] == "SKIPPED")

    if diar_records:
        diar_schema = T.StructType(
            [
                T.StructField("call_id", T.StringType(), False),
                T.StructField("segment_id", T.StringType(), False),
                T.StructField("speaker_label", T.StringType(), False),
                T.StructField("start_sec", T.DoubleType(), False),
                T.StructField("end_sec", T.DoubleType(), False),
                T.StructField("method", T.StringType(), True),
                T.StructField("diarization_method", T.StringType(), True),
                T.StructField("diarization_model", T.StringType(), True),
                T.StructField("diarization_version", T.StringType(), False),
                T.StructField("confidence", T.DoubleType(), True),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )
        diar_df = spark.createDataFrame(diar_records, schema=diar_schema)
        success_ids_sql = ", ".join(
            _sql_literal(call_id) for call_id in sorted(set(successful_call_ids))
        )
        if success_ids_sql:
            spark.sql(f"DELETE FROM {diar_table} WHERE call_id IN ({success_ids_sql})")
        diar_df.write.format("delta").mode("append").saveAsTable(diar_table)
        segments_written = len(diar_records)

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
        status_df.createOrReplaceTempView("tmp_foundation_03_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file_status_table} AS t
            USING tmp_foundation_03_status AS s
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

    if eligible_count > 0 and ops_success_count == 0 and ops_failed_count == eligible_count:
        final_status = "FAILED"
        error_summary = "All eligible calls failed during diarize_audio."
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
        f"segments_written={segments_written} pyannote_success_calls={pyannote_success_calls} "
        f"fallback_success_calls={fallback_success_calls} "
        f"warning_low_segment_calls={warning_low_segment_calls} status={final_status}"
    )
