# Databricks notebook source
# MAGIC %md
# MAGIC # FOUNDATION_04_TRANSCRIBE_AUDIO
# MAGIC
# MAGIC **Purpose**
# MAGIC - Transcribe eligible calls into time-aligned ASR segments for downstream alignment and analytics.
# MAGIC
# MAGIC **Inputs**
# MAGIC - Parameters: `catalog`, `schema`, `run_id`, `run_mode`, `max_files_per_run`, `asr_model_name`, `compute_type`, `use_preprocessed_audio`, `language_mode`, `forced_language`
# MAGIC - Tables: `<catalog>.<schema>.bronze_audio_files`, optional `<catalog>.<schema>.silver_audio_preprocessed`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.<schema>.silver_asr_segments`
# MAGIC - `<catalog>.<schema>.ops_file_status` (stage: `transcribe_audio`)
# MAGIC - `<catalog>.<schema>.ops_pipeline_runs`
# MAGIC
# MAGIC **Key rules**
# MAGIC - `sample | incremental | full` run modes supported.
# MAGIC - File-level failures are isolated in `ops_file_status`; batch fails only when zero eligible calls succeed.
# MAGIC - Idempotent writes: successful call outputs are overwritten by `call_id` before insert.

# COMMAND ----------

import json
import os
import re
from datetime import datetime
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from pyspark.sql import functions as F
from pyspark.sql import types as T


WORKFLOW_NAME = "foundation"
STAGE_NAME = "transcribe_audio"
ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
ALLOWED_LANGUAGE_MODES = {"auto", "force"}
LOW_COVERAGE_WARNING_THRESHOLD = 0.05


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


def _truncate_error(message: object, max_len: int = 1000) -> str:
    text = str(message).strip() or "Unknown error"
    return text[:max_len]


def _table_exists(catalog: str, schema: str, table: str) -> bool:
    return bool(spark.catalog.tableExists(f"{catalog}.{schema}.{table}"))


def _ensure_tables(
    asr_table: str, ops_file_status_table: str, ops_pipeline_runs_table: str
) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {asr_table} (
          call_id STRING,
          asr_segment_id STRING,
          start_sec DOUBLE,
          end_sec DOUBLE,
          text STRING,
          language_detected STRING,
          asr_model_name STRING,
          compute_type STRING,
          avg_logprob DOUBLE,
          no_speech_prob DOUBLE,
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


def _create_transcriber(
    asr_model_name: str, compute_type: Optional[str]
) -> Tuple[str, Callable[[str, Optional[str]], Tuple[List[Dict[str, object]], Optional[str]]]]:
    init_errors: List[str] = []

    try:
        from faster_whisper import WhisperModel  # type: ignore

        kwargs: Dict[str, object] = {}
        if compute_type:
            kwargs["compute_type"] = compute_type
        model = WhisperModel(asr_model_name, **kwargs)

        def _transcribe_faster_whisper(
            local_audio_path: str, language: Optional[str]
        ) -> Tuple[List[Dict[str, object]], Optional[str]]:
            segments_iter, info = model.transcribe(
                local_audio_path,
                language=language,
                task="transcribe",
            )
            output: List[Dict[str, object]] = []
            for seg in segments_iter:
                output.append(
                    {
                        "start_sec": float(seg.start),
                        "end_sec": float(seg.end),
                        "text": (seg.text or "").strip(),
                        "avg_logprob": (
                            float(seg.avg_logprob)
                            if getattr(seg, "avg_logprob", None) is not None
                            else None
                        ),
                        "no_speech_prob": (
                            float(seg.no_speech_prob)
                            if getattr(seg, "no_speech_prob", None) is not None
                            else None
                        ),
                    }
                )
            detected_language = getattr(info, "language", None)
            return output, detected_language

        return "faster_whisper", _transcribe_faster_whisper
    except Exception as exc:  # pragma: no cover - runtime behavior in Databricks
        init_errors.append(f"faster_whisper: {_truncate_error(exc, 300)}")

    try:
        import whisper  # type: ignore

        model = whisper.load_model(asr_model_name)

        def _transcribe_openai_whisper(
            local_audio_path: str, language: Optional[str]
        ) -> Tuple[List[Dict[str, object]], Optional[str]]:
            result = model.transcribe(
                local_audio_path,
                language=language,
                task="transcribe",
                verbose=False,
            )
            output: List[Dict[str, object]] = []
            for seg in result.get("segments", []):
                output.append(
                    {
                        "start_sec": float(seg.get("start", 0.0)),
                        "end_sec": float(seg.get("end", 0.0)),
                        "text": (seg.get("text") or "").strip(),
                        "avg_logprob": (
                            float(seg.get("avg_logprob"))
                            if seg.get("avg_logprob") is not None
                            else None
                        ),
                        "no_speech_prob": (
                            float(seg.get("no_speech_prob"))
                            if seg.get("no_speech_prob") is not None
                            else None
                        ),
                    }
                )
            detected_language = result.get("language")
            return output, detected_language

        return "openai_whisper", _transcribe_openai_whisper
    except Exception as exc:  # pragma: no cover - runtime behavior in Databricks
        init_errors.append(f"openai_whisper: {_truncate_error(exc, 300)}")

    raise RuntimeError(
        "Could not initialize ASR backend. "
        "Install `faster-whisper` or `openai-whisper`. "
        + " | ".join(init_errors)
    )


if _is_databricks():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("asr_model_name", "base")
    dbutils.widgets.text("compute_type", "")
    dbutils.widgets.text("use_preprocessed_audio", "false")
    dbutils.widgets.text("language_mode", "auto")
    dbutils.widgets.text("forced_language", "")

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    asr_model_name = dbutils.widgets.get("asr_model_name").strip()
    compute_type = dbutils.widgets.get("compute_type").strip()
    use_preprocessed_audio_raw = dbutils.widgets.get("use_preprocessed_audio").strip()
    language_mode = dbutils.widgets.get("language_mode").strip().lower()
    forced_language = dbutils.widgets.get("forced_language").strip().lower()
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    asr_model_name = os.getenv("ASR_MODEL_NAME", "base").strip()
    compute_type = os.getenv("COMPUTE_TYPE", "").strip()
    use_preprocessed_audio_raw = os.getenv("USE_PREPROCESSED_AUDIO", "false").strip()
    language_mode = os.getenv("LANGUAGE_MODE", "auto").strip().lower()
    forced_language = os.getenv("FORCED_LANGUAGE", "").strip().lower()

catalog = _validate_identifier("catalog", catalog)
schema = _validate_identifier("schema", schema)
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid `run_mode`: {run_mode!r}. Allowed: {sorted(ALLOWED_RUN_MODES)}")
if not asr_model_name:
    raise ValueError("Parameter `asr_model_name` is required.")
if language_mode not in ALLOWED_LANGUAGE_MODES:
    raise ValueError(
        f"Invalid `language_mode`: {language_mode!r}. Allowed: {sorted(ALLOWED_LANGUAGE_MODES)}"
    )
if language_mode == "force" and not forced_language:
    raise ValueError("`forced_language` is required when `language_mode=force`.")

use_preprocessed_audio = _parse_bool(
    "use_preprocessed_audio", use_preprocessed_audio_raw, default=False
)
max_files_per_run: Optional[int] = None
if max_files_per_run_raw:
    max_files_per_run = int(max_files_per_run_raw)
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError(
        "In sample mode, `max_files_per_run` must be provided as an integer > 0."
    )

bronze_table = _fq_table(catalog, schema, "bronze_audio_files")
preprocessed_table = _fq_table(catalog, schema, "silver_audio_preprocessed")
asr_table = _fq_table(catalog, schema, "silver_asr_segments")
ops_file_status_table = _fq_table(catalog, schema, "ops_file_status")
ops_pipeline_runs_table = _fq_table(catalog, schema, "ops_pipeline_runs")

params_snapshot = {
    "catalog": catalog,
    "schema": schema,
    "run_id": run_id,
    "run_mode": run_mode,
    "max_files_per_run": max_files_per_run,
    "asr_model_name": asr_model_name,
    "compute_type": compute_type,
    "use_preprocessed_audio": use_preprocessed_audio,
    "language_mode": language_mode,
    "forced_language": forced_language if language_mode == "force" else None,
}
parameters_json = json.dumps(params_snapshot, sort_keys=True)

print(
    f"[{STAGE_NAME}] Starting with run_id={run_id}, run_mode={run_mode}, "
    f"asr_model_name={asr_model_name}"
)

_ensure_tables(asr_table, ops_file_status_table, ops_pipeline_runs_table)
_upsert_pipeline_run_running(ops_pipeline_runs_table, run_id, parameters_json)

eligible_count = 0
ops_success_count = 0
ops_failed_count = 0
segments_written = 0
final_status = "SUCCESS"
error_summary: Optional[str] = None

try:
    bronze_df = (
        spark.table(bronze_table)
        .select("call_id", "file_path", "duration_sec", "status")
        .where("call_id IS NOT NULL AND file_path IS NOT NULL")
    )

    if run_mode in {"sample", "incremental"}:
        bronze_df = bronze_df.where(
            F.upper(F.col("status")).isin("NEW", "PROCESSED")
        )

    ops_stage_df = (
        spark.table(ops_file_status_table)
        .where(F.col("stage_name") == STAGE_NAME)
        .select("call_id", F.col("status").alias("stage_status"))
    )

    eligible_df = bronze_df.alias("b").join(ops_stage_df.alias("o"), on="call_id", how="left")
    if run_mode in {"sample", "incremental"}:
        eligible_df = eligible_df.where(
            F.col("stage_status").isNull() | (F.upper(F.col("stage_status")) == "FAILED")
        )

    eligible_df = eligible_df.select("call_id", "file_path", "duration_sec").orderBy("call_id")

    if use_preprocessed_audio:
        if not _table_exists(catalog, schema, "silver_audio_preprocessed"):
            raise RuntimeError(
                "`use_preprocessed_audio=true` but "
                f"{preprocessed_table} does not exist."
            )
        pre_df = spark.table(preprocessed_table)
        pre_cols = set(pre_df.columns)
        required_cols = {"call_id", "output_path"}
        missing_cols = sorted(required_cols - pre_cols)
        if missing_cols:
            raise RuntimeError(
                f"{preprocessed_table} is missing required columns: {missing_cols}"
            )

        if "status" in pre_cols:
            pre_df = pre_df.where(F.upper(F.col("status")) == "SUCCESS")

        pre_df = pre_df.select(
            "call_id",
            F.col("output_path").alias("preprocessed_path"),
        )

        eligible_df = (
            eligible_df.join(pre_df, on="call_id", how="left")
            .withColumn("audio_path", F.coalesce(F.col("preprocessed_path"), F.col("file_path")))
            .drop("preprocessed_path")
            .drop("file_path")
            .withColumnRenamed("audio_path", "file_path")
        )

    if run_mode == "sample" and max_files_per_run is not None:
        eligible_df = eligible_df.limit(max_files_per_run)

    eligible_rows = eligible_df.collect()
    eligible_count = len(eligible_rows)
    print(f"[{STAGE_NAME}] Eligible calls: {eligible_count}")

    status_records: List[Dict[str, object]] = []
    segment_records: List[Dict[str, object]] = []
    successful_call_ids: List[str] = []

    transcriber_backend: Optional[str] = None
    transcribe_fn: Optional[
        Callable[[str, Optional[str]], Tuple[List[Dict[str, object]], Optional[str]]]
    ] = None
    transcriber_error: Optional[str] = None

    if eligible_count > 0:
        try:
            transcriber_backend, transcribe_fn = _create_transcriber(
                asr_model_name=asr_model_name,
                compute_type=compute_type or None,
            )
            print(f"[{STAGE_NAME}] ASR backend initialized: {transcriber_backend}")
        except Exception as exc:
            transcriber_error = _truncate_error(exc)
            print(f"[{STAGE_NAME}] ASR backend initialization failed: {transcriber_error}")

    process_ts = datetime.utcnow()
    for row in eligible_rows:
        call_id = str(row["call_id"])
        file_path = _canonical_volume_path(str(row["file_path"]))
        duration_sec = float(row["duration_sec"]) if row["duration_sec"] is not None else None
        try:
            if transcriber_error is not None or transcribe_fn is None:
                raise RuntimeError(transcriber_error or "ASR backend is unavailable.")

            local_audio_path = _to_local_read_path(file_path)
            model_language = forced_language if language_mode == "force" else None
            raw_segments, detected_language = transcribe_fn(local_audio_path, model_language)
            if language_mode == "force":
                detected_language = forced_language

            cleaned_segments: List[Dict[str, object]] = []
            spoken_duration = 0.0
            for seg in raw_segments:
                start_sec = float(seg.get("start_sec", 0.0))
                end_sec = float(seg.get("end_sec", 0.0))
                text = str(seg.get("text") or "").strip()

                if end_sec <= start_sec:
                    raise ValueError(
                        f"Invalid ASR segment timing for call_id={call_id}: "
                        f"start_sec={start_sec}, end_sec={end_sec}"
                    )
                if not text:
                    continue

                spoken_duration += end_sec - start_sec
                cleaned_segments.append(
                    {
                        "start_sec": start_sec,
                        "end_sec": end_sec,
                        "text": text,
                        "avg_logprob": seg.get("avg_logprob"),
                        "no_speech_prob": seg.get("no_speech_prob"),
                    }
                )

            if not cleaned_segments:
                raise ValueError("No valid speech segments produced.")

            if duration_sec and duration_sec > 0:
                coverage_ratio = spoken_duration / duration_sec
                if coverage_ratio < LOW_COVERAGE_WARNING_THRESHOLD:
                    print(
                        f"[{STAGE_NAME}] WARNING low transcript coverage for call_id={call_id}: "
                        f"{coverage_ratio:.4f}"
                    )

            for idx, seg in enumerate(cleaned_segments, start=1):
                segment_records.append(
                    {
                        "call_id": call_id,
                        "asr_segment_id": f"{call_id}_seg_{idx:05d}",
                        "start_sec": float(seg["start_sec"]),
                        "end_sec": float(seg["end_sec"]),
                        "text": str(seg["text"]),
                        "language_detected": detected_language,
                        "asr_model_name": asr_model_name,
                        "compute_type": compute_type or None,
                        "avg_logprob": (
                            float(seg["avg_logprob"])
                            if seg["avg_logprob"] is not None
                            else None
                        ),
                        "no_speech_prob": (
                            float(seg["no_speech_prob"])
                            if seg["no_speech_prob"] is not None
                            else None
                        ),
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

    if segment_records:
        asr_schema = T.StructType(
            [
                T.StructField("call_id", T.StringType(), False),
                T.StructField("asr_segment_id", T.StringType(), False),
                T.StructField("start_sec", T.DoubleType(), False),
                T.StructField("end_sec", T.DoubleType(), False),
                T.StructField("text", T.StringType(), False),
                T.StructField("language_detected", T.StringType(), True),
                T.StructField("asr_model_name", T.StringType(), False),
                T.StructField("compute_type", T.StringType(), True),
                T.StructField("avg_logprob", T.DoubleType(), True),
                T.StructField("no_speech_prob", T.DoubleType(), True),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )
        asr_df = spark.createDataFrame(segment_records, schema=asr_schema)
        successful_call_ids_sql = ", ".join(
            _sql_literal(call_id) for call_id in sorted(set(successful_call_ids))
        )
        if successful_call_ids_sql:
            spark.sql(
                f"DELETE FROM {asr_table} WHERE call_id IN ({successful_call_ids_sql})"
            )
        asr_df.write.format("delta").mode("append").saveAsTable(asr_table)
        segments_written = len(segment_records)

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
        status_df.createOrReplaceTempView("tmp_foundation_04_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file_status_table} AS t
            USING tmp_foundation_04_status AS s
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
            "Zero calls were successfully transcribed while eligible calls existed."
        )
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
        f"failed_calls={ops_failed_count} segments_written={segments_written} "
        f"status={final_status}"
    )
