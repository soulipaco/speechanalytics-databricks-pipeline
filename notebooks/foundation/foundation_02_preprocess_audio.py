# Databricks notebook source
# MAGIC %md
# MAGIC # FOUNDATION_02_PREPROCESS_AUDIO
# MAGIC
# MAGIC **Purpose**
# MAGIC - Standardize raw WAV files to target audio format (default mono / 16kHz).
# MAGIC
# MAGIC **Inputs**
# MAGIC - Parameters: `catalog`, `schema`, `volume_root`, `run_id`, `run_mode`, `max_files_per_run`, `enable_preprocess_audio`, `target_sample_rate`, `target_channels`, `output_dir`, `preprocess_version`
# MAGIC - Table: `<catalog>.<schema>.bronze_audio_files`
# MAGIC - Files: `${volume_root}/bronze/audio_raw/*.wav` via `bronze_audio_files.file_path`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.<schema>.silver_audio_preprocessed`
# MAGIC - `<catalog>.<schema>.ops_file_status` (stage: `preprocess_audio`)
# MAGIC - `<catalog>.<schema>.ops_pipeline_runs`
# MAGIC
# MAGIC **Key rules**
# MAGIC - `sample | incremental | full` supported.
# MAGIC - Incremental/sample: calls with preprocess status missing or FAILED.
# MAGIC - Idempotent per-call overwrite for successful outputs (delete then append).

# COMMAND ----------

import json
import os
import re
import wave
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import functions as F
from pyspark.sql import types as T


WORKFLOW_NAME = "foundation"
STAGE_NAME = "preprocess_audio"
ALLOWED_RUN_MODES = {"sample", "incremental", "full"}


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
    if raw.startswith("dbfs:/"):
        return raw.rstrip("/")
    return raw.rstrip("/")


def _to_dbutils_path(path: str) -> str:
    canonical = _canonical_volume_path(path)
    if canonical.startswith("/Volumes/"):
        return f"dbfs:{canonical}"
    if canonical.startswith("dbfs:/"):
        return canonical
    if canonical.startswith("/dbfs/"):
        return "dbfs:/" + canonical[len("/dbfs/") :]
    return canonical


def _to_local_rw_path(path: str) -> str:
    canonical = _canonical_volume_path(path)
    if canonical.startswith("dbfs:/Volumes/"):
        return "/" + canonical[len("dbfs:/") :]
    if canonical.startswith("dbfs:/"):
        return "/dbfs/" + canonical[len("dbfs:/") :]
    if canonical.startswith("/Volumes/"):
        return canonical
    if canonical.startswith("Volumes/"):
        return "/" + canonical
    return canonical


def _table_exists(catalog: str, schema: str, table: str) -> bool:
    return bool(spark.catalog.tableExists(f"{catalog}.{schema}.{table}"))


def _ensure_tables(
    preprocess_table: str, ops_file_status_table: str, ops_pipeline_runs_table: str
) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {preprocess_table} (
          call_id STRING,
          input_path STRING,
          output_path STRING,
          output_sample_rate INT,
          output_channels INT,
          preprocess_version STRING,
          run_id STRING,
          status STRING,
          error_message STRING,
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


def _convert_wav_to_target(
    input_local_path: str,
    output_local_path: str,
    target_sample_rate: int,
    target_channels: int,
) -> None:
    try:
        import audioop  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Python `audioop` module is unavailable; preprocessing cannot run."
        ) from exc

    with wave.open(input_local_path, "rb") as reader:
        in_channels = int(reader.getnchannels())
        sampwidth = int(reader.getsampwidth())
        in_rate = int(reader.getframerate())
        comp_type = str(reader.getcomptype())
        frames = reader.readframes(reader.getnframes())

    if comp_type != "NONE":
        raise ValueError(f"Unsupported compressed WAV (comptype={comp_type}).")
    if target_channels not in {1, 2}:
        raise ValueError(f"Unsupported target_channels={target_channels}. Allowed: 1 or 2.")
    if in_channels not in {1, 2}:
        raise ValueError(
            f"Unsupported input channel count={in_channels}. Expected mono/stereo WAV."
        )
    if target_sample_rate <= 0:
        raise ValueError(f"Invalid target_sample_rate={target_sample_rate}.")

    converted = frames
    out_channels = in_channels

    if in_channels == 2 and target_channels == 1:
        converted = audioop.tomono(converted, sampwidth, 0.5, 0.5)
        out_channels = 1
    elif in_channels == 1 and target_channels == 2:
        converted = audioop.tostereo(converted, sampwidth, 1.0, 1.0)
        out_channels = 2
    elif in_channels == target_channels:
        out_channels = target_channels
    else:
        raise ValueError(
            f"Unsupported channel conversion {in_channels} -> {target_channels}."
        )

    if in_rate != target_sample_rate:
        converted, _ = audioop.ratecv(
            converted, sampwidth, out_channels, in_rate, target_sample_rate, None
        )

    out_dir = os.path.dirname(output_local_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with wave.open(output_local_path, "wb") as writer:
        writer.setnchannels(target_channels)
        writer.setsampwidth(sampwidth)
        writer.setframerate(target_sample_rate)
        writer.writeframes(converted)


def _verify_output(
    output_local_path: str, target_sample_rate: int, target_channels: int
) -> None:
    if not os.path.exists(output_local_path):
        raise ValueError("Output file does not exist.")
    with wave.open(output_local_path, "rb") as wav:
        out_rate = int(wav.getframerate())
        out_channels = int(wav.getnchannels())
        if out_rate != target_sample_rate:
            raise ValueError(
                f"output_sample_rate mismatch: expected={target_sample_rate}, actual={out_rate}"
            )
        if out_channels != target_channels:
            raise ValueError(
                f"output_channels mismatch: expected={target_channels}, actual={out_channels}"
            )


if _is_databricks():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("volume_root", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("enable_preprocess_audio", "false")
    dbutils.widgets.text("target_sample_rate", "16000")
    dbutils.widgets.text("target_channels", "1")
    dbutils.widgets.text("output_dir", "")
    dbutils.widgets.text("preprocess_version", "v1")

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    volume_root = dbutils.widgets.get("volume_root").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    enable_preprocess_audio_raw = dbutils.widgets.get("enable_preprocess_audio").strip()
    target_sample_rate_raw = dbutils.widgets.get("target_sample_rate").strip()
    target_channels_raw = dbutils.widgets.get("target_channels").strip()
    output_dir_raw = dbutils.widgets.get("output_dir").strip()
    preprocess_version = dbutils.widgets.get("preprocess_version").strip() or "v1"
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    volume_root = os.getenv("VOLUME_ROOT", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    enable_preprocess_audio_raw = os.getenv("ENABLE_PREPROCESS_AUDIO", "false").strip()
    target_sample_rate_raw = os.getenv("TARGET_SAMPLE_RATE", "16000").strip()
    target_channels_raw = os.getenv("TARGET_CHANNELS", "1").strip()
    output_dir_raw = os.getenv("OUTPUT_DIR", "").strip()
    preprocess_version = os.getenv("PREPROCESS_VERSION", "v1").strip() or "v1"

catalog = _validate_identifier("catalog", catalog)
schema = _validate_identifier("schema", schema)
if not volume_root:
    raise ValueError("Parameter `volume_root` is required.")
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid `run_mode`: {run_mode!r}. Allowed: {sorted(ALLOWED_RUN_MODES)}")

max_files_per_run: Optional[int] = None
if max_files_per_run_raw:
    max_files_per_run = int(max_files_per_run_raw)
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError(
        "In sample mode, `max_files_per_run` must be provided as an integer > 0."
    )

enable_preprocess_audio = _parse_bool(
    "enable_preprocess_audio", enable_preprocess_audio_raw, default=False
)
target_sample_rate = int(target_sample_rate_raw or 16000)
target_channels = int(target_channels_raw or 1)
if target_sample_rate <= 0:
    raise ValueError("`target_sample_rate` must be > 0.")
if target_channels <= 0:
    raise ValueError("`target_channels` must be > 0.")

default_output_dir = f"{volume_root.rstrip('/')}/silver/audio_preprocessed/"
output_dir = _canonical_volume_path(output_dir_raw or default_output_dir).rstrip("/")
if not output_dir:
    raise ValueError("`output_dir` resolved to empty path.")

bronze_table = _fq_table(catalog, schema, "bronze_audio_files")
preprocess_table = _fq_table(catalog, schema, "silver_audio_preprocessed")
ops_file_status_table = _fq_table(catalog, schema, "ops_file_status")
ops_pipeline_runs_table = _fq_table(catalog, schema, "ops_pipeline_runs")

params_snapshot = {
    "catalog": catalog,
    "schema": schema,
    "volume_root": volume_root,
    "run_id": run_id,
    "run_mode": run_mode,
    "max_files_per_run": max_files_per_run,
    "enable_preprocess_audio": enable_preprocess_audio,
    "target_sample_rate": target_sample_rate,
    "target_channels": target_channels,
    "output_dir": output_dir,
    "preprocess_version": preprocess_version,
}
parameters_json = json.dumps(params_snapshot, sort_keys=True)

print(
    f"[{STAGE_NAME}] Starting with run_id={run_id}, run_mode={run_mode}, "
    f"enable_preprocess_audio={enable_preprocess_audio}"
)
print(f"[{STAGE_NAME}] output_dir={output_dir}")

_ensure_tables(preprocess_table, ops_file_status_table, ops_pipeline_runs_table)
_upsert_pipeline_run_running(ops_pipeline_runs_table, run_id, parameters_json)

eligible_count = 0
ops_success_count = 0
ops_failed_count = 0
ops_skipped_count = 0
rows_written = 0
final_status = "SUCCESS"
error_summary: Optional[str] = None

try:
    if not _table_exists(catalog, schema, "bronze_audio_files"):
        raise RuntimeError(f"Required input table is missing: {bronze_table}")

    bronze_df = (
        spark.table(bronze_table)
        .select("call_id", "file_path")
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

    eligible_df = eligible_df.select("call_id", "file_path").orderBy("call_id")
    if run_mode == "sample" and max_files_per_run is not None:
        eligible_df = eligible_df.limit(max_files_per_run)

    eligible_rows = eligible_df.collect()
    eligible_count = len(eligible_rows)
    print(f"[{STAGE_NAME}] Eligible calls: {eligible_count}")

    if enable_preprocess_audio and eligible_count > 0:
        try:
            import audioop  # type: ignore  # noqa: F401
        except Exception as exc:
            raise RuntimeError(
                "Conversion library unavailable: Python `audioop` could not be imported."
            ) from exc

    output_dir_dbutils = _to_dbutils_path(output_dir)
    try:
        dbutils.fs.mkdirs(output_dir_dbutils)
    except Exception:
        os.makedirs(_to_local_rw_path(output_dir), exist_ok=True)

    process_ts = datetime.utcnow()
    status_records: List[Dict[str, object]] = []
    output_records: List[Dict[str, object]] = []
    successful_call_ids: List[str] = []
    skipped_call_ids: List[str] = []

    for row in eligible_rows:
        call_id = str(row["call_id"])
        input_path = _canonical_volume_path(str(row["file_path"]))
        output_path = f"{output_dir}/{call_id}_{preprocess_version}.wav"
        try:
            if not enable_preprocess_audio:
                skipped_call_ids.append(call_id)
                status_records.append(
                    {
                        "call_id": call_id,
                        "stage_name": STAGE_NAME,
                        "status": "SKIPPED",
                        "error_message": None,
                        "run_id": run_id,
                        "updated_at": process_ts,
                    }
                )
                continue

            input_local_path = _to_local_rw_path(input_path)
            output_local_path = _to_local_rw_path(output_path)

            _convert_wav_to_target(
                input_local_path=input_local_path,
                output_local_path=output_local_path,
                target_sample_rate=target_sample_rate,
                target_channels=target_channels,
            )
            _verify_output(
                output_local_path=output_local_path,
                target_sample_rate=target_sample_rate,
                target_channels=target_channels,
            )

            output_records.append(
                {
                    "call_id": call_id,
                    "input_path": input_path,
                    "output_path": _canonical_volume_path(output_path),
                    "output_sample_rate": int(target_sample_rate),
                    "output_channels": int(target_channels),
                    "preprocess_version": preprocess_version,
                    "run_id": run_id,
                    "status": "SUCCESS",
                    "error_message": None,
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
    ops_skipped_count = len(skipped_call_ids)
    ops_failed_count = sum(1 for row in status_records if row["status"] == "FAILED")

    if output_records:
        out_schema = T.StructType(
            [
                T.StructField("call_id", T.StringType(), False),
                T.StructField("input_path", T.StringType(), False),
                T.StructField("output_path", T.StringType(), False),
                T.StructField("output_sample_rate", T.IntegerType(), False),
                T.StructField("output_channels", T.IntegerType(), False),
                T.StructField("preprocess_version", T.StringType(), False),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("status", T.StringType(), False),
                T.StructField("error_message", T.StringType(), True),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )
        out_df = spark.createDataFrame(output_records, schema=out_schema)
        success_ids_sql = ", ".join(
            _sql_literal(call_id) for call_id in sorted(set(successful_call_ids))
        )
        if success_ids_sql:
            spark.sql(f"DELETE FROM {preprocess_table} WHERE call_id IN ({success_ids_sql})")
        out_df.write.format("delta").mode("append").saveAsTable(preprocess_table)
        rows_written = len(output_records)

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
        status_df.createOrReplaceTempView("tmp_foundation_02_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file_status_table} AS t
            USING tmp_foundation_02_status AS s
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

    if enable_preprocess_audio and eligible_count > 0 and ops_success_count == 0:
        final_status = "FAILED"
        error_summary = (
            "Zero calls were successfully preprocessed while eligible calls existed."
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
        success_count=ops_success_count + ops_skipped_count,
        failed_count=ops_failed_count,
        error_summary=error_summary,
    )
    print(
        f"[{STAGE_NAME}] eligible={eligible_count} success_calls={ops_success_count} "
        f"skipped_calls={ops_skipped_count} failed_calls={ops_failed_count} "
        f"rows_written={rows_written} status={final_status}"
    )
