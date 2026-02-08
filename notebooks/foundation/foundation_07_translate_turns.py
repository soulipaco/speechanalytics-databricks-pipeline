# Databricks notebook source
# MAGIC %md
# MAGIC # FOUNDATION_07_TRANSLATE_TURNS
# MAGIC
# MAGIC **Purpose**
# MAGIC - Translate redacted turns into a target language (default `en`) with strict skip logic.
# MAGIC
# MAGIC **Inputs**
# MAGIC - Parameters: `catalog`, `schema`, `run_id`, `run_mode`, `max_files_per_run`, `enable_translation`, `translation_target_language`, `translation_model_name`, `translation_version`, `skip_when_same_language`
# MAGIC - Optional backend params: `translation_endpoint_name`, `translation_service_url`, `translation_service_api_key`, `translation_timeout_sec`
# MAGIC - Table: `<catalog>.<schema>.gold_turns_redacted`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.<schema>.gold_turns_translated`
# MAGIC - `<catalog>.<schema>.ops_file_status` (stage: `translate_turns`)
# MAGIC - `<catalog>.<schema>.ops_pipeline_runs`
# MAGIC
# MAGIC **Key rules**
# MAGIC - Translation input is always `gold_turns_redacted.text_redacted`.
# MAGIC - If `language_final == translation_target_language` and skip is enabled, `translation_skipped_flag=true` and `text_translated=text_redacted_source`.
# MAGIC - Supports `sample | incremental | full` with idempotent per-call overwrite.

# COMMAND ----------

import json
import os
import re
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

from pyspark.sql import functions as F
from pyspark.sql import types as T


WORKFLOW_NAME = "foundation"
STAGE_NAME = "translate_turns"
ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
DEFAULT_TARGET_LANGUAGE = "en"
DEFAULT_TRANSLATION_VERSION = "v1"
DEFAULT_TRANSLATION_TIMEOUT_SEC = 30


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


def _normalize_language_code(value: Optional[str]) -> str:
    text = (value or "").strip().lower()
    if not text:
        return "unknown"
    text = text.replace("_", "-")
    return text.split("-")[0] if "-" in text else text


def _fq_table(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def _table_exists(catalog: str, schema: str, table: str) -> bool:
    return bool(spark.catalog.tableExists(f"{catalog}.{schema}.{table}"))


def _ensure_tables(
    translated_table: str, ops_file_status_table: str, ops_pipeline_runs_table: str
) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {translated_table} (
          call_id STRING,
          turn_id STRING,
          role STRING,
          start_sec DOUBLE,
          end_sec DOUBLE,
          language_final STRING,
          translation_target_language STRING,
          translation_skipped_flag BOOLEAN,
          text_redacted_source STRING,
          text_translated STRING,
          translation_model STRING,
          translation_version STRING,
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


def _extract_text_from_response(payload) -> str:
    if payload is None:
        raise ValueError("Empty translation response payload.")

    if isinstance(payload, str):
        text = payload.strip()
        if not text:
            raise ValueError("Translation response string is empty.")
        return text

    if isinstance(payload, list):
        if not payload:
            raise ValueError("Translation response list is empty.")
        first = payload[0]
        if isinstance(first, dict):
            for key in (
                "translated_text",
                "translation_text",
                "text",
                "output_text",
                "translation",
            ):
                if key in first and first[key]:
                    return str(first[key]).strip()
        return _extract_text_from_response(first)

    if isinstance(payload, dict):
        for key in (
            "translated_text",
            "translation_text",
            "text",
            "output_text",
            "translation",
        ):
            if key in payload and payload[key]:
                return str(payload[key]).strip()
        if "predictions" in payload:
            return _extract_text_from_response(payload["predictions"])
        if "outputs" in payload:
            return _extract_text_from_response(payload["outputs"])

    raise ValueError("Unsupported translation response format.")


def _build_translation_backend(
    translation_model_name: str,
    translation_endpoint_name: str,
    translation_service_url: str,
    translation_service_api_key: str,
    translation_timeout_sec: int,
) -> Tuple[Optional[str], Optional[Callable[[str, Optional[str], str], str]], Optional[str]]:
    endpoint_name = translation_endpoint_name.strip()
    service_url = translation_service_url.strip()

    if endpoint_name or service_url:
        if endpoint_name:
            try:
                import mlflow.deployments  # type: ignore

                client = mlflow.deployments.get_deploy_client("databricks")

                def _translate_endpoint(
                    text: str, source_language: Optional[str], target_language: str
                ) -> str:
                    inputs = {
                        "text": text,
                        "source_language": source_language,
                        "target_language": target_language,
                        "model": translation_model_name,
                    }
                    response = client.predict(endpoint=endpoint_name, inputs=inputs)
                    return _extract_text_from_response(response)

                return f"databricks_endpoint:{endpoint_name}", _translate_endpoint, None
            except Exception as exc:
                return None, None, (
                    "Configured translation endpoint is unavailable: "
                    + _truncate_error(exc, 500)
                )

        if service_url:
            try:
                import requests  # type: ignore

                def _translate_service(
                    text: str, source_language: Optional[str], target_language: str
                ) -> str:
                    headers = {"Content-Type": "application/json"}
                    if translation_service_api_key:
                        headers["Authorization"] = f"Bearer {translation_service_api_key}"
                    payload = {
                        "text": text,
                        "source_language": source_language,
                        "target_language": target_language,
                        "model": translation_model_name,
                    }
                    response = requests.post(
                        service_url,
                        json=payload,
                        headers=headers,
                        timeout=translation_timeout_sec,
                    )
                    response.raise_for_status()
                    try:
                        body = response.json()
                    except Exception:
                        body = response.text
                    return _extract_text_from_response(body)

                return f"translation_service:{service_url}", _translate_service, None
            except Exception as exc:
                return None, None, (
                    "Configured translation service URL is unavailable: "
                    + _truncate_error(exc, 500)
                )

    try:
        from transformers import pipeline  # type: ignore

        translator = pipeline("translation", model=translation_model_name)

        def _translate_transformers(
            text: str, source_language: Optional[str], target_language: str
        ) -> str:
            kwargs: Dict[str, str] = {}
            if source_language and source_language != "unknown":
                kwargs["src_lang"] = source_language
            if target_language:
                kwargs["tgt_lang"] = target_language

            try:
                outputs = translator(text, **kwargs)
            except TypeError:
                outputs = translator(text)
            return _extract_text_from_response(outputs)

        return f"transformers:{translation_model_name}", _translate_transformers, None
    except Exception as exc:
        return None, None, (
            "No translation capability available. Configure endpoint/service or install transformers. "
            + _truncate_error(exc, 500)
        )


if _is_databricks():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("enable_translation", "true")
    dbutils.widgets.text("translation_target_language", DEFAULT_TARGET_LANGUAGE)
    dbutils.widgets.text("translation_model_name", "")
    dbutils.widgets.text("translation_version", DEFAULT_TRANSLATION_VERSION)
    dbutils.widgets.text("skip_when_same_language", "true")
    dbutils.widgets.text("translation_endpoint_name", "")
    dbutils.widgets.text("translation_service_url", "")
    dbutils.widgets.text("translation_service_api_key", "")
    dbutils.widgets.text("translation_timeout_sec", str(DEFAULT_TRANSLATION_TIMEOUT_SEC))

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    enable_translation_raw = dbutils.widgets.get("enable_translation").strip()
    translation_target_language = (
        dbutils.widgets.get("translation_target_language").strip()
        or DEFAULT_TARGET_LANGUAGE
    )
    translation_model_name = dbutils.widgets.get("translation_model_name").strip()
    translation_version = dbutils.widgets.get("translation_version").strip() or DEFAULT_TRANSLATION_VERSION
    skip_when_same_language_raw = dbutils.widgets.get("skip_when_same_language").strip()
    translation_endpoint_name = dbutils.widgets.get("translation_endpoint_name").strip()
    translation_service_url = dbutils.widgets.get("translation_service_url").strip()
    translation_service_api_key = dbutils.widgets.get("translation_service_api_key").strip()
    translation_timeout_sec_raw = dbutils.widgets.get("translation_timeout_sec").strip()
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    enable_translation_raw = os.getenv("ENABLE_TRANSLATION", "true").strip()
    translation_target_language = (
        os.getenv("TRANSLATION_TARGET_LANGUAGE", DEFAULT_TARGET_LANGUAGE).strip()
        or DEFAULT_TARGET_LANGUAGE
    )
    translation_model_name = os.getenv("TRANSLATION_MODEL_NAME", "").strip()
    translation_version = os.getenv("TRANSLATION_VERSION", DEFAULT_TRANSLATION_VERSION).strip() or DEFAULT_TRANSLATION_VERSION
    skip_when_same_language_raw = os.getenv("SKIP_WHEN_SAME_LANGUAGE", "true").strip()
    translation_endpoint_name = os.getenv("TRANSLATION_ENDPOINT_NAME", "").strip()
    translation_service_url = os.getenv("TRANSLATION_SERVICE_URL", "").strip()
    translation_service_api_key = os.getenv("TRANSLATION_SERVICE_API_KEY", "").strip()
    translation_timeout_sec_raw = os.getenv(
        "TRANSLATION_TIMEOUT_SEC", str(DEFAULT_TRANSLATION_TIMEOUT_SEC)
    ).strip()

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

enable_translation = _parse_bool("enable_translation", enable_translation_raw, default=True)
skip_when_same_language = _parse_bool(
    "skip_when_same_language", skip_when_same_language_raw, default=True
)
translation_target_language = _normalize_language_code(translation_target_language)
if enable_translation and not translation_model_name:
    raise ValueError("`translation_model_name` is required when `enable_translation=true`.")

translation_timeout_sec = int(translation_timeout_sec_raw or DEFAULT_TRANSLATION_TIMEOUT_SEC)
if translation_timeout_sec <= 0:
    translation_timeout_sec = DEFAULT_TRANSLATION_TIMEOUT_SEC

input_redacted_table = _fq_table(catalog, schema, "gold_turns_redacted")
aligned_table = _fq_table(catalog, schema, "silver_turns_aligned")
translated_table = _fq_table(catalog, schema, "gold_turns_translated")
ops_file_status_table = _fq_table(catalog, schema, "ops_file_status")
ops_pipeline_runs_table = _fq_table(catalog, schema, "ops_pipeline_runs")

params_snapshot = {
    "catalog": catalog,
    "schema": schema,
    "run_id": run_id,
    "run_mode": run_mode,
    "max_files_per_run": max_files_per_run,
    "enable_translation": enable_translation,
    "translation_target_language": translation_target_language,
    "translation_model_name": translation_model_name,
    "translation_version": translation_version,
    "skip_when_same_language": skip_when_same_language,
    "translation_endpoint_name": translation_endpoint_name,
    "translation_service_url": translation_service_url,
    "translation_timeout_sec": translation_timeout_sec,
}
parameters_json = json.dumps(params_snapshot, sort_keys=True)

print(
    f"[{STAGE_NAME}] Starting with run_id={run_id}, run_mode={run_mode}, "
    f"target={translation_target_language}"
)

_ensure_tables(translated_table, ops_file_status_table, ops_pipeline_runs_table)
_upsert_pipeline_run_running(ops_pipeline_runs_table, run_id, parameters_json)

eligible_count = 0
ops_success_count = 0
ops_failed_count = 0
ops_skipped_count = 0
rows_written = 0
final_status = "SUCCESS"
error_summary: Optional[str] = None

try:
    if not _table_exists(catalog, schema, "gold_turns_redacted"):
        raise RuntimeError(f"Required input table is missing: {input_redacted_table}")

    redacted_df = spark.table(input_redacted_table)
    required_columns = {"call_id", "turn_id", "role", "start_sec", "end_sec", "text_redacted"}
    missing_columns = sorted(required_columns - set(redacted_df.columns))
    if missing_columns:
        raise RuntimeError(
            f"Input table {input_redacted_table} is missing required columns: {missing_columns}"
        )

    input_df = redacted_df.select(
        "call_id",
        "turn_id",
        "role",
        "start_sec",
        "end_sec",
        F.col("text_redacted").alias("text_redacted_source"),
    )

    if "language_final" in redacted_df.columns:
        input_df = input_df.join(
            redacted_df.select("call_id", "turn_id", "language_final"),
            on=["call_id", "turn_id"],
            how="left",
        )
    elif _table_exists(catalog, schema, "silver_turns_aligned"):
        input_df = input_df.join(
            spark.table(aligned_table).select("call_id", "turn_id", "language_final"),
            on=["call_id", "turn_id"],
            how="left",
        )
    else:
        input_df = input_df.withColumn("language_final", F.lit("unknown"))

    stage_df = (
        spark.table(ops_file_status_table)
        .where(F.col("stage_name") == STAGE_NAME)
        .select("call_id", F.col("status").alias("stage_status"))
    )

    call_df = input_df.select("call_id").distinct().orderBy("call_id")
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

    backend_name: Optional[str] = None
    translator_fn: Optional[Callable[[str, Optional[str], str], str]] = None
    translation_backend_error: Optional[str] = None
    if enable_translation:
        backend_name, translator_fn, translation_backend_error = _build_translation_backend(
            translation_model_name=translation_model_name,
            translation_endpoint_name=translation_endpoint_name,
            translation_service_url=translation_service_url,
            translation_service_api_key=translation_service_api_key,
            translation_timeout_sec=translation_timeout_sec,
        )
        if translation_backend_error:
            print(f"[{STAGE_NAME}] Translation backend unavailable: {translation_backend_error}")
        else:
            print(f"[{STAGE_NAME}] Translation backend: {backend_name}")

    output_rows: List[Dict[str, object]] = []
    status_records: List[Dict[str, object]] = []
    successful_call_ids: List[str] = []
    skipped_call_ids: List[str] = []
    process_ts = datetime.utcnow()

    for call_id in eligible_calls:
        try:
            call_rows = (
                input_df.where(F.col("call_id") == call_id)
                .orderBy("start_sec", "end_sec", "turn_id")
                .collect()
            )
            if not call_rows:
                raise ValueError("No redacted turns found for call_id.")

            call_status = "SUCCESS"
            call_output: List[Dict[str, object]] = []

            for row in call_rows:
                source_text = str(row["text_redacted_source"] or "")
                language_final = _normalize_language_code(row["language_final"])

                should_skip_same_language = (
                    skip_when_same_language
                    and language_final != "unknown"
                    and language_final == translation_target_language
                )

                if not enable_translation:
                    skipped_flag = True
                    translated_text = source_text
                elif should_skip_same_language:
                    skipped_flag = True
                    translated_text = source_text
                else:
                    skipped_flag = False
                    if translator_fn is None:
                        raise RuntimeError(
                            translation_backend_error
                            or "No translation backend is available."
                        )
                    translated_text = translator_fn(
                        source_text,
                        None if language_final == "unknown" else language_final,
                        translation_target_language,
                    )
                    translated_text = (translated_text or "").strip()
                    if not translated_text:
                        raise ValueError("Translated text is empty for non-skipped turn.")
                    if len(source_text) >= 120 and len(translated_text) <= 3:
                        print(
                            f"[{STAGE_NAME}] WARNING very short translation for "
                            f"call_id={call_id}, turn_id={row['turn_id']}"
                        )

                if (
                    skip_when_same_language
                    and language_final == translation_target_language
                    and not skipped_flag
                ):
                    raise ValueError(
                        "Skip rule violation: same-language turn was translated."
                    )

                call_output.append(
                    {
                        "call_id": call_id,
                        "turn_id": str(row["turn_id"]),
                        "role": str(row["role"] or "Unknown"),
                        "start_sec": float(row["start_sec"]),
                        "end_sec": float(row["end_sec"]),
                        "language_final": language_final,
                        "translation_target_language": translation_target_language,
                        "translation_skipped_flag": bool(skipped_flag),
                        "text_redacted_source": source_text,
                        "text_translated": translated_text,
                        "translation_model": (
                            translation_model_name
                            if enable_translation
                            else "disabled_passthrough"
                        ),
                        "translation_version": translation_version,
                        "run_id": run_id,
                        "updated_at": process_ts,
                    }
                )

            output_rows.extend(call_output)
            if not enable_translation:
                call_status = "SKIPPED"
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

    if output_rows:
        output_schema = T.StructType(
            [
                T.StructField("call_id", T.StringType(), False),
                T.StructField("turn_id", T.StringType(), False),
                T.StructField("role", T.StringType(), False),
                T.StructField("start_sec", T.DoubleType(), False),
                T.StructField("end_sec", T.DoubleType(), False),
                T.StructField("language_final", T.StringType(), False),
                T.StructField("translation_target_language", T.StringType(), False),
                T.StructField("translation_skipped_flag", T.BooleanType(), False),
                T.StructField("text_redacted_source", T.StringType(), False),
                T.StructField("text_translated", T.StringType(), False),
                T.StructField("translation_model", T.StringType(), False),
                T.StructField("translation_version", T.StringType(), False),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )
        out_df = spark.createDataFrame(output_rows, schema=output_schema)
        completed_call_ids = sorted(set(successful_call_ids + skipped_call_ids))
        if completed_call_ids:
            completed_ids_sql = ", ".join(_sql_literal(call_id) for call_id in completed_call_ids)
            spark.sql(f"DELETE FROM {translated_table} WHERE call_id IN ({completed_ids_sql})")
        out_df.write.format("delta").mode("append").saveAsTable(translated_table)
        rows_written = len(output_rows)

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
        status_df.createOrReplaceTempView("tmp_foundation_07_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file_status_table} AS t
            USING tmp_foundation_07_status AS s
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
        error_summary = "All eligible calls failed in translate_turns."
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
