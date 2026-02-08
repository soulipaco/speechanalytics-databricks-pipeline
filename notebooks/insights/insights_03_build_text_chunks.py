# Databricks notebook source
# MAGIC %md
# MAGIC # INSIGHTS_03_BUILD_TEXT_CHUNKS
# MAGIC
# MAGIC **Purpose**
# MAGIC - Build deterministic, bounded text chunks from conversation turns for retrieval and LLM stages.
# MAGIC
# MAGIC **Inputs**
# MAGIC - Parameters: `catalog`, `schema`, `run_id`, `run_mode`, `max_files_per_run`, `translation_enabled`, `chunking_strategy`, `max_turns_per_chunk`, `max_seconds_per_chunk`, `chunking_version`, `include_role_prefix`, `enable_chunking`
# MAGIC - Preferred source: `<catalog>.<schema>.gold_turns_translated` (if translation enabled and available)
# MAGIC - Fallback source: `<catalog>.<schema>.gold_turns_redacted`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.<schema>.silver_text_chunks`
# MAGIC - `<catalog>.<schema>.ops_file_status` (stage: `insights_03_build_text_chunks`)
# MAGIC - `<catalog>.<schema>.ops_pipeline_runs` (workflow: `insights`)
# MAGIC
# MAGIC **Key rules**
# MAGIC - Supports `sample | incremental | full`.
# MAGIC - Idempotent write by successful `call_id + chunking_version`.
# MAGIC - Per-call failure isolation; stage fails only if eligible calls exist and zero complete.

# COMMAND ----------

import hashlib
import json
import os
import re
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


WORKFLOW_NAME = "insights"
STAGE_NAME = "insights_03_build_text_chunks"
ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
ALLOWED_CHUNKING_STRATEGIES = {"turn_count", "time_window", "hybrid"}
DEFAULT_CHUNKING_STRATEGY = "turn_count"
DEFAULT_MAX_TURNS_PER_CHUNK = 12
DEFAULT_MAX_SECONDS_PER_CHUNK = 90
DEFAULT_CHUNK_VERSION = "v1"


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


def _normalize_role(value: Optional[object]) -> str:
    role = str(value or "").strip().lower()
    if role == "agent":
        return "Agent"
    if role == "customer":
        return "Customer"
    return "Unknown"


def _build_chunk_id(
    call_id: str,
    chunk_index: int,
    chunking_version: str,
    chunking_strategy: str,
    start_turn_id: str,
    end_turn_id: str,
    start_sec: float,
    end_sec: float,
) -> str:
    seed = (
        f"{call_id}|{chunking_version}|{chunking_strategy}|{chunk_index}|"
        f"{start_turn_id}|{end_turn_id}|{start_sec:.6f}|{end_sec:.6f}"
    )
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"{call_id}_{chunk_index:04d}_{digest}"


def _chunk_turns(
    turns: List[Dict[str, object]],
    chunking_strategy: str,
    max_turns_per_chunk: int,
    max_seconds_per_chunk: int,
) -> List[List[Dict[str, object]]]:
    if not turns:
        return []

    chunks: List[List[Dict[str, object]]] = []

    if chunking_strategy == "turn_count":
        for start in range(0, len(turns), max_turns_per_chunk):
            chunks.append(turns[start : start + max_turns_per_chunk])
        return chunks

    if chunking_strategy == "time_window":
        current = [turns[0]]
        window_start = float(turns[0]["start_sec"])
        for turn in turns[1:]:
            candidate_end = float(turn["end_sec"])
            if (candidate_end - window_start) <= max_seconds_per_chunk:
                current.append(turn)
                continue
            chunks.append(current)
            current = [turn]
            window_start = float(turn["start_sec"])
        if current:
            chunks.append(current)
        return chunks

    if chunking_strategy == "hybrid":
        current: List[Dict[str, object]] = []
        for turn in turns:
            if not current:
                current.append(turn)
                continue

            would_exceed_turns = len(current) >= max_turns_per_chunk
            chunk_start = float(current[0]["start_sec"])
            candidate_end = float(turn["end_sec"])
            would_exceed_time = (candidate_end - chunk_start) > max_seconds_per_chunk

            if would_exceed_turns or would_exceed_time:
                chunks.append(current)
                current = [turn]
            else:
                current.append(turn)

        if current:
            chunks.append(current)
        return chunks

    raise ValueError(
        f"Unsupported `chunking_strategy`: {chunking_strategy!r}. "
        f"Allowed: {sorted(ALLOWED_CHUNKING_STRATEGIES)}"
    )


def _ensure_tables(
    chunks_table: str, ops_file_status_table: str, ops_pipeline_runs_table: str
) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {chunks_table} (
          call_id STRING,
          chunk_id STRING,
          chunk_index BIGINT,
          start_sec DOUBLE,
          end_sec DOUBLE,
          chunk_start_sec DOUBLE,
          chunk_end_sec DOUBLE,
          start_turn_id STRING,
          end_turn_id STRING,
          chunk_text STRING,
          chunk_source STRING,
          source_language STRING,
          target_language STRING,
          turn_count_in_chunk BIGINT,
          agent_turn_count BIGINT,
          customer_turn_count BIGINT,
          unknown_turn_count BIGINT,
          char_count BIGINT,
          token_count BIGINT,
          chunking_strategy STRING,
          chunking_version STRING,
          include_role_prefix BOOLEAN,
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


if _is_databricks():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("translation_enabled", "true")
    dbutils.widgets.text("chunking_strategy", DEFAULT_CHUNKING_STRATEGY)
    dbutils.widgets.text("max_turns_per_chunk", str(DEFAULT_MAX_TURNS_PER_CHUNK))
    dbutils.widgets.text("max_seconds_per_chunk", str(DEFAULT_MAX_SECONDS_PER_CHUNK))
    dbutils.widgets.text("chunking_version", DEFAULT_CHUNK_VERSION)
    dbutils.widgets.text("chunk_version", DEFAULT_CHUNK_VERSION)
    dbutils.widgets.text("chunks_version", "")
    dbutils.widgets.text("include_role_prefix", "true")
    dbutils.widgets.text("enable_chunking", "true")

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    translation_enabled_raw = dbutils.widgets.get("translation_enabled").strip()
    chunking_strategy = dbutils.widgets.get("chunking_strategy").strip().lower()
    max_turns_per_chunk_raw = dbutils.widgets.get("max_turns_per_chunk").strip()
    max_seconds_per_chunk_raw = dbutils.widgets.get("max_seconds_per_chunk").strip()
    chunking_version_raw = dbutils.widgets.get("chunking_version").strip()
    chunk_version = dbutils.widgets.get("chunk_version").strip()
    chunks_version_alias = dbutils.widgets.get("chunks_version").strip()
    include_role_prefix_raw = dbutils.widgets.get("include_role_prefix").strip()
    enable_chunking_raw = dbutils.widgets.get("enable_chunking").strip()
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    translation_enabled_raw = os.getenv("TRANSLATION_ENABLED", "true").strip()
    chunking_strategy = (
        os.getenv("CHUNKING_STRATEGY", DEFAULT_CHUNKING_STRATEGY).strip().lower()
    )
    max_turns_per_chunk_raw = os.getenv(
        "MAX_TURNS_PER_CHUNK", str(DEFAULT_MAX_TURNS_PER_CHUNK)
    ).strip()
    max_seconds_per_chunk_raw = os.getenv(
        "MAX_SECONDS_PER_CHUNK", str(DEFAULT_MAX_SECONDS_PER_CHUNK)
    ).strip()
    chunking_version_raw = os.getenv("CHUNKING_VERSION", DEFAULT_CHUNK_VERSION).strip()
    chunk_version = os.getenv("CHUNK_VERSION", DEFAULT_CHUNK_VERSION).strip()
    chunks_version_alias = os.getenv("CHUNKS_VERSION", "").strip()
    include_role_prefix_raw = os.getenv("INCLUDE_ROLE_PREFIX", "true").strip()
    enable_chunking_raw = os.getenv("ENABLE_CHUNKING", "true").strip()

catalog = _validate_identifier("catalog", catalog)
schema = _validate_identifier("schema", schema)
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid `run_mode`: {run_mode!r}. Allowed: {sorted(ALLOWED_RUN_MODES)}")
if chunking_strategy not in ALLOWED_CHUNKING_STRATEGIES:
    raise ValueError(
        f"Invalid `chunking_strategy`: {chunking_strategy!r}. "
        f"Allowed: {sorted(ALLOWED_CHUNKING_STRATEGIES)}"
    )

max_files_per_run: Optional[int] = None
if max_files_per_run_raw:
    max_files_per_run = int(max_files_per_run_raw)
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError("In sample mode, `max_files_per_run` must be provided as an integer > 0.")

translation_enabled = _parse_bool("translation_enabled", translation_enabled_raw, default=True)
include_role_prefix = _parse_bool("include_role_prefix", include_role_prefix_raw, default=True)
enable_chunking = _parse_bool("enable_chunking", enable_chunking_raw, default=True)

max_turns_per_chunk = int(max_turns_per_chunk_raw or DEFAULT_MAX_TURNS_PER_CHUNK)
max_seconds_per_chunk = int(max_seconds_per_chunk_raw or DEFAULT_MAX_SECONDS_PER_CHUNK)
if chunking_strategy in {"turn_count", "hybrid"} and max_turns_per_chunk <= 0:
    raise ValueError("`max_turns_per_chunk` must be > 0 for selected chunking strategy.")
if chunking_strategy in {"time_window", "hybrid"} and max_seconds_per_chunk <= 0:
    raise ValueError("`max_seconds_per_chunk` must be > 0 for selected chunking strategy.")

chunking_version = (
    chunking_version_raw
    or chunk_version
    or chunks_version_alias
    or DEFAULT_CHUNK_VERSION
).strip()
if not chunking_version:
    raise ValueError("Parameter `chunking_version` is required.")

translated_turns_table = _fq_table(catalog, schema, "gold_turns_translated")
redacted_turns_table = _fq_table(catalog, schema, "gold_turns_redacted")
chunks_table = _fq_table(catalog, schema, "silver_text_chunks")
ops_file_status_table = _fq_table(catalog, schema, "ops_file_status")
ops_pipeline_runs_table = _fq_table(catalog, schema, "ops_pipeline_runs")

if translation_enabled and _table_exists(catalog, schema, "gold_turns_translated"):
    source_table_name = "gold_turns_translated"
    source_turns_table = translated_turns_table
    source_text_column = "text_translated"
    chunk_source = "translated"
else:
    source_table_name = "gold_turns_redacted"
    source_turns_table = redacted_turns_table
    source_text_column = "text_redacted"
    chunk_source = "redacted"

params_snapshot = {
    "catalog": catalog,
    "schema": schema,
    "run_id": run_id,
    "run_mode": run_mode,
    "max_files_per_run": max_files_per_run,
    "translation_enabled": translation_enabled,
    "source_table_name": source_table_name,
    "chunking_strategy": chunking_strategy,
    "max_turns_per_chunk": max_turns_per_chunk,
    "max_seconds_per_chunk": max_seconds_per_chunk,
    "chunking_version": chunking_version,
    "include_role_prefix": include_role_prefix,
    "enable_chunking": enable_chunking,
}
parameters_json = json.dumps(params_snapshot, sort_keys=True)

print(
    f"[{STAGE_NAME}] Starting with run_id={run_id}, run_mode={run_mode}, "
    f"source_table={source_table_name}, chunking_strategy={chunking_strategy}, "
    f"chunking_version={chunking_version}"
)

_ensure_tables(chunks_table, ops_file_status_table, ops_pipeline_runs_table)
_upsert_pipeline_run_running(ops_pipeline_runs_table, run_id, parameters_json)

eligible_count = 0
ops_success_count = 0
ops_failed_count = 0
ops_skipped_count = 0
rows_written = 0
small_text_chunk_count = 0
final_status = "SUCCESS"
error_summary: Optional[str] = None

try:
    if not _table_exists(catalog, schema, source_table_name):
        if source_table_name == "gold_turns_redacted":
            raise RuntimeError(f"Required input table is missing: {redacted_turns_table}")
        raise RuntimeError(
            f"Preferred input table missing and no fallback available: {source_turns_table}"
        )

    input_df = spark.table(source_turns_table)
    required_columns = {"call_id", "role", "start_sec", "end_sec", source_text_column}
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
        F.col(source_text_column).cast("string").alias("text_for_chunk"),
        (
            F.col("turn_id").cast("string")
            if "turn_id" in input_df.columns
            else F.lit(None).cast("string")
        ).alias("turn_id"),
        (
            F.col("language_final").cast("string")
            if "language_final" in input_df.columns
            else F.lit("unknown").cast("string")
        ).alias("source_language"),
        (
            F.col("translation_target_language").cast("string")
            if "translation_target_language" in input_df.columns
            else F.lit(None).cast("string")
        ).alias("target_language"),
    ).where("call_id IS NOT NULL")

    if "turn_id" not in input_df.columns:
        order_window = Window.partitionBy("call_id").orderBy("start_sec", "end_sec")
        turns_df = turns_df.withColumn(
            "turn_id", F.concat(F.lit("auto_"), F.row_number().over(order_window).cast("string"))
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

    status_records: List[Dict[str, object]] = []
    chunk_rows: List[Dict[str, object]] = []
    successful_call_ids: List[str] = []
    process_ts = datetime.utcnow()

    for call_id in eligible_calls:
        if not enable_chunking:
            ops_skipped_count += 1
            status_records.append(
                {
                    "call_id": str(call_id),
                    "stage_name": STAGE_NAME,
                    "status": "SKIPPED",
                    "error_message": "Chunking disabled by `enable_chunking=false`.",
                    "run_id": run_id,
                    "updated_at": process_ts,
                }
            )
            continue

        try:
            call_turn_rows = (
                turns_df.where(F.col("call_id") == call_id)
                .orderBy("start_sec", "end_sec", "turn_id")
                .collect()
            )
            if not call_turn_rows:
                raise ValueError("No turns found for call_id.")

            usable_turns: List[Dict[str, object]] = []
            for row in call_turn_rows:
                text = str(row["text_for_chunk"] or "").strip()
                start_sec = row["start_sec"]
                end_sec = row["end_sec"]
                if not text:
                    continue
                if start_sec is None or end_sec is None:
                    continue
                start_value = float(start_sec)
                end_value = float(end_sec)
                if end_value <= start_value:
                    continue
                usable_turns.append(
                    {
                        "call_id": str(call_id),
                        "turn_id": str(row["turn_id"] or ""),
                        "role": _normalize_role(row["role"]),
                        "start_sec": start_value,
                        "end_sec": end_value,
                        "text": text,
                        "source_language": str(row["source_language"] or "unknown"),
                        "target_language": (
                            str(row["target_language"]).strip()
                            if row["target_language"] is not None
                            else None
                        ),
                    }
                )

            if not usable_turns:
                raise ValueError("No usable turns (non-empty text with valid timestamps).")

            chunks = _chunk_turns(
                turns=usable_turns,
                chunking_strategy=chunking_strategy,
                max_turns_per_chunk=max_turns_per_chunk,
                max_seconds_per_chunk=max_seconds_per_chunk,
            )
            if not chunks:
                raise ValueError("Chunking produced zero chunks.")

            call_chunk_rows: List[Dict[str, object]] = []
            for chunk_index, chunk_turns in enumerate(chunks, start=1):
                chunk_start_sec = float(min(turn["start_sec"] for turn in chunk_turns))
                chunk_end_sec = float(max(turn["end_sec"] for turn in chunk_turns))
                if chunk_end_sec <= chunk_start_sec:
                    raise ValueError(
                        f"Invalid chunk timings for chunk {chunk_index}: "
                        f"{chunk_start_sec} >= {chunk_end_sec}"
                    )

                role_lines: List[str] = []
                role_counts = {"Agent": 0, "Customer": 0, "Unknown": 0}
                for turn in chunk_turns:
                    role = str(turn["role"])
                    role_counts[role] += 1
                    text = str(turn["text"])
                    if include_role_prefix:
                        role_lines.append(f"{role.upper()}: {text}")
                    else:
                        role_lines.append(text)

                chunk_text = "\n".join(role_lines).strip()
                if not chunk_text:
                    raise ValueError(f"Chunk text empty for chunk {chunk_index}.")

                char_count = len(chunk_text)
                token_count = len(re.findall(r"\S+", chunk_text))
                if char_count < 20:
                    small_text_chunk_count += 1

                start_turn_id = str(chunk_turns[0]["turn_id"])
                end_turn_id = str(chunk_turns[-1]["turn_id"])

                source_language = "unknown"
                for turn in chunk_turns:
                    candidate = str(turn.get("source_language") or "").strip()
                    if candidate:
                        source_language = candidate
                        break

                target_language: Optional[str] = None
                for turn in chunk_turns:
                    candidate = turn.get("target_language")
                    if candidate is not None:
                        candidate_text = str(candidate).strip()
                        if candidate_text:
                            target_language = candidate_text
                            break

                chunk_id = _build_chunk_id(
                    call_id=str(call_id),
                    chunk_index=chunk_index,
                    chunking_version=chunking_version,
                    chunking_strategy=chunking_strategy,
                    start_turn_id=start_turn_id,
                    end_turn_id=end_turn_id,
                    start_sec=chunk_start_sec,
                    end_sec=chunk_end_sec,
                )

                call_chunk_rows.append(
                    {
                        "call_id": str(call_id),
                        "chunk_id": chunk_id,
                        "chunk_index": int(chunk_index),
                        "start_sec": chunk_start_sec,
                        "end_sec": chunk_end_sec,
                        "chunk_start_sec": chunk_start_sec,
                        "chunk_end_sec": chunk_end_sec,
                        "start_turn_id": start_turn_id,
                        "end_turn_id": end_turn_id,
                        "chunk_text": chunk_text,
                        "chunk_source": chunk_source,
                        "source_language": source_language,
                        "target_language": target_language,
                        "turn_count_in_chunk": int(len(chunk_turns)),
                        "agent_turn_count": int(role_counts["Agent"]),
                        "customer_turn_count": int(role_counts["Customer"]),
                        "unknown_turn_count": int(role_counts["Unknown"]),
                        "char_count": int(char_count),
                        "token_count": int(token_count),
                        "chunking_strategy": chunking_strategy,
                        "chunking_version": chunking_version,
                        "include_role_prefix": bool(include_role_prefix),
                        "run_id": run_id,
                        "updated_at": process_ts,
                    }
                )

            if not call_chunk_rows:
                raise ValueError("No valid chunks produced for call.")

            chunk_rows.extend(call_chunk_rows)
            successful_call_ids.append(str(call_id))
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

    ops_success_count = len(successful_call_ids)
    ops_failed_count = sum(1 for row in status_records if row["status"] == "FAILED")

    if chunk_rows:
        chunk_schema = T.StructType(
            [
                T.StructField("call_id", T.StringType(), False),
                T.StructField("chunk_id", T.StringType(), False),
                T.StructField("chunk_index", T.LongType(), False),
                T.StructField("start_sec", T.DoubleType(), False),
                T.StructField("end_sec", T.DoubleType(), False),
                T.StructField("chunk_start_sec", T.DoubleType(), False),
                T.StructField("chunk_end_sec", T.DoubleType(), False),
                T.StructField("start_turn_id", T.StringType(), False),
                T.StructField("end_turn_id", T.StringType(), False),
                T.StructField("chunk_text", T.StringType(), False),
                T.StructField("chunk_source", T.StringType(), False),
                T.StructField("source_language", T.StringType(), False),
                T.StructField("target_language", T.StringType(), True),
                T.StructField("turn_count_in_chunk", T.LongType(), False),
                T.StructField("agent_turn_count", T.LongType(), False),
                T.StructField("customer_turn_count", T.LongType(), False),
                T.StructField("unknown_turn_count", T.LongType(), False),
                T.StructField("char_count", T.LongType(), False),
                T.StructField("token_count", T.LongType(), False),
                T.StructField("chunking_strategy", T.StringType(), False),
                T.StructField("chunking_version", T.StringType(), False),
                T.StructField("include_role_prefix", T.BooleanType(), False),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )
        out_df = spark.createDataFrame(chunk_rows, schema=chunk_schema)
        if successful_call_ids:
            ids_sql = ", ".join(_sql_literal(call) for call in sorted(set(successful_call_ids)))
            spark.sql(
                f"""
                DELETE FROM {chunks_table}
                WHERE call_id IN ({ids_sql})
                  AND chunking_version = {_sql_literal(chunking_version)}
                """
            )
        out_df.write.format("delta").mode("append").saveAsTable(chunks_table)
        rows_written = len(chunk_rows)

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
        status_df.createOrReplaceTempView("tmp_insights_03_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file_status_table} AS t
            USING tmp_insights_03_status AS s
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
            "Zero call_ids completed in insights_03_build_text_chunks while eligible calls existed."
        )
        raise RuntimeError(error_summary)

    if ops_failed_count > 0:
        final_status = "WARN"
        error_summary = (
            f"{ops_failed_count} call(s) failed in {STAGE_NAME}; "
            f"see {ops_file_status_table} for details."
        )
    elif small_text_chunk_count > 0:
        final_status = "WARN"
        error_summary = f"{small_text_chunk_count} chunk(s) had very small text (<20 chars)."
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
        success_count=ops_success_count + ops_skipped_count,
        failed_count=ops_failed_count,
        error_summary=error_summary,
    )
    print(
        f"[{STAGE_NAME}] eligible={eligible_count} success_calls={ops_success_count} "
        f"skipped_calls={ops_skipped_count} failed_calls={ops_failed_count} "
        f"rows_written={rows_written} small_text_chunks={small_text_chunk_count} status={final_status}"
    )
