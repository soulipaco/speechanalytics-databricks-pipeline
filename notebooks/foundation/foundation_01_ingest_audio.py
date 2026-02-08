# Databricks notebook source
# MAGIC %md
# MAGIC # FOUNDATION_01_INGEST_AUDIO
# MAGIC
# MAGIC **Purpose**
# MAGIC - Discover `.wav` files from Bronze Volume storage and register them in `bronze_audio_files`.
# MAGIC
# MAGIC **Inputs**
# MAGIC - Parameters: `catalog`, `schema`, `volume_root`, `run_id`, `run_mode`, `max_files_per_run`, `language_hint_strategy`, `source_type`, `language_hint_mapping_table`
# MAGIC - Files: `${volume_root}/bronze/audio_raw/**/*.wav`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.<schema>.bronze_audio_files`
# MAGIC - `<catalog>.<schema>.ops_file_status` (stage: `ingest_audio`)
# MAGIC - `<catalog>.<schema>.ops_pipeline_runs`
# MAGIC
# MAGIC **Key rules**
# MAGIC - Idempotent by `file_hash` de-duplication.
# MAGIC - `sample | incremental | full` run modes are supported.
# MAGIC - Per-file failures are isolated; task fails only for unreachable source or all scanned files failed.

# COMMAND ----------

import hashlib
import json
import os
import re
import wave
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from pyspark.sql import functions as F
from pyspark.sql import types as T


WORKFLOW_NAME = "foundation"
STAGE_NAME = "ingest_audio"
ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
ALLOWED_LANGUAGE_HINT_STRATEGIES = {
    "none",
    "from_filename_prefix",
    "from_folder_name",
    "mapping_table",
}


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


def _build_audio_root(volume_root: str) -> str:
    root = volume_root.strip().rstrip("/")
    if root.startswith("dbfs:/"):
        return f"{root}/bronze/audio_raw"
    if root.startswith("/"):
        return f"{root}/bronze/audio_raw"
    return f"/{root}/bronze/audio_raw"


def _list_wav_files_recursive(audio_root_path: str) -> List[str]:
    discovered: List[str] = []
    stack: List[str] = [audio_root_path]
    while stack:
        current = stack.pop()
        try:
            entries = dbutils.fs.ls(current)
        except Exception as exc:  # pragma: no cover - runtime behavior in Databricks
            raise RuntimeError(
                f"Source directory is unreachable: {audio_root_path}. {exc}"
            ) from exc

        for entry in entries:
            is_dir_attr = getattr(entry, "isDir", None)
            is_dir = is_dir_attr() if callable(is_dir_attr) else bool(is_dir_attr)
            path = str(entry.path).rstrip("/")
            if is_dir:
                stack.append(path)
            elif path.lower().endswith(".wav"):
                discovered.append(_canonical_volume_path(path))

    return sorted(set(discovered))


def _compute_sha256(local_path: str, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(local_path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _read_wav_metadata(local_path: str) -> Tuple[float, int, int]:
    with wave.open(local_path, "rb") as wav_handle:
        frame_rate = int(wav_handle.getframerate())
        channels = int(wav_handle.getnchannels())
        frame_count = int(wav_handle.getnframes())
        if frame_rate <= 0:
            raise ValueError(f"Invalid WAV frame rate: {frame_rate}")
        duration_sec = frame_count / float(frame_rate)
    return duration_sec, frame_rate, channels


def _load_mapping_rules(
    catalog: str, schema: str, language_hint_mapping_table: str
) -> List[Tuple[str, str]]:
    table_name = (language_hint_mapping_table or "").strip()
    if not table_name:
        raise ValueError(
            "language_hint_mapping_table is required when "
            "language_hint_strategy='mapping_table'."
        )

    if "." not in table_name:
        table_name = _fq_table(catalog, schema, table_name)

    rules_df = (
        spark.table(table_name)
        .select("match_pattern", "language_hint")
        .where("match_pattern IS NOT NULL AND language_hint IS NOT NULL")
        .orderBy(F.col("match_pattern"))
    )
    return [(row["match_pattern"], row["language_hint"]) for row in rules_df.collect()]


def _derive_language_hint(
    file_path: str,
    strategy: str,
    audio_root_path: str,
    mapping_rules: List[Tuple[str, str]],
    regex_cache: Dict[str, Optional[re.Pattern]],
) -> Optional[str]:
    canonical_path = _canonical_volume_path(file_path)
    filename = os.path.basename(canonical_path)
    filename_stem, _ = os.path.splitext(filename)

    if strategy == "none":
        return None

    if strategy == "from_filename_prefix":
        token = filename_stem.split("_")[0].strip() if filename_stem else ""
        return token or None

    if strategy == "from_folder_name":
        root = _canonical_volume_path(audio_root_path).rstrip("/")
        relative = canonical_path[len(root) :].lstrip("/") if canonical_path.startswith(root) else canonical_path
        parent = os.path.basename(os.path.dirname(relative))
        return parent or None

    if strategy == "mapping_table":
        filename_lower = filename.lower()
        for pattern, language_hint in mapping_rules:
            cached = regex_cache.get(pattern)
            if cached is None and pattern not in regex_cache:
                try:
                    regex_cache[pattern] = re.compile(pattern, re.IGNORECASE)
                except re.error:
                    regex_cache[pattern] = None
            compiled = regex_cache.get(pattern)
            if compiled is not None and compiled.search(filename):
                return language_hint
            if compiled is None and pattern.lower() in filename_lower:
                return language_hint
        return None

    raise ValueError(f"Unsupported language_hint_strategy: {strategy}")


def _ensure_tables(
    bronze_table: str, ops_file_status_table: str, ops_pipeline_runs_table: str
) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {bronze_table} (
          call_id STRING,
          file_path STRING,
          file_hash STRING,
          duration_sec DOUBLE,
          sample_rate INT,
          channels INT,
          language_hint STRING,
          source_type STRING,
          status STRING,
          error_message STRING,
          ingested_at TIMESTAMP,
          updated_at TIMESTAMP,
          run_id STRING
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


def _collapse_status_records(records: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    by_call_id: Dict[str, Dict[str, object]] = {}
    for record in records:
        call_id = str(record["call_id"])
        previous = by_call_id.get(call_id)
        if previous is None:
            by_call_id[call_id] = record
            continue

        prev_status = str(previous["status"])
        curr_status = str(record["status"])
        if prev_status == "FAILED":
            continue
        if curr_status == "FAILED":
            by_call_id[call_id] = record
            continue
        by_call_id[call_id] = record
    return list(by_call_id.values())


if _is_databricks():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("volume_root", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("language_hint_strategy", "from_filename_prefix")
    dbutils.widgets.text("source_type", "synthetic")
    dbutils.widgets.text("language_hint_mapping_table", "")

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    volume_root = dbutils.widgets.get("volume_root").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    language_hint_strategy = dbutils.widgets.get("language_hint_strategy").strip().lower()
    source_type = dbutils.widgets.get("source_type").strip() or "synthetic"
    language_hint_mapping_table = dbutils.widgets.get("language_hint_mapping_table").strip()
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    volume_root = os.getenv("VOLUME_ROOT", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    language_hint_strategy = os.getenv(
        "LANGUAGE_HINT_STRATEGY", "from_filename_prefix"
    ).strip().lower()
    source_type = os.getenv("SOURCE_TYPE", "synthetic").strip() or "synthetic"
    language_hint_mapping_table = os.getenv("LANGUAGE_HINT_MAPPING_TABLE", "").strip()

catalog = _validate_identifier("catalog", catalog)
schema = _validate_identifier("schema", schema)
if not volume_root:
    raise ValueError("Parameter `volume_root` is required.")
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid `run_mode`: {run_mode!r}. Allowed: {sorted(ALLOWED_RUN_MODES)}")
if language_hint_strategy not in ALLOWED_LANGUAGE_HINT_STRATEGIES:
    raise ValueError(
        f"Invalid `language_hint_strategy`: {language_hint_strategy!r}. "
        f"Allowed: {sorted(ALLOWED_LANGUAGE_HINT_STRATEGIES)}"
    )

max_files_per_run: Optional[int] = None
if max_files_per_run_raw:
    max_files_per_run = int(max_files_per_run_raw)
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError(
        "In sample mode, `max_files_per_run` must be provided as an integer > 0."
    )

audio_root_path = _build_audio_root(volume_root)
bronze_table = _fq_table(catalog, schema, "bronze_audio_files")
ops_file_status_table = _fq_table(catalog, schema, "ops_file_status")
ops_pipeline_runs_table = _fq_table(catalog, schema, "ops_pipeline_runs")

params_snapshot = {
    "catalog": catalog,
    "schema": schema,
    "volume_root": volume_root,
    "run_id": run_id,
    "run_mode": run_mode,
    "max_files_per_run": max_files_per_run,
    "language_hint_strategy": language_hint_strategy,
    "source_type": source_type,
    "language_hint_mapping_table": language_hint_mapping_table,
    "audio_root_path": audio_root_path,
}
parameters_json = json.dumps(params_snapshot, sort_keys=True)

print(f"[{STAGE_NAME}] Starting with run_id={run_id}, run_mode={run_mode}")
print(f"[{STAGE_NAME}] Audio root path: {audio_root_path}")

_ensure_tables(bronze_table, ops_file_status_table, ops_pipeline_runs_table)
_upsert_pipeline_run_running(ops_pipeline_runs_table, run_id, parameters_json)

total_scanned = 0
ops_success_count = 0
ops_failed_count = 0
inserted_count = 0
updated_count = 0
final_status = "SUCCESS"
error_summary: Optional[str] = None

try:
    discovered_files = _list_wav_files_recursive(audio_root_path)
    discovered_count = len(discovered_files)
    print(f"[{STAGE_NAME}] Discovered .wav files: {discovered_count}")

    existing_rows = spark.table(bronze_table).select("file_path", "file_hash").collect()
    existing_paths = {
        _canonical_volume_path(row["file_path"])
        for row in existing_rows
        if row["file_path"] is not None
    }
    existing_hashes = {
        row["file_hash"] for row in existing_rows if row["file_hash"] is not None
    }

    if run_mode == "full":
        eligible_files = discovered_files
    else:
        eligible_files = [
            path
            for path in discovered_files
            if _canonical_volume_path(path) not in existing_paths
        ]

    if run_mode == "sample" and max_files_per_run is not None:
        eligible_files = eligible_files[:max_files_per_run]

    total_scanned = len(eligible_files)
    print(f"[{STAGE_NAME}] Eligible files to scan: {total_scanned}")

    mapping_rules: List[Tuple[str, str]] = []
    regex_cache: Dict[str, Optional[re.Pattern]] = {}
    if language_hint_strategy == "mapping_table":
        mapping_rules = _load_mapping_rules(
            catalog, schema, language_hint_mapping_table
        )
        print(f"[{STAGE_NAME}] Loaded language mapping rules: {len(mapping_rules)}")

    process_ts = datetime.utcnow()
    success_records: List[Dict[str, object]] = []
    status_records_raw: List[Dict[str, object]] = []

    for file_path in eligible_files:
        canonical_file_path = _canonical_volume_path(file_path)
        fallback_call_id = (
            "call_err_"
            + hashlib.sha256(canonical_file_path.encode("utf-8")).hexdigest()[:12]
        )
        try:
            local_path = _to_local_read_path(canonical_file_path)
            file_hash = _compute_sha256(local_path)
            duration_sec, sample_rate, channels = _read_wav_metadata(local_path)
            call_id = f"call_{file_hash[:12]}"

            if not file_hash:
                raise ValueError("file_hash is null")
            if duration_sec <= 0:
                raise ValueError(
                    f"duration_sec must be > 0 but is {duration_sec} for {canonical_file_path}"
                )

            language_hint = _derive_language_hint(
                file_path=canonical_file_path,
                strategy=language_hint_strategy,
                audio_root_path=audio_root_path,
                mapping_rules=mapping_rules,
                regex_cache=regex_cache,
            )

            success_records.append(
                {
                    "call_id": call_id,
                    "file_path": canonical_file_path,
                    "file_hash": file_hash,
                    "duration_sec": float(duration_sec),
                    "sample_rate": int(sample_rate),
                    "channels": int(channels),
                    "language_hint": language_hint,
                    "source_type": source_type,
                    "status": "NEW",
                    "error_message": None,
                    "ingested_at": process_ts,
                    "updated_at": process_ts,
                    "run_id": run_id,
                }
            )
            status_records_raw.append(
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
            status_records_raw.append(
                {
                    "call_id": fallback_call_id,
                    "stage_name": STAGE_NAME,
                    "status": "FAILED",
                    "error_message": _truncate_error(exc),
                    "run_id": run_id,
                    "updated_at": process_ts,
                }
            )

    deduped_by_hash: Dict[str, Dict[str, object]] = {}
    for record in success_records:
        file_hash = str(record["file_hash"])
        if file_hash not in deduped_by_hash:
            deduped_by_hash[file_hash] = record
    bronze_records = list(deduped_by_hash.values())

    if bronze_records:
        success_schema = T.StructType(
            [
                T.StructField("call_id", T.StringType(), False),
                T.StructField("file_path", T.StringType(), False),
                T.StructField("file_hash", T.StringType(), False),
                T.StructField("duration_sec", T.DoubleType(), False),
                T.StructField("sample_rate", T.IntegerType(), False),
                T.StructField("channels", T.IntegerType(), False),
                T.StructField("language_hint", T.StringType(), True),
                T.StructField("source_type", T.StringType(), False),
                T.StructField("status", T.StringType(), False),
                T.StructField("error_message", T.StringType(), True),
                T.StructField("ingested_at", T.TimestampType(), False),
                T.StructField("updated_at", T.TimestampType(), False),
                T.StructField("run_id", T.StringType(), False),
            ]
        )
        success_df = spark.createDataFrame(bronze_records, schema=success_schema)
        success_df.createOrReplaceTempView("tmp_foundation_01_success")

        if run_mode == "full":
            spark.sql(
                f"""
                MERGE INTO {bronze_table} AS t
                USING tmp_foundation_01_success AS s
                ON t.file_hash = s.file_hash
                WHEN MATCHED THEN UPDATE SET
                  t.call_id = s.call_id,
                  t.file_path = s.file_path,
                  t.duration_sec = s.duration_sec,
                  t.sample_rate = s.sample_rate,
                  t.channels = s.channels,
                  t.language_hint = s.language_hint,
                  t.source_type = s.source_type,
                  t.status = COALESCE(t.status, 'NEW'),
                  t.error_message = NULL,
                  t.updated_at = s.updated_at,
                  t.run_id = s.run_id
                WHEN NOT MATCHED THEN INSERT (
                  call_id,
                  file_path,
                  file_hash,
                  duration_sec,
                  sample_rate,
                  channels,
                  language_hint,
                  source_type,
                  status,
                  error_message,
                  ingested_at,
                  updated_at,
                  run_id
                ) VALUES (
                  s.call_id,
                  s.file_path,
                  s.file_hash,
                  s.duration_sec,
                  s.sample_rate,
                  s.channels,
                  s.language_hint,
                  s.source_type,
                  'NEW',
                  NULL,
                  s.ingested_at,
                  s.updated_at,
                  s.run_id
                )
                """
            )
        else:
            spark.sql(
                f"""
                MERGE INTO {bronze_table} AS t
                USING tmp_foundation_01_success AS s
                ON t.file_hash = s.file_hash
                WHEN NOT MATCHED THEN INSERT (
                  call_id,
                  file_path,
                  file_hash,
                  duration_sec,
                  sample_rate,
                  channels,
                  language_hint,
                  source_type,
                  status,
                  error_message,
                  ingested_at,
                  updated_at,
                  run_id
                ) VALUES (
                  s.call_id,
                  s.file_path,
                  s.file_hash,
                  s.duration_sec,
                  s.sample_rate,
                  s.channels,
                  s.language_hint,
                  s.source_type,
                  'NEW',
                  NULL,
                  s.ingested_at,
                  s.updated_at,
                  s.run_id
                )
                """
            )

        inserted_count = sum(
            1 for record in bronze_records if record["file_hash"] not in existing_hashes
        )
        updated_count = (
            sum(1 for record in bronze_records if record["file_hash"] in existing_hashes)
            if run_mode == "full"
            else 0
        )

    status_records = _collapse_status_records(status_records_raw)
    ops_success_count = sum(1 for row in status_records if row["status"] == "SUCCESS")
    ops_failed_count = sum(1 for row in status_records if row["status"] == "FAILED")

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
        status_df.createOrReplaceTempView("tmp_foundation_01_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file_status_table} AS t
            USING tmp_foundation_01_status AS s
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

    if total_scanned > 0 and ops_success_count == 0 and ops_failed_count == total_scanned:
        final_status = "FAILED"
        error_summary = "All scanned files failed during ingest_audio."
        raise RuntimeError(error_summary)

    if ops_failed_count > 0:
        error_summary = (
            f"{ops_failed_count} file(s) failed in ingest_audio; "
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
        total_files=total_scanned,
        success_count=ops_success_count,
        failed_count=ops_failed_count,
        error_summary=error_summary,
    )
    print(
        f"[{STAGE_NAME}] discovered={locals().get('discovered_count', 0)} "
        f"eligible={total_scanned} inserted={inserted_count} "
        f"updated={updated_count} failed={ops_failed_count} status={final_status}"
    )
