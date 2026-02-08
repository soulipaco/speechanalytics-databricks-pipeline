# Databricks notebook source
# MAGIC %md
# MAGIC # INSIGHTS_04_EMBED_CHUNKS
# MAGIC
# MAGIC **Purpose**
# MAGIC - Generate embeddings for text chunks to support retrieval (RAG) and downstream LLM stages.
# MAGIC
# MAGIC **Inputs**
# MAGIC - Parameters: `catalog`, `schema`, `run_id`, `run_mode`, `max_files_per_run`, `enable_embeddings`, `embedding_model`, `embedding_version`, `embedding_dim`, `embedding_storage_format`, `embedding_backend`, `embedding_endpoint_name`, `embedding_service_url`, `embedding_service_api_key`, `embedding_timeout_sec`, `batch_size`, `chunking_version_filter`
# MAGIC - Table: `<catalog>.<schema>.silver_text_chunks`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.<schema>.silver_embeddings`
# MAGIC - `<catalog>.<schema>.ops_file_status` (stage: `insights_04_embed_chunks`)
# MAGIC - `<catalog>.<schema>.ops_pipeline_runs` (workflow: `insights`)
# MAGIC
# MAGIC **Key rules**
# MAGIC - Supports `sample | incremental | full`.
# MAGIC - Idempotent upsert scope: `(call_id, chunk_id, embedding_model, embedding_version)`.
# MAGIC - Per-call failure isolation; stage fails only if eligible calls exist and zero complete.

# COMMAND ----------

import json
import os
import re
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

from pyspark.sql import functions as F
from pyspark.sql import types as T


WORKFLOW_NAME = "insights"
STAGE_NAME = "insights_04_embed_chunks"
ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
ALLOWED_EMBEDDING_BACKENDS = {"auto", "endpoint", "service", "local_transformers"}
ALLOWED_EMBEDDING_STORAGE_FORMATS = {"array_float", "binary"}
DEFAULT_EMBEDDING_VERSION = "v1"
DEFAULT_EMBEDDING_STORAGE_FORMAT = "array_float"
DEFAULT_EMBEDDING_BACKEND = "auto"
DEFAULT_EMBEDDING_TIMEOUT_SEC = 30
DEFAULT_BATCH_SIZE = 32
FAILURE_WARN_RATIO_THRESHOLD = 0.20


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
    embeddings_table: str, ops_file_status_table: str, ops_pipeline_runs_table: str
) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {embeddings_table} (
          call_id STRING,
          chunk_id STRING,
          embedding_vector ARRAY<FLOAT>,
          embedding_binary BINARY,
          embedding_model STRING,
          embedding_dim INT,
          embedding_version STRING,
          chunking_version STRING,
          chunk_source STRING,
          chunking_strategy STRING,
          start_sec DOUBLE,
          end_sec DOUBLE,
          embedding_backend STRING,
          embedding_storage_format STRING,
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


def _coerce_vector(value) -> List[float]:
    if isinstance(value, list):
        if not value:
            raise ValueError("Embedding vector is empty.")
        return [float(x) for x in value]
    raise ValueError("Embedding vector must be a list of numeric values.")


def _extract_embeddings(payload) -> List[List[float]]:
    if payload is None:
        raise ValueError("Empty embedding response payload.")

    if isinstance(payload, dict):
        for key in ("embeddings", "data", "predictions", "outputs", "result"):
            if key in payload:
                return _extract_embeddings(payload[key])
        for key in ("embedding", "vector", "values"):
            if key in payload:
                return [_coerce_vector(payload[key])]
        raise ValueError("Unsupported embedding dict response format.")

    if isinstance(payload, list):
        if not payload:
            raise ValueError("Embedding response list is empty.")
        if all(isinstance(x, (int, float)) for x in payload):
            return [_coerce_vector(payload)]
        if all(isinstance(x, list) for x in payload):
            return [_coerce_vector(x) for x in payload]
        if all(isinstance(x, dict) for x in payload):
            vectors: List[List[float]] = []
            for item in payload:
                extracted = None
                for key in ("embedding", "vector", "values"):
                    if key in item:
                        extracted = item[key]
                        break
                if extracted is None:
                    raise ValueError("Embedding dict item missing embedding vector field.")
                vectors.append(_coerce_vector(extracted))
            return vectors

    raise ValueError("Unsupported embedding response format.")


def _build_embedding_backend(
    embedding_backend: str,
    embedding_model: str,
    embedding_endpoint_name: str,
    embedding_service_url: str,
    embedding_service_api_key: str,
    embedding_timeout_sec: int,
) -> Tuple[Optional[str], Optional[Callable[[List[str]], List[List[float]]]], Optional[str]]:
    errors: List[str] = []

    def _build_endpoint_backend():
        if not embedding_endpoint_name:
            return None, None, "embedding_endpoint_name is required for endpoint backend."
        try:
            import mlflow.deployments  # type: ignore

            client = mlflow.deployments.get_deploy_client("databricks")

            def _embed_endpoint(texts: List[str]) -> List[List[float]]:
                attempts = [
                    {"input": texts, "model": embedding_model},
                    {"inputs": texts, "model": embedding_model},
                    {"input": texts},
                    {"inputs": texts},
                ]
                last_error = None
                for payload in attempts:
                    try:
                        response = client.predict(endpoint=embedding_endpoint_name, inputs=payload)
                        vectors = _extract_embeddings(response)
                        if len(vectors) == len(texts):
                            return vectors
                    except Exception as exc:  # pragma: no cover
                        last_error = exc
                raise RuntimeError(
                    "Endpoint embedding response was invalid or count-mismatched."
                ) from last_error

            return f"databricks_endpoint:{embedding_endpoint_name}", _embed_endpoint, None
        except Exception as exc:
            return None, None, f"Endpoint backend unavailable: {_truncate_error(exc, 500)}"

    def _build_service_backend():
        if not embedding_service_url:
            return None, None, "embedding_service_url is required for service backend."
        try:
            import requests  # type: ignore

            def _embed_service(texts: List[str]) -> List[List[float]]:
                headers = {"Content-Type": "application/json"}
                if embedding_service_api_key:
                    headers["Authorization"] = f"Bearer {embedding_service_api_key}"

                attempts = [
                    {"input": texts, "model": embedding_model},
                    {"inputs": texts, "model": embedding_model},
                    {"texts": texts, "model": embedding_model},
                ]
                last_error = None
                for payload in attempts:
                    try:
                        response = requests.post(
                            embedding_service_url,
                            json=payload,
                            headers=headers,
                            timeout=embedding_timeout_sec,
                        )
                        response.raise_for_status()
                        vectors = _extract_embeddings(response.json())
                        if len(vectors) == len(texts):
                            return vectors
                    except Exception as exc:  # pragma: no cover
                        last_error = exc
                raise RuntimeError(
                    "Service embedding response was invalid or count-mismatched."
                ) from last_error

            return f"embedding_service:{embedding_service_url}", _embed_service, None
        except Exception as exc:
            return None, None, f"Service backend unavailable: {_truncate_error(exc, 500)}"

    def _build_local_backend():
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore

            model = SentenceTransformer(embedding_model)

            def _embed_local_sentence_transformers(texts: List[str]) -> List[List[float]]:
                embeddings = model.encode(
                    texts, show_progress_bar=False, convert_to_numpy=False
                )
                vectors: List[List[float]] = []
                for item in embeddings:
                    if hasattr(item, "tolist"):
                        item = item.tolist()
                    vectors.append(_coerce_vector(item))
                return vectors

            return (
                f"sentence_transformers:{embedding_model}",
                _embed_local_sentence_transformers,
                None,
            )
        except Exception as first_exc:
            try:
                import torch  # type: ignore
                from transformers import AutoModel, AutoTokenizer  # type: ignore

                tokenizer = AutoTokenizer.from_pretrained(embedding_model)
                model = AutoModel.from_pretrained(embedding_model)
                model.eval()

                def _embed_local_transformers(texts: List[str]) -> List[List[float]]:
                    encoded = tokenizer(
                        texts,
                        padding=True,
                        truncation=True,
                        return_tensors="pt",
                    )
                    with torch.no_grad():
                        outputs = model(**encoded)
                        hidden = outputs.last_hidden_state
                        mask = encoded["attention_mask"].unsqueeze(-1)
                        summed = (hidden * mask).sum(dim=1)
                        counts = mask.sum(dim=1).clamp(min=1)
                        pooled = summed / counts
                        vectors = pooled.cpu().tolist()
                    return [_coerce_vector(v) for v in vectors]

                return f"transformers:{embedding_model}", _embed_local_transformers, None
            except Exception as second_exc:
                return (
                    None,
                    None,
                    "Local backend unavailable: "
                    + _truncate_error(first_exc, 250)
                    + " | "
                    + _truncate_error(second_exc, 250),
                )

    if embedding_backend == "endpoint":
        return _build_endpoint_backend()
    if embedding_backend == "service":
        return _build_service_backend()
    if embedding_backend == "local_transformers":
        return _build_local_backend()

    # auto mode
    if embedding_endpoint_name:
        name, fn, error = _build_endpoint_backend()
        if fn is not None:
            return name, fn, None
        if error:
            errors.append(error)

    if embedding_service_url:
        name, fn, error = _build_service_backend()
        if fn is not None:
            return name, fn, None
        if error:
            errors.append(error)

    name, fn, error = _build_local_backend()
    if fn is not None:
        return name, fn, None
    if error:
        errors.append(error)

    return None, None, "; ".join(errors) or "No embedding backend available."


if _is_databricks():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("enable_embeddings", "false")
    dbutils.widgets.text("embedding_enabled", "")
    dbutils.widgets.text("embedding_model", "")
    dbutils.widgets.text("embedding_model_name", "")
    dbutils.widgets.text("embedding_version", DEFAULT_EMBEDDING_VERSION)
    dbutils.widgets.text("embedding_dim", "")
    dbutils.widgets.text("embedding_storage_format", DEFAULT_EMBEDDING_STORAGE_FORMAT)
    dbutils.widgets.text("embedding_backend", DEFAULT_EMBEDDING_BACKEND)
    dbutils.widgets.text("embedding_endpoint_name", "")
    dbutils.widgets.text("embedding_service_url", "")
    dbutils.widgets.text("embedding_service_api_key", "")
    dbutils.widgets.text("embedding_timeout_sec", str(DEFAULT_EMBEDDING_TIMEOUT_SEC))
    dbutils.widgets.text("batch_size", str(DEFAULT_BATCH_SIZE))
    dbutils.widgets.text("chunking_version_filter", "")

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    enable_embeddings_raw = dbutils.widgets.get("enable_embeddings").strip()
    embedding_enabled_alias_raw = dbutils.widgets.get("embedding_enabled").strip()
    embedding_model_raw = dbutils.widgets.get("embedding_model").strip()
    embedding_model_name_alias = dbutils.widgets.get("embedding_model_name").strip()
    embedding_version = dbutils.widgets.get("embedding_version").strip()
    embedding_dim_raw = dbutils.widgets.get("embedding_dim").strip()
    embedding_storage_format = dbutils.widgets.get("embedding_storage_format").strip().lower()
    embedding_backend = dbutils.widgets.get("embedding_backend").strip().lower()
    embedding_endpoint_name = dbutils.widgets.get("embedding_endpoint_name").strip()
    embedding_service_url = dbutils.widgets.get("embedding_service_url").strip()
    embedding_service_api_key = dbutils.widgets.get("embedding_service_api_key").strip()
    embedding_timeout_sec_raw = dbutils.widgets.get("embedding_timeout_sec").strip()
    batch_size_raw = dbutils.widgets.get("batch_size").strip()
    chunking_version_filter = dbutils.widgets.get("chunking_version_filter").strip()
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    enable_embeddings_raw = os.getenv("ENABLE_EMBEDDINGS", "false").strip()
    embedding_enabled_alias_raw = os.getenv("EMBEDDING_ENABLED", "").strip()
    embedding_model_raw = os.getenv("EMBEDDING_MODEL", "").strip()
    embedding_model_name_alias = os.getenv("EMBEDDING_MODEL_NAME", "").strip()
    embedding_version = os.getenv("EMBEDDING_VERSION", DEFAULT_EMBEDDING_VERSION).strip()
    embedding_dim_raw = os.getenv("EMBEDDING_DIM", "").strip()
    embedding_storage_format = os.getenv(
        "EMBEDDING_STORAGE_FORMAT", DEFAULT_EMBEDDING_STORAGE_FORMAT
    ).strip().lower()
    embedding_backend = os.getenv("EMBEDDING_BACKEND", DEFAULT_EMBEDDING_BACKEND).strip().lower()
    embedding_endpoint_name = os.getenv("EMBEDDING_ENDPOINT_NAME", "").strip()
    embedding_service_url = os.getenv("EMBEDDING_SERVICE_URL", "").strip()
    embedding_service_api_key = os.getenv("EMBEDDING_SERVICE_API_KEY", "").strip()
    embedding_timeout_sec_raw = os.getenv(
        "EMBEDDING_TIMEOUT_SEC", str(DEFAULT_EMBEDDING_TIMEOUT_SEC)
    ).strip()
    batch_size_raw = os.getenv("BATCH_SIZE", str(DEFAULT_BATCH_SIZE)).strip()
    chunking_version_filter = os.getenv("CHUNKING_VERSION_FILTER", "").strip()

catalog = _validate_identifier("catalog", catalog)
schema = _validate_identifier("schema", schema)
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid `run_mode`: {run_mode!r}. Allowed: {sorted(ALLOWED_RUN_MODES)}")
if embedding_backend not in ALLOWED_EMBEDDING_BACKENDS:
    raise ValueError(
        f"Invalid `embedding_backend`: {embedding_backend!r}. "
        f"Allowed: {sorted(ALLOWED_EMBEDDING_BACKENDS)}"
    )
if embedding_storage_format not in ALLOWED_EMBEDDING_STORAGE_FORMATS:
    raise ValueError(
        f"Invalid `embedding_storage_format`: {embedding_storage_format!r}. "
        f"Allowed: {sorted(ALLOWED_EMBEDDING_STORAGE_FORMATS)}"
    )

max_files_per_run: Optional[int] = None
if max_files_per_run_raw:
    max_files_per_run = int(max_files_per_run_raw)
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError("In sample mode, `max_files_per_run` must be provided as an integer > 0.")

enable_embeddings_source = enable_embeddings_raw or embedding_enabled_alias_raw or "false"
enable_embeddings = _parse_bool("enable_embeddings", enable_embeddings_source, default=False)
embedding_model = (embedding_model_raw or embedding_model_name_alias).strip()
if enable_embeddings and not embedding_model:
    raise ValueError(
        "`embedding_model` is required when embeddings are enabled "
        "(or provide alias `embedding_model_name`)."
    )

embedding_version = (embedding_version or DEFAULT_EMBEDDING_VERSION).strip()
if not embedding_version:
    raise ValueError("Parameter `embedding_version` is required.")

embedding_dim: Optional[int] = None
if embedding_dim_raw:
    embedding_dim = int(embedding_dim_raw)
    if embedding_dim <= 0:
        raise ValueError("`embedding_dim` must be > 0 if provided.")

embedding_timeout_sec = int(embedding_timeout_sec_raw or DEFAULT_EMBEDDING_TIMEOUT_SEC)
if embedding_timeout_sec <= 0:
    embedding_timeout_sec = DEFAULT_EMBEDDING_TIMEOUT_SEC

batch_size = int(batch_size_raw or DEFAULT_BATCH_SIZE)
if batch_size <= 0:
    batch_size = DEFAULT_BATCH_SIZE

chunks_table = _fq_table(catalog, schema, "silver_text_chunks")
embeddings_table = _fq_table(catalog, schema, "silver_embeddings")
ops_file_status_table = _fq_table(catalog, schema, "ops_file_status")
ops_pipeline_runs_table = _fq_table(catalog, schema, "ops_pipeline_runs")

params_snapshot = {
    "catalog": catalog,
    "schema": schema,
    "run_id": run_id,
    "run_mode": run_mode,
    "max_files_per_run": max_files_per_run,
    "enable_embeddings": enable_embeddings,
    "embedding_model": embedding_model,
    "embedding_version": embedding_version,
    "embedding_dim": embedding_dim,
    "embedding_storage_format": embedding_storage_format,
    "embedding_backend": embedding_backend,
    "embedding_endpoint_name": embedding_endpoint_name,
    "embedding_service_url": embedding_service_url,
    "embedding_timeout_sec": embedding_timeout_sec,
    "batch_size": batch_size,
    "chunking_version_filter": chunking_version_filter or None,
}
parameters_json = json.dumps(params_snapshot, sort_keys=True)

print(
    f"[{STAGE_NAME}] Starting with run_id={run_id}, run_mode={run_mode}, "
    f"enable_embeddings={enable_embeddings}, embedding_backend={embedding_backend}"
)

_ensure_tables(embeddings_table, ops_file_status_table, ops_pipeline_runs_table)
_upsert_pipeline_run_running(ops_pipeline_runs_table, run_id, parameters_json)

eligible_count = 0
ops_success_count = 0
ops_failed_count = 0
ops_skipped_count = 0
rows_written = 0
final_status = "SUCCESS"
error_summary: Optional[str] = None

try:
    if not _table_exists(catalog, schema, "silver_text_chunks"):
        raise RuntimeError(f"Required input table is missing: {chunks_table}")

    chunks_df = spark.table(chunks_table)
    required_columns = {
        "call_id",
        "chunk_id",
        "chunk_text",
        "start_sec",
        "end_sec",
        "chunk_source",
        "chunking_strategy",
    }
    chunking_version_column = "chunking_version"
    if "chunking_version" not in chunks_df.columns:
        if "chunk_version" in chunks_df.columns:
            chunking_version_column = "chunk_version"
        else:
            missing_columns = sorted(
                (required_columns | {"chunking_version"}) - set(chunks_df.columns)
            )
            raise RuntimeError(
                f"Input table {chunks_table} is missing required columns: {missing_columns}"
            )

    required_columns.add(chunking_version_column)
    missing_columns = sorted(required_columns - set(chunks_df.columns))
    if missing_columns:
        raise RuntimeError(
            f"Input table {chunks_table} is missing required columns: {missing_columns}"
        )

    chunks_df = chunks_df.select(
        "call_id",
        "chunk_id",
        F.col("chunk_text").cast("string").alias("chunk_text"),
        F.col("start_sec").cast("double").alias("start_sec"),
        F.col("end_sec").cast("double").alias("end_sec"),
        F.col("chunk_source").cast("string").alias("chunk_source"),
        F.col("chunking_strategy").cast("string").alias("chunking_strategy"),
        F.col(chunking_version_column).cast("string").alias("chunking_version"),
    ).where("call_id IS NOT NULL AND chunk_id IS NOT NULL")

    if chunking_version_filter:
        chunks_df = chunks_df.where(F.col("chunking_version") == chunking_version_filter)

    stage_df = (
        spark.table(ops_file_status_table)
        .where(F.col("stage_name") == STAGE_NAME)
        .select("call_id", F.col("status").alias("stage_status"))
    )

    call_df = chunks_df.select("call_id").distinct().orderBy("call_id")
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
    embed_fn: Optional[Callable[[List[str]], List[List[float]]]] = None
    backend_error: Optional[str] = None
    if enable_embeddings and eligible_count > 0:
        backend_name, embed_fn, backend_error = _build_embedding_backend(
            embedding_backend=embedding_backend,
            embedding_model=embedding_model,
            embedding_endpoint_name=embedding_endpoint_name,
            embedding_service_url=embedding_service_url,
            embedding_service_api_key=embedding_service_api_key,
            embedding_timeout_sec=embedding_timeout_sec,
        )

    status_records: List[Dict[str, object]] = []
    embedding_rows: List[Dict[str, object]] = []
    successful_call_ids: List[str] = []
    process_ts = datetime.utcnow()
    observed_dim: Optional[int] = embedding_dim

    for call_id in eligible_calls:
        if not enable_embeddings:
            ops_skipped_count += 1
            status_records.append(
                {
                    "call_id": str(call_id),
                    "stage_name": STAGE_NAME,
                    "status": "SKIPPED",
                    "error_message": "Embeddings disabled by `enable_embeddings=false`.",
                    "run_id": run_id,
                    "updated_at": process_ts,
                }
            )
            continue

        try:
            if embed_fn is None:
                raise RuntimeError(backend_error or "No embedding backend available.")

            call_chunks = (
                chunks_df.where(F.col("call_id") == call_id)
                .orderBy("start_sec", "end_sec", "chunk_id")
                .collect()
            )
            if not call_chunks:
                raise ValueError("No chunks found for call_id.")

            chunk_payloads: List[Dict[str, object]] = []
            empty_chunks = 0
            for chunk in call_chunks:
                chunk_text = str(chunk["chunk_text"] or "").strip()
                if not chunk_text:
                    empty_chunks += 1
                    continue
                chunk_payloads.append(
                    {
                        "call_id": str(chunk["call_id"]),
                        "chunk_id": str(chunk["chunk_id"]),
                        "chunk_text": chunk_text,
                        "start_sec": float(chunk["start_sec"]),
                        "end_sec": float(chunk["end_sec"]),
                        "chunk_source": str(chunk["chunk_source"] or "unknown"),
                        "chunking_strategy": str(chunk["chunking_strategy"] or "unknown"),
                        "chunking_version": str(chunk["chunking_version"] or "unknown"),
                    }
                )

            if empty_chunks > 0:
                raise ValueError(
                    f"{empty_chunks} chunk(s) have empty `chunk_text`; cannot embed call."
                )
            if not chunk_payloads:
                raise ValueError("No non-empty chunks available for embedding.")

            all_vectors: List[List[float]] = []
            for offset in range(0, len(chunk_payloads), batch_size):
                batch = chunk_payloads[offset : offset + batch_size]
                batch_texts = [row["chunk_text"] for row in batch]
                vectors = embed_fn(batch_texts)
                if len(vectors) != len(batch_texts):
                    raise ValueError(
                        "Embedding backend returned vector count that does not match batch size."
                    )
                all_vectors.extend(vectors)

            if len(all_vectors) != len(chunk_payloads):
                raise ValueError("Embedding vector count mismatch for call payload.")

            call_rows: List[Dict[str, object]] = []
            call_dim: Optional[int] = None
            for index, payload in enumerate(chunk_payloads):
                vector = _coerce_vector(all_vectors[index])
                if not vector:
                    raise ValueError("Embedding vector is empty.")

                vector_dim = len(vector)
                if call_dim is None:
                    call_dim = vector_dim
                elif call_dim != vector_dim:
                    raise ValueError("Embedding dimension mismatch inside call payload.")

                if embedding_dim is not None and vector_dim != embedding_dim:
                    raise ValueError(
                        f"Vector dimension {vector_dim} does not match expected {embedding_dim}."
                    )

                if observed_dim is None:
                    observed_dim = vector_dim
                elif observed_dim != vector_dim:
                    raise ValueError(
                        f"Embedding dimension drift detected: {vector_dim} vs {observed_dim}."
                    )

                binary_payload = None
                if embedding_storage_format == "binary":
                    binary_payload = json.dumps(vector, separators=(",", ":")).encode("utf-8")

                call_rows.append(
                    {
                        "call_id": payload["call_id"],
                        "chunk_id": payload["chunk_id"],
                        "embedding_vector": [float(v) for v in vector],
                        "embedding_binary": binary_payload,
                        "embedding_model": embedding_model,
                        "embedding_dim": int(vector_dim),
                        "embedding_version": embedding_version,
                        "chunking_version": payload["chunking_version"],
                        "chunk_source": payload["chunk_source"],
                        "chunking_strategy": payload["chunking_strategy"],
                        "start_sec": float(payload["start_sec"]),
                        "end_sec": float(payload["end_sec"]),
                        "embedding_backend": backend_name or embedding_backend,
                        "embedding_storage_format": embedding_storage_format,
                        "run_id": run_id,
                        "updated_at": process_ts,
                    }
                )

            if not call_rows:
                raise ValueError("No embedding rows produced for call.")

            embedding_rows.extend(call_rows)
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

    if embedding_rows:
        schema_def = T.StructType(
            [
                T.StructField("call_id", T.StringType(), False),
                T.StructField("chunk_id", T.StringType(), False),
                T.StructField("embedding_vector", T.ArrayType(T.FloatType()), False),
                T.StructField("embedding_binary", T.BinaryType(), True),
                T.StructField("embedding_model", T.StringType(), False),
                T.StructField("embedding_dim", T.IntegerType(), False),
                T.StructField("embedding_version", T.StringType(), False),
                T.StructField("chunking_version", T.StringType(), False),
                T.StructField("chunk_source", T.StringType(), False),
                T.StructField("chunking_strategy", T.StringType(), False),
                T.StructField("start_sec", T.DoubleType(), False),
                T.StructField("end_sec", T.DoubleType(), False),
                T.StructField("embedding_backend", T.StringType(), False),
                T.StructField("embedding_storage_format", T.StringType(), False),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )
        out_df = spark.createDataFrame(embedding_rows, schema=schema_def)
        if successful_call_ids:
            ids_sql = ", ".join(_sql_literal(call_id) for call_id in sorted(set(successful_call_ids)))
            extra_filter = ""
            if chunking_version_filter:
                extra_filter = f" AND chunking_version = {_sql_literal(chunking_version_filter)}"
            spark.sql(
                f"""
                DELETE FROM {embeddings_table}
                WHERE call_id IN ({ids_sql})
                  AND embedding_model = {_sql_literal(embedding_model)}
                  AND embedding_version = {_sql_literal(embedding_version)}
                  {extra_filter}
                """
            )
        out_df.write.format("delta").mode("append").saveAsTable(embeddings_table)
        rows_written = len(embedding_rows)

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
        status_df.createOrReplaceTempView("tmp_insights_04_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file_status_table} AS t
            USING tmp_insights_04_status AS s
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
        error_summary = "Zero eligible calls completed in insights_04_embed_chunks."
        raise RuntimeError(error_summary)

    failure_ratio = (ops_failed_count / eligible_count) if eligible_count > 0 else 0.0
    if ops_failed_count > 0:
        final_status = "WARN"
        error_summary = (
            f"{ops_failed_count} call(s) failed in {STAGE_NAME}; "
            f"failure_ratio={failure_ratio:.2%}"
        )
    elif failure_ratio > FAILURE_WARN_RATIO_THRESHOLD:
        final_status = "WARN"
        error_summary = (
            f"Failure ratio {failure_ratio:.2%} exceeded warning threshold "
            f"{FAILURE_WARN_RATIO_THRESHOLD:.0%}."
        )
    elif eligible_count == 0:
        final_status = "SUCCESS"
        error_summary = f"No eligible calls for stage {STAGE_NAME}."
    elif not enable_embeddings:
        final_status = "SUCCESS"
        error_summary = "Embeddings disabled; eligible calls marked SKIPPED."

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
