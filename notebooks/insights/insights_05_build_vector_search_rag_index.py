# Databricks notebook source
# MAGIC %md
# MAGIC # INSIGHTS_05_BUILD_VECTOR_SEARCH_RAG_INDEX
# MAGIC
# MAGIC **Purpose**
# MAGIC - Build retrieval backend resources for RAG over chunk embeddings.
# MAGIC - Support Databricks Vector Search and Delta-similarity fallback.
# MAGIC
# MAGIC **Inputs**
# MAGIC - Parameters: `catalog`, `schema`, `run_id`, `run_mode`, `max_files_per_run`,
# MAGIC   `enable_rag_index` (alias: `enable_rag`), `index_backend` (alias: `rag_backend`),
# MAGIC   `index_name`, `vector_search_endpoint_name`, `embedding_model_filter`,
# MAGIC   `embedding_version_filter`, `chunking_version_filter`, `rag_top_k`, `rag_version`
# MAGIC - Tables: `<catalog>.<schema>.silver_embeddings`, `<catalog>.<schema>.silver_text_chunks`
# MAGIC
# MAGIC **Outputs**
# MAGIC - Optional external vector index when backend is Databricks Vector Search
# MAGIC - `<catalog>.<schema>.ops_file_status` (stage: `insights_05_build_vector_search_rag_index`)
# MAGIC - `<catalog>.<schema>.ops_pipeline_runs` (workflow: `insights`)
# MAGIC
# MAGIC **Key Rules**
# MAGIC - Supports `sample | incremental | full`.
# MAGIC - Idempotent behavior:
# MAGIC   - external index is created-or-synced by deterministic `index_name`
# MAGIC   - `ops_file_status` and `ops_pipeline_runs` are upserted/merged
# MAGIC - Per-item isolation (index scope item); stage fails only when eligible items exist and none complete.

# COMMAND ----------

import json
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from pyspark.sql import functions as F
from pyspark.sql import types as T


WORKFLOW_NAME = "insights"
STAGE_NAME = "insights_05_build_vector_search_rag_index"

ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
ALLOWED_INDEX_BACKEND_INPUTS = {
    "auto",
    "databricks_vector_search",
    "delta_similarity",
    "skip",
    "vector_search",
}

DEFAULT_INDEX_BACKEND = "auto"
DEFAULT_RAG_BACKEND = "delta_similarity"
DEFAULT_RAG_TOP_K = 5
DEFAULT_RAG_VERSION = "v1"
DEFAULT_INDEX_NAME = "speechanalytics_rag_index"
MAX_INDEX_NAME_LEN = 120


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


def _normalize_backend_token(raw: str) -> str:
    token = (raw or "").strip().lower()
    if not token:
        return DEFAULT_INDEX_BACKEND
    if token == "vector_search":
        return "databricks_vector_search"
    if token not in ALLOWED_INDEX_BACKEND_INPUTS:
        raise ValueError(
            f"Invalid backend value: {raw!r}. "
            f"Allowed: {sorted(ALLOWED_INDEX_BACKEND_INPUTS)}"
        )
    if token == "vector_search":
        return "databricks_vector_search"
    return token


def _truncate_error(message: object, max_len: int = 1000) -> str:
    text = str(message).strip() or "Unknown error"
    return text[:max_len]


def _sanitize_name_token(value: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9_]+", "_", (value or "").strip())
    token = re.sub(r"_+", "_", token).strip("_").lower()
    return token or "all"


def _derive_index_name(
    index_name_raw: str,
    catalog: str,
    schema: str,
    embedding_model_filter: str,
    embedding_version_filter: str,
    chunking_version_filter: str,
    rag_version: str,
) -> str:
    base = _sanitize_name_token(index_name_raw or DEFAULT_INDEX_NAME)
    tokens = [
        base,
        _sanitize_name_token(catalog),
        _sanitize_name_token(schema),
        _sanitize_name_token(embedding_model_filter or "all_models"),
        _sanitize_name_token(embedding_version_filter or "all_versions"),
        _sanitize_name_token(chunking_version_filter or "all_chunking"),
        _sanitize_name_token(rag_version or DEFAULT_RAG_VERSION),
    ]
    name = "_".join(tokens)
    if len(name) > MAX_INDEX_NAME_LEN:
        name = name[:MAX_INDEX_NAME_LEN].rstrip("_")
    return name


def _fq_table(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def _table_exists(catalog: str, schema: str, table: str) -> bool:
    return bool(spark.catalog.tableExists(f"{catalog}.{schema}.{table}"))


def _ensure_ops_tables(ops_file_status_table: str, ops_pipeline_runs_table: str) -> None:
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


def _vector_search_library_available() -> bool:
    try:
        import databricks.vector_search.client  # type: ignore  # noqa: F401

        return True
    except Exception:
        return False


def _resolve_backend(
    normalized_backend_input: str,
    vector_search_endpoint_name: str,
) -> Tuple[str, Optional[str]]:
    if normalized_backend_input == "skip":
        return "skip", "RAG indexing skipped by backend selection."
    if normalized_backend_input == "delta_similarity":
        return "delta_similarity", None
    if normalized_backend_input == "databricks_vector_search":
        return "databricks_vector_search", None

    if vector_search_endpoint_name and _vector_search_library_available():
        return "databricks_vector_search", None

    return (
        "delta_similarity",
        "Auto backend resolved to delta_similarity (vector search endpoint/library unavailable).",
    )


def _create_or_sync_vector_search_index(
    endpoint_name: str,
    index_name: str,
    source_table: str,
    primary_key: str,
    embedding_vector_column: str,
) -> str:
    if not endpoint_name:
        raise ValueError(
            "Databricks Vector Search backend requires `vector_search_endpoint_name`."
        )

    try:
        from databricks.vector_search.client import VectorSearchClient  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Databricks Vector Search client is unavailable in this environment."
        ) from exc

    client = VectorSearchClient()

    existing_index = None
    if hasattr(client, "get_index"):
        get_variants = [
            {"endpoint_name": endpoint_name, "index_name": index_name},
            {"endpoint": endpoint_name, "index_name": index_name},
            {"index_name": index_name},
        ]
        for kwargs in get_variants:
            try:
                existing_index = client.get_index(**kwargs)
                if existing_index is not None:
                    break
            except Exception:
                continue

    if existing_index is not None:
        if hasattr(existing_index, "sync"):
            try:
                existing_index.sync()
                return "synced_existing_index"
            except Exception:
                return "index_exists_sync_not_supported"
        return "index_exists"

    create_variants = [
        {
            "endpoint_name": endpoint_name,
            "index_name": index_name,
            "source_table_name": source_table,
            "primary_key": primary_key,
            "embedding_vector_column": embedding_vector_column,
            "pipeline_type": "TRIGGERED",
        },
        {
            "endpoint": endpoint_name,
            "index_name": index_name,
            "source_table_name": source_table,
            "primary_key": primary_key,
            "embedding_vector_column": embedding_vector_column,
            "pipeline_type": "TRIGGERED",
        },
        {
            "endpoint_name": endpoint_name,
            "index_name": index_name,
            "source_table_name": source_table,
            "primary_key": primary_key,
            "embedding_source_column": embedding_vector_column,
            "pipeline_type": "TRIGGERED",
        },
    ]

    last_error = None
    for kwargs in create_variants:
        try:
            client.create_delta_sync_index(**kwargs)
            return "created_index"
        except TypeError as exc:
            last_error = exc
            continue
        except Exception as exc:
            last_error = exc
            continue

    raise RuntimeError(
        "Failed to create Databricks Vector Search index. "
        f"Last error: {_truncate_error(last_error or 'unknown', 500)}"
    )


if _is_databricks():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("enable_rag_index", "false")
    dbutils.widgets.text("enable_rag", "")
    dbutils.widgets.text("index_backend", DEFAULT_INDEX_BACKEND)
    dbutils.widgets.text("rag_backend", DEFAULT_RAG_BACKEND)
    dbutils.widgets.text("index_name", DEFAULT_INDEX_NAME)
    dbutils.widgets.text("vector_search_endpoint_name", "")
    dbutils.widgets.text("embedding_model_filter", "")
    dbutils.widgets.text("embedding_version_filter", "")
    dbutils.widgets.text("chunking_version_filter", "")
    dbutils.widgets.text("rag_top_k", str(DEFAULT_RAG_TOP_K))
    dbutils.widgets.text("rag_version", DEFAULT_RAG_VERSION)

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    enable_rag_index_raw = dbutils.widgets.get("enable_rag_index").strip()
    enable_rag_alias_raw = dbutils.widgets.get("enable_rag").strip()
    index_backend_raw = dbutils.widgets.get("index_backend").strip()
    rag_backend_raw = dbutils.widgets.get("rag_backend").strip()
    index_name_raw = dbutils.widgets.get("index_name").strip()
    vector_search_endpoint_name = dbutils.widgets.get("vector_search_endpoint_name").strip()
    embedding_model_filter = dbutils.widgets.get("embedding_model_filter").strip()
    embedding_version_filter = dbutils.widgets.get("embedding_version_filter").strip()
    chunking_version_filter = dbutils.widgets.get("chunking_version_filter").strip()
    rag_top_k_raw = dbutils.widgets.get("rag_top_k").strip()
    rag_version = dbutils.widgets.get("rag_version").strip()
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    enable_rag_index_raw = os.getenv("ENABLE_RAG_INDEX", "false").strip()
    enable_rag_alias_raw = os.getenv("ENABLE_RAG", "").strip()
    index_backend_raw = os.getenv("INDEX_BACKEND", DEFAULT_INDEX_BACKEND).strip()
    rag_backend_raw = os.getenv("RAG_BACKEND", DEFAULT_RAG_BACKEND).strip()
    index_name_raw = os.getenv("INDEX_NAME", DEFAULT_INDEX_NAME).strip()
    vector_search_endpoint_name = os.getenv("VECTOR_SEARCH_ENDPOINT_NAME", "").strip()
    embedding_model_filter = os.getenv("EMBEDDING_MODEL_FILTER", "").strip()
    embedding_version_filter = os.getenv("EMBEDDING_VERSION_FILTER", "").strip()
    chunking_version_filter = os.getenv("CHUNKING_VERSION_FILTER", "").strip()
    rag_top_k_raw = os.getenv("RAG_TOP_K", str(DEFAULT_RAG_TOP_K)).strip()
    rag_version = os.getenv("RAG_VERSION", DEFAULT_RAG_VERSION).strip()

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

enable_rag_source = enable_rag_index_raw or enable_rag_alias_raw or "false"
enable_rag = _parse_bool("enable_rag_index", enable_rag_source, default=False)

normalized_index_backend = _normalize_backend_token(index_backend_raw or rag_backend_raw)
resolved_backend, backend_resolution_note = _resolve_backend(
    normalized_backend_input=normalized_index_backend,
    vector_search_endpoint_name=vector_search_endpoint_name,
)

rag_top_k = int(rag_top_k_raw or DEFAULT_RAG_TOP_K)
if rag_top_k <= 0:
    raise ValueError("`rag_top_k` must be > 0.")

rag_version = (rag_version or DEFAULT_RAG_VERSION).strip()
if not rag_version:
    raise ValueError("`rag_version` cannot be empty.")

final_index_name = _derive_index_name(
    index_name_raw=index_name_raw,
    catalog=catalog,
    schema=schema,
    embedding_model_filter=embedding_model_filter,
    embedding_version_filter=embedding_version_filter,
    chunking_version_filter=chunking_version_filter,
    rag_version=rag_version,
)

status_item_key = f"rag_index:{final_index_name}"

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
    "enable_rag_index": enable_rag,
    "index_backend_input": normalized_index_backend,
    "index_backend_resolved": resolved_backend,
    "backend_resolution_note": backend_resolution_note,
    "index_name": final_index_name,
    "vector_search_endpoint_name": vector_search_endpoint_name or None,
    "embedding_model_filter": embedding_model_filter or None,
    "embedding_version_filter": embedding_version_filter or None,
    "chunking_version_filter": chunking_version_filter or None,
    "rag_top_k": rag_top_k,
    "rag_version": rag_version,
}
parameters_json = json.dumps(params_snapshot, sort_keys=True)

print(
    f"[{STAGE_NAME}] Starting with run_id={run_id}, run_mode={run_mode}, "
    f"enable_rag_index={enable_rag}, backend={resolved_backend}, index_name={final_index_name}"
)

_ensure_ops_tables(
    ops_file_status_table=ops_file_status_table,
    ops_pipeline_runs_table=ops_pipeline_runs_table,
)
_upsert_pipeline_run_running(ops_pipeline_runs_table, run_id, parameters_json)

eligible_count = 0
ops_success_count = 0
ops_failed_count = 0
ops_skipped_count = 0
final_status = "SUCCESS"
error_summary: Optional[str] = None

try:
    previous_status = None
    prior_status_rows = (
        spark.table(ops_file_status_table)
        .where((F.col("stage_name") == STAGE_NAME) & (F.col("call_id") == status_item_key))
        .select(F.upper(F.col("status")).alias("status"))
        .limit(1)
        .collect()
    )
    if prior_status_rows:
        previous_status = str(prior_status_rows[0]["status"] or "").upper()

    eligible_items: List[Dict[str, str]] = []
    if run_mode == "full":
        eligible_items.append({"call_id": status_item_key, "index_name": final_index_name})
    elif run_mode in {"sample", "incremental"}:
        if previous_status is None or previous_status == "FAILED":
            eligible_items.append({"call_id": status_item_key, "index_name": final_index_name})

    if run_mode == "sample" and max_files_per_run is not None:
        eligible_items = eligible_items[:max_files_per_run]

    eligible_count = len(eligible_items)
    print(f"[{STAGE_NAME}] Eligible index items: {eligible_count}")

    status_records: List[Dict[str, object]] = []
    process_ts = datetime.utcnow()

    for item in eligible_items:
        item_key = item["call_id"]
        try:
            if not enable_rag:
                ops_skipped_count += 1
                status_records.append(
                    {
                        "call_id": item_key,
                        "stage_name": STAGE_NAME,
                        "status": "SKIPPED",
                        "error_message": "RAG disabled by `enable_rag_index=false`.",
                        "run_id": run_id,
                        "updated_at": process_ts,
                    }
                )
                continue

            if not _table_exists(catalog, schema, "silver_embeddings"):
                raise RuntimeError(f"Required input table is missing: {embeddings_table}")
            if not _table_exists(catalog, schema, "silver_text_chunks"):
                raise RuntimeError(f"Required input table is missing: {chunks_table}")

            embeddings_df = spark.table(embeddings_table)
            required_embedding_columns = {
                "call_id",
                "chunk_id",
                "embedding_model",
                "embedding_vector",
                "embedding_version",
                "run_id",
            }
            missing_embedding_columns = sorted(
                required_embedding_columns - set(embeddings_df.columns)
            )
            if missing_embedding_columns:
                raise RuntimeError(
                    f"Input table {embeddings_table} is missing required columns: "
                    f"{missing_embedding_columns}"
                )

            chunking_version_column = (
                "chunking_version" if "chunking_version" in embeddings_df.columns else None
            )
            if chunking_version_filter and chunking_version_column is None:
                raise RuntimeError(
                    "chunking_version_filter was provided but `chunking_version` "
                    f"column is missing in {embeddings_table}."
                )

            filtered_embeddings_df = embeddings_df.select(*embeddings_df.columns)
            if embedding_model_filter:
                filtered_embeddings_df = filtered_embeddings_df.where(
                    F.col("embedding_model") == embedding_model_filter
                )
            if embedding_version_filter:
                filtered_embeddings_df = filtered_embeddings_df.where(
                    F.col("embedding_version") == embedding_version_filter
                )
            if chunking_version_filter and chunking_version_column:
                filtered_embeddings_df = filtered_embeddings_df.where(
                    F.col(chunking_version_column) == chunking_version_filter
                )

            embedding_count = filtered_embeddings_df.count()
            if embedding_count <= 0:
                raise RuntimeError(
                    "Missing embeddings for configured RAG scope. "
                    "Precondition failure for enable_rag_index=true."
                )

            chunks_df = spark.table(chunks_table)
            required_chunk_columns = {"call_id", "chunk_id", "chunk_text"}
            missing_chunk_columns = sorted(required_chunk_columns - set(chunks_df.columns))
            if missing_chunk_columns:
                raise RuntimeError(
                    f"Input table {chunks_table} is missing required columns: "
                    f"{missing_chunk_columns}"
                )

            chunk_keys_df = chunks_df.select("call_id", "chunk_id").dropDuplicates()
            embedding_keys_df = (
                filtered_embeddings_df.select("call_id", "chunk_id").dropDuplicates()
            )
            matched_key_count = embedding_keys_df.join(
                chunk_keys_df, on=["call_id", "chunk_id"], how="inner"
            ).count()
            if matched_key_count <= 0:
                raise RuntimeError(
                    "No matching chunk keys between silver_embeddings and silver_text_chunks "
                    "for the selected scope."
                )

            if resolved_backend == "skip":
                ops_skipped_count += 1
                status_records.append(
                    {
                        "call_id": item_key,
                        "stage_name": STAGE_NAME,
                        "status": "SKIPPED",
                        "error_message": "RAG backend configured as skip.",
                        "run_id": run_id,
                        "updated_at": process_ts,
                    }
                )
                continue

            if resolved_backend == "delta_similarity":
                note = (
                    backend_resolution_note
                    or "Delta-similarity backend selected; no external index object is created."
                )
                ops_success_count += 1
                status_records.append(
                    {
                        "call_id": item_key,
                        "stage_name": STAGE_NAME,
                        "status": "SUCCESS",
                        "error_message": note,
                        "run_id": run_id,
                        "updated_at": process_ts,
                    }
                )
                continue

            if resolved_backend == "databricks_vector_search":
                action = _create_or_sync_vector_search_index(
                    endpoint_name=vector_search_endpoint_name,
                    index_name=final_index_name,
                    source_table=embeddings_table,
                    primary_key="chunk_id",
                    embedding_vector_column="embedding_vector",
                )
                ops_success_count += 1
                status_records.append(
                    {
                        "call_id": item_key,
                        "stage_name": STAGE_NAME,
                        "status": "SUCCESS",
                        "error_message": (
                            f"Databricks Vector Search index {final_index_name} "
                            f"({action}) on endpoint {vector_search_endpoint_name}."
                        ),
                        "run_id": run_id,
                        "updated_at": process_ts,
                    }
                )
                continue

            raise RuntimeError(f"Unhandled backend resolution: {resolved_backend}")
        except Exception as exc:
            ops_failed_count += 1
            status_records.append(
                {
                    "call_id": item_key,
                    "stage_name": STAGE_NAME,
                    "status": "FAILED",
                    "error_message": _truncate_error(exc),
                    "run_id": run_id,
                    "updated_at": process_ts,
                }
            )

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
        status_df.createOrReplaceTempView("tmp_insights_05_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file_status_table} AS t
            USING tmp_insights_05_status AS s
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
        error_summary = "Zero eligible RAG index items completed in insights_05 stage."
        raise RuntimeError(error_summary)

    if ops_failed_count > 0 and success_like_count > 0:
        final_status = "WARN"
        error_summary = f"{ops_failed_count} RAG index item(s) failed in {STAGE_NAME}."
    elif ops_failed_count > 0:
        final_status = "FAILED"
        error_summary = f"{ops_failed_count} RAG index item(s) failed in {STAGE_NAME}."
    elif eligible_count == 0:
        final_status = "SUCCESS"
        error_summary = f"No eligible RAG index items for stage {STAGE_NAME}."
    elif not enable_rag:
        final_status = "SUCCESS"
        error_summary = "RAG indexing disabled; eligible items marked SKIPPED."
    else:
        final_status = "SUCCESS"
        error_summary = backend_resolution_note

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
        f"[{STAGE_NAME}] eligible={eligible_count} success_items={ops_success_count} "
        f"skipped_items={ops_skipped_count} failed_items={ops_failed_count} "
        f"status={final_status} index_name={final_index_name}"
    )
