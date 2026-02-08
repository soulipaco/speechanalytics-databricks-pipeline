# Databricks notebook source
# MAGIC %md
# MAGIC # INSIGHTS_06_LLM_EXTRACT_CHUNK_INSIGHTS

# COMMAND ----------

import hashlib
import json
import os
import re
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T

WORKFLOW_NAME = "insights"
STAGE_NAME = "insights_06_llm_extract_chunk_insights"
OUTPUT_TABLE_NAME = "silver_llm_chunk_insights"

ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
ALLOWED_LLM_BACKENDS = {"auto", "databricks_endpoint", "openai", "skip"}
ALLOWED_RAG_BACKENDS = {"auto", "vector_search", "delta_similarity", "skip", "none"}
SENTIMENT_VALUES = {"Positive", "Neutral", "Negative"}


def _is_dbx():
    return "dbutils" in globals()


def _lit(v):
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


def _truncate(msg, n=1000):
    return (str(msg).strip() or "Unknown error")[:n]


def _validate_identifier(name, value):
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


def _fq(catalog, schema, table):
    return f"`{catalog}`.`{schema}`.`{table}`"


def _exists(catalog, schema, table):
    return bool(spark.catalog.tableExists(f"{catalog}.{schema}.{table}"))


def _hash_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _json_text(raw):
    text = str(raw or "").strip()
    text = re.sub(r"^```(?:json)?\\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\\s*```$", "", text, flags=re.IGNORECASE)
    i, j = text.find("{"), text.rfind("}")
    if i < 0 or j <= i:
        raise ValueError("LLM response missing JSON object.")
    return text[i : j + 1]


def _payload_text(payload):
    if payload is None:
        raise ValueError("Empty LLM payload.")
    if isinstance(payload, str):
        return payload.strip()
    if isinstance(payload, dict):
        if "choices" in payload and payload["choices"]:
            c0 = payload["choices"][0]
            if isinstance(c0, dict):
                if isinstance(c0.get("message"), dict):
                    return str(c0["message"].get("content", "")).strip()
                return str(c0.get("text", "")).strip()
        for k in ("text", "output_text", "content", "response", "predictions", "outputs", "result"):
            if k in payload and payload[k] is not None:
                return _payload_text(payload[k])
        return json.dumps(payload, ensure_ascii=True)
    if isinstance(payload, list) and payload:
        return _payload_text(payload[0])
    return str(payload).strip()


def _ensure_tables(out_table, ops_file, ops_runs):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {out_table} (
          call_id STRING,
          chunk_id STRING,
          chunk_text_hash STRING,
          taxonomy_version STRING,
          candidate_driver_label STRING,
          candidate_driver_confidence DOUBLE,
          candidate_issue_label STRING,
          candidate_issue_confidence DOUBLE,
          candidate_intent_label STRING,
          candidate_intent_confidence DOUBLE,
          pii_possible_remaining_flag BOOLEAN,
          pii_notes STRING,
          chunk_summary STRING,
          sentiment_signal STRING,
          sentiment_confidence DOUBLE,
          customer_emotion_signal STRING,
          agent_emotion_signal STRING,
          llm_model_name STRING,
          llm_provider STRING,
          llm_backend STRING,
          llm_prompt_version STRING,
          extraction_version STRING,
          rag_used_flag BOOLEAN,
          rag_backend STRING,
          rag_index_name STRING,
          rag_top_k INT,
          rag_retrieved_chunk_ids ARRAY<STRING>,
          llm_response_hash STRING,
          run_id STRING,
          updated_at TIMESTAMP
        ) USING DELTA
        """
    )
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
        f"DELETE FROM {ops_runs} WHERE run_id = {_lit(run_id)} AND workflow_name = {_lit(WORKFLOW_NAME)}"
    )
    spark.sql(
        f"""
        INSERT INTO {ops_runs}
        SELECT
          {_lit(run_id)} AS run_id,
          {_lit(WORKFLOW_NAME)} AS workflow_name,
          current_timestamp() AS started_at,
          CAST(NULL AS TIMESTAMP) AS ended_at,
          'RUNNING' AS status,
          NULL AS trigger_type,
          {_lit(params_json)} AS parameters,
          CAST(NULL AS STRING) AS error_summary,
          CAST(0 AS BIGINT) AS total_files,
          CAST(0 AS BIGINT) AS success_count,
          CAST(0 AS BIGINT) AS failed_count,
          current_timestamp() AS updated_at
        """
    )


def _upsert_run_final(ops_runs, run_id, status, total_files, success_count, failed_count, error_summary):
    spark.sql(
        f"DELETE FROM {ops_runs} WHERE run_id = {_lit(run_id)} AND workflow_name = {_lit(WORKFLOW_NAME)}"
    )
    spark.sql(
        f"""
        INSERT INTO {ops_runs}
        SELECT
          {_lit(run_id)} AS run_id,
          {_lit(WORKFLOW_NAME)} AS workflow_name,
          current_timestamp() AS started_at,
          current_timestamp() AS ended_at,
          {_lit(status)} AS status,
          NULL AS trigger_type,
          NULL AS parameters,
          {_lit(error_summary)} AS error_summary,
          CAST({int(total_files)} AS BIGINT) AS total_files,
          CAST({int(success_count)} AS BIGINT) AS success_count,
          CAST({int(failed_count)} AS BIGINT) AS failed_count,
          current_timestamp() AS updated_at
        """
    )


def _read(payload, keys, required=True):
    for k in keys:
        if k in payload:
            return payload[k]
    if required:
        raise ValueError(f"Missing key. Expected one of {list(keys)}")
    return None


def _confidence(name, value):
    try:
        v = float(value)
    except Exception as exc:
        raise ValueError(f"{name} must be numeric.") from exc
    if v < 0.0 or v > 1.0:
        raise ValueError(f"{name} must be in [0,1].")
    return float(v)


def _as_bool(name, value):
    if isinstance(value, bool):
        return value
    return _parse_bool(name, str(value), default=False)


def _resolve_taxonomy_version(catalog, schema, requested):
    for n in ["dim_contact_driver", "dim_issue", "dim_intent", "dim_emotion_catalog"]:
        if not _exists(catalog, schema, n):
            raise RuntimeError(f"Required dimension table is missing: {_fq(catalog, schema, n)}")

    t_driver = _fq(catalog, schema, "dim_contact_driver")
    t_issue = _fq(catalog, schema, "dim_issue")
    t_intent = _fq(catalog, schema, "dim_intent")
    t_emo = _fq(catalog, schema, "dim_emotion_catalog")

    tv = requested.strip()
    if not tv:
        rows = (
            spark.table(t_driver)
            .select(F.col("taxonomy_version").cast("string").alias("taxonomy_version"))
            .where("taxonomy_version IS NOT NULL")
            .orderBy(F.col("taxonomy_version").desc())
            .limit(1)
            .collect()
        )
        if not rows:
            raise RuntimeError("Cannot resolve taxonomy_version from dim_contact_driver.")
        tv = str(rows[0]["taxonomy_version"]).strip()

    drivers = {
        str(r["label"]).strip()
        for r in spark.table(t_driver)
        .where((F.col("taxonomy_version") == tv) & (F.col("active_flag") == F.lit(True)))
        .select(F.col("label").cast("string").alias("label"))
        .collect()
        if str(r["label"]).strip()
    }
    issues = {
        str(r["label"]).strip()
        for r in spark.table(t_issue)
        .where((F.col("taxonomy_version") == tv) & (F.col("active_flag") == F.lit(True)))
        .select(F.col("label").cast("string").alias("label"))
        .collect()
        if str(r["label"]).strip()
    }
    intents = {
        str(r["label"]).strip()
        for r in spark.table(t_intent)
        .where((F.col("taxonomy_version") == tv) & (F.col("active_flag") == F.lit(True)))
        .select(F.col("label").cast("string").alias("label"))
        .collect()
        if str(r["label"]).strip()
    }
    emotions = {
        str(r["emotion_name"]).strip()
        for r in spark.table(t_emo)
        .where(((F.col("taxonomy_version") == tv) | (F.col("catalog_version") == tv)) & (F.col("active_flag") == F.lit(True)))
        .select(F.col("emotion_name").cast("string").alias("emotion_name"))
        .collect()
        if str(r["emotion_name"]).strip()
    }

    allowed = {"driver": drivers, "issue": issues, "intent": intents, "emotion": emotions}
    for k, vals in allowed.items():
        if not vals:
            raise RuntimeError(f"No active taxonomy values for {k} at taxonomy_version={tv!r}.")
    return tv, allowed

def _resolve_llm_backend(raw_backend, endpoint, openai_key):
    if raw_backend == "auto":
        if endpoint:
            return "databricks_endpoint", "auto->databricks_endpoint"
        if openai_key:
            return "openai", "auto->openai"
        return "skip", "auto->skip (no endpoint/api key)"
    return raw_backend, None


def _build_llm_fn(backend, model, endpoint, openai_key, openai_base_url, temperature, max_tokens, timeout_sec):
    if backend == "skip":
        return "skip", None, "LLM backend skip."
    if backend == "databricks_endpoint":
        if not endpoint:
            return None, None, "llm_endpoint_name required for databricks_endpoint"
        try:
            import mlflow.deployments  # type: ignore

            client = mlflow.deployments.get_deploy_client("databricks")

            def _call(prompt):
                payload = {
                    "messages": [
                        {"role": "system", "content": "Return JSON only."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": float(temperature),
                    "max_tokens": int(max_tokens),
                    "model": model,
                }
                return _payload_text(client.predict(endpoint=endpoint, inputs=payload))

            return "databricks_endpoint", _call, None
        except Exception as exc:
            return None, None, _truncate(exc, 500)
    if backend == "openai":
        if not openai_key:
            return None, None, "openai_api_key required for openai backend"
        try:
            from openai import OpenAI  # type: ignore

            client = OpenAI(api_key=openai_key, base_url=(openai_base_url or None))

            def _call(prompt):
                resp = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "Return JSON only."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=float(temperature),
                    max_tokens=int(max_tokens),
                    timeout=int(timeout_sec),
                )
                if not resp or not getattr(resp, "choices", None):
                    raise RuntimeError("OpenAI response missing choices")
                return str(resp.choices[0].message.content or "").strip()

            return "openai", _call, None
        except Exception as exc:
            return None, None, _truncate(exc, 500)
    return None, None, f"Unsupported backend {backend!r}"


def _vector_search_available():
    try:
        import databricks.vector_search.client  # type: ignore  # noqa: F401

        return True
    except Exception:
        return False


def _vector_search_ids(index_name, query_text, top_k):
    if not index_name or not _vector_search_available():
        return []
    try:
        from databricks.vector_search.client import VectorSearchClient  # type: ignore

        client = VectorSearchClient()
        idx = client.get_index(index_name=index_name)
        if not hasattr(idx, "similarity_search"):
            return []
        out = idx.similarity_search(query_text=query_text, num_results=int(top_k))
        result = []
        if isinstance(out, dict):
            data = out.get("result", {}).get("data_array", [])
            for row in data:
                if isinstance(row, dict) and row.get("chunk_id"):
                    result.append(str(row["chunk_id"]))
        return result[: int(top_k)]
    except Exception:
        return []


def _neighbor_ids(chunks, idx, top_k):
    pairs = [(abs(j - idx), str(c["chunk_id"])) for j, c in enumerate(chunks) if j != idx]
    pairs.sort(key=lambda x: (x[0], x[1]))
    return [cid for _, cid in pairs[: int(top_k)]]


def _build_prompt(chunk_text, taxonomy_version, prompt_version, allowed, rag_snips):
    context = "\n".join([f"- {s}" for s in rag_snips if s]) or "- none"
    return (
        "Return exactly one JSON object and no markdown.\n"
        f"taxonomy_version={taxonomy_version}; prompt_version={prompt_version}\n"
        f"allowed_driver={sorted(allowed['driver'])}\n"
        f"allowed_issue={sorted(allowed['issue'])}\n"
        f"allowed_intent={sorted(allowed['intent'])}\n"
        f"allowed_emotion={sorted(allowed['emotion'])}\n"
        f"allowed_sentiment={sorted(SENTIMENT_VALUES)}\n"
        "Required keys: chunk_summary,candidate_driver_label,candidate_driver_confidence,"
        "candidate_issue_label,candidate_issue_confidence,candidate_intent_label,"
        "candidate_intent_confidence,pii_possible_remaining_flag,pii_notes,"
        "sentiment_signal,sentiment_confidence,customer_emotion_signal,agent_emotion_signal.\n"
        f"RAG context:\n{context}\n"
        f"Chunk text:\n{chunk_text}"
    )


def _validate_output(raw_text, allowed):
    payload = json.loads(_json_text(raw_text))
    if not isinstance(payload, dict):
        raise ValueError("LLM JSON must be an object.")
    summary = str(_read(payload, ["chunk_summary", "summary", "summary_text"])).strip()
    driver = str(_read(payload, ["candidate_driver_label", "contact_driver_label"])).strip()
    issue = str(_read(payload, ["candidate_issue_label", "issue_label"])).strip()
    intent = str(_read(payload, ["candidate_intent_label", "intent_label"])).strip()
    cust_emo = str(_read(payload, ["customer_emotion_signal", "customer_emotion"])).strip()
    agent_emo = str(_read(payload, ["agent_emotion_signal", "agent_emotion"])).strip()
    sentiment = str(_read(payload, ["sentiment_signal", "sentiment"])).strip()

    if not summary:
        raise ValueError("chunk_summary cannot be empty.")
    if driver not in allowed["driver"] or issue not in allowed["issue"] or intent not in allowed["intent"]:
        raise ValueError("Taxonomy labels outside allowed sets.")
    if cust_emo not in allowed["emotion"] or agent_emo not in allowed["emotion"]:
        raise ValueError("Emotion labels outside allowed set.")
    if sentiment not in SENTIMENT_VALUES:
        raise ValueError("Invalid sentiment value.")

    return {
        "chunk_summary": summary,
        "candidate_driver_label": driver,
        "candidate_driver_confidence": _confidence("candidate_driver_confidence", _read(payload, ["candidate_driver_confidence", "contact_driver_confidence"])),
        "candidate_issue_label": issue,
        "candidate_issue_confidence": _confidence("candidate_issue_confidence", _read(payload, ["candidate_issue_confidence", "issue_confidence"])),
        "candidate_intent_label": intent,
        "candidate_intent_confidence": _confidence("candidate_intent_confidence", _read(payload, ["candidate_intent_confidence", "intent_confidence"])),
        "pii_possible_remaining_flag": _as_bool("pii_possible_remaining_flag", _read(payload, ["pii_possible_remaining_flag", "pii_flag"])),
        "pii_notes": str(_read(payload, ["pii_notes"], required=False) or "").strip(),
        "sentiment_signal": sentiment,
        "sentiment_confidence": _confidence("sentiment_confidence", _read(payload, ["sentiment_confidence"], required=False) or 0.5),
        "customer_emotion_signal": cust_emo,
        "agent_emotion_signal": agent_emo,
    }

if _is_dbx():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("enable_llm", "false")
    dbutils.widgets.text("enable_llm_chunk", "")
    dbutils.widgets.text("enable_llm_insights", "")
    dbutils.widgets.text("llm_backend", "auto")
    dbutils.widgets.text("llm_model_name", "")
    dbutils.widgets.text("llm_temperature", "0.0")
    dbutils.widgets.text("llm_max_tokens", "512")
    dbutils.widgets.text("llm_timeout_sec", "45")
    dbutils.widgets.text("llm_endpoint_name", "")
    dbutils.widgets.text("openai_api_key", "")
    dbutils.widgets.text("openai_base_url", "")
    dbutils.widgets.text("taxonomy_version", "")
    dbutils.widgets.text("prompt_version", "v1")
    dbutils.widgets.text("extraction_version", "v1")
    dbutils.widgets.text("enable_rag", "false")
    dbutils.widgets.text("rag_top_k", "5")
    dbutils.widgets.text("rag_backend", "auto")
    dbutils.widgets.text("rag_index_name", "")

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    enable_llm_raw = (
        dbutils.widgets.get("enable_llm").strip()
        or dbutils.widgets.get("enable_llm_chunk").strip()
        or dbutils.widgets.get("enable_llm_insights").strip()
        or "false"
    )
    llm_backend = dbutils.widgets.get("llm_backend").strip().lower()
    llm_model_name = dbutils.widgets.get("llm_model_name").strip()
    llm_temperature = float(dbutils.widgets.get("llm_temperature").strip() or "0.0")
    llm_max_tokens = int(dbutils.widgets.get("llm_max_tokens").strip() or "512")
    llm_timeout_sec = int(dbutils.widgets.get("llm_timeout_sec").strip() or "45")
    llm_endpoint_name = dbutils.widgets.get("llm_endpoint_name").strip()
    openai_api_key = dbutils.widgets.get("openai_api_key").strip()
    openai_base_url = dbutils.widgets.get("openai_base_url").strip()
    taxonomy_version = dbutils.widgets.get("taxonomy_version").strip()
    prompt_version = dbutils.widgets.get("prompt_version").strip() or "v1"
    extraction_version = dbutils.widgets.get("extraction_version").strip() or "v1"
    enable_rag = _parse_bool("enable_rag", dbutils.widgets.get("enable_rag").strip() or "false")
    rag_top_k = int(dbutils.widgets.get("rag_top_k").strip() or "5")
    rag_backend = dbutils.widgets.get("rag_backend").strip().lower()
    rag_index_name = dbutils.widgets.get("rag_index_name").strip()
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    enable_llm_raw = (
        os.getenv("ENABLE_LLM", "").strip()
        or os.getenv("ENABLE_LLM_CHUNK", "").strip()
        or os.getenv("ENABLE_LLM_INSIGHTS", "").strip()
        or "false"
    )
    llm_backend = os.getenv("LLM_BACKEND", "auto").strip().lower()
    llm_model_name = os.getenv("LLM_MODEL_NAME", "").strip()
    llm_temperature = float(os.getenv("LLM_TEMPERATURE", "0.0").strip())
    llm_max_tokens = int(os.getenv("LLM_MAX_TOKENS", "512").strip())
    llm_timeout_sec = int(os.getenv("LLM_TIMEOUT_SEC", "45").strip())
    llm_endpoint_name = os.getenv("LLM_ENDPOINT_NAME", "").strip()
    openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
    openai_base_url = os.getenv("OPENAI_BASE_URL", "").strip()
    taxonomy_version = os.getenv("TAXONOMY_VERSION", "").strip()
    prompt_version = os.getenv("PROMPT_VERSION", "v1").strip() or "v1"
    extraction_version = os.getenv("EXTRACTION_VERSION", "v1").strip() or "v1"
    enable_rag = _parse_bool("enable_rag", os.getenv("ENABLE_RAG", "false").strip() or "false")
    rag_top_k = int(os.getenv("RAG_TOP_K", "5").strip())
    rag_backend = os.getenv("RAG_BACKEND", "auto").strip().lower()
    rag_index_name = os.getenv("RAG_INDEX_NAME", "").strip()

catalog = _validate_identifier("catalog", catalog)
schema = _validate_identifier("schema", schema)
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid run_mode {run_mode!r}")
if llm_backend not in ALLOWED_LLM_BACKENDS:
    raise ValueError(f"Invalid llm_backend {llm_backend!r}")
if rag_backend not in ALLOWED_RAG_BACKENDS:
    raise ValueError(f"Invalid rag_backend {rag_backend!r}")
if llm_max_tokens <= 0:
    raise ValueError("llm_max_tokens must be > 0")
if rag_top_k <= 0:
    raise ValueError("rag_top_k must be > 0")

enable_llm = _parse_bool("enable_llm", enable_llm_raw, default=False)
if enable_llm and not llm_model_name:
    raise ValueError("llm_model_name is required when enable_llm=true")

max_files_per_run = int(max_files_per_run_raw) if max_files_per_run_raw else None
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError("sample mode requires max_files_per_run > 0")

chunks_table = _fq(catalog, schema, "silver_text_chunks")
out_table = _fq(catalog, schema, OUTPUT_TABLE_NAME)
ops_file = _fq(catalog, schema, "ops_file_status")
ops_runs = _fq(catalog, schema, "ops_pipeline_runs")

params_json = json.dumps(
    {
        "catalog": catalog,
        "schema": schema,
        "run_id": run_id,
        "run_mode": run_mode,
        "max_files_per_run": max_files_per_run,
        "enable_llm": enable_llm,
        "llm_backend": llm_backend,
        "llm_model_name": llm_model_name or None,
        "taxonomy_version": taxonomy_version or None,
        "prompt_version": prompt_version,
        "extraction_version": extraction_version,
        "enable_rag": enable_rag,
        "rag_backend": rag_backend,
        "rag_top_k": rag_top_k,
        "rag_index_name": rag_index_name or None,
    },
    sort_keys=True,
)

_ensure_tables(out_table, ops_file, ops_runs)
_upsert_run_running(ops_runs, run_id, params_json)

eligible_count = 0
success_calls = 0
failed_calls = 0
skipped_calls = 0
rows_written = 0
final_status = "SUCCESS"
error_summary = None

try:
    if not _exists(catalog, schema, "silver_text_chunks"):
        raise RuntimeError(f"Required table missing: {chunks_table}")

    chunks_df = spark.table(chunks_table)
    missing_cols = sorted({"call_id", "chunk_id", "chunk_text"} - set(chunks_df.columns))
    if missing_cols:
        raise RuntimeError(f"{chunks_table} missing columns: {missing_cols}")

    chunks_df = chunks_df.select(
        F.col("call_id").cast("string").alias("call_id"),
        F.col("chunk_id").cast("string").alias("chunk_id"),
        F.col("chunk_text").cast("string").alias("chunk_text"),
        (F.col("start_sec").cast("double") if "start_sec" in chunks_df.columns else F.lit(0.0)).alias("start_sec"),
        (F.col("end_sec").cast("double") if "end_sec" in chunks_df.columns else F.lit(0.0)).alias("end_sec"),
    ).where("call_id IS NOT NULL AND chunk_id IS NOT NULL")

    taxonomy_version, allowed = _resolve_taxonomy_version(catalog, schema, taxonomy_version)
    llm_backend_resolved, llm_backend_note = _resolve_llm_backend(llm_backend, llm_endpoint_name, openai_api_key)
    llm_provider, llm_fn, llm_build_error = _build_llm_fn(
        llm_backend_resolved,
        llm_model_name,
        llm_endpoint_name,
        openai_api_key,
        openai_base_url,
        llm_temperature,
        llm_max_tokens,
        llm_timeout_sec,
    )

    stage_df = (
        spark.table(ops_file)
        .where(F.col("stage_name") == STAGE_NAME)
        .select("call_id", F.upper(F.col("status")).alias("stage_status"))
    )

    call_df = chunks_df.select("call_id").distinct().orderBy("call_id").join(stage_df, on="call_id", how="left")
    if run_mode in {"sample", "incremental"}:
        call_df = call_df.where(F.col("stage_status").isNull() | (F.col("stage_status") == "FAILED"))
    if run_mode == "sample" and max_files_per_run is not None:
        call_df = call_df.limit(max_files_per_run)

    eligible_calls = [r["call_id"] for r in call_df.select("call_id").collect()]
    eligible_count = len(eligible_calls)

    ts = datetime.utcnow()
    status_rows = []
    out_rows = []

    for call_id in eligible_calls:
        if not enable_llm or llm_backend_resolved == "skip":
            skipped_calls += 1
            status_rows.append(
                {
                    "call_id": str(call_id),
                    "stage_name": STAGE_NAME,
                    "status": "SKIPPED",
                    "error_message": "LLM disabled." if not enable_llm else (llm_backend_note or "LLM backend skip."),
                    "run_id": run_id,
                    "updated_at": ts,
                }
            )
            continue

        call_chunks = [
            {
                "call_id": str(r["call_id"]),
                "chunk_id": str(r["chunk_id"]),
                "chunk_text": str(r["chunk_text"] or "").strip(),
            }
            for r in chunks_df.where(F.col("call_id") == call_id).orderBy("start_sec", "end_sec", "chunk_id").collect()
        ]

        chunk_ok = 0
        chunk_fail = 0
        errs = []
        call_out_rows = []

        for idx, chunk in enumerate(call_chunks):
            chunk_text = chunk["chunk_text"]
            if not chunk_text:
                chunk_fail += 1
                errs.append(f"{chunk['chunk_id']}:empty")
                continue
            try:
                if llm_fn is None:
                    raise RuntimeError(llm_build_error or "LLM backend unavailable.")

                rag_used = False
                rag_backend_eff = "none"
                rag_ids = []

                if enable_rag:
                    if rag_backend == "vector_search":
                        rag_backend_eff = "vector_search"
                        rag_ids = _vector_search_ids(rag_index_name, chunk_text, rag_top_k)
                        rag_used = len(rag_ids) > 0
                    elif rag_backend == "delta_similarity":
                        rag_backend_eff = "delta_similarity"
                        rag_ids = _neighbor_ids(call_chunks, idx, rag_top_k)
                        rag_used = len(rag_ids) > 0
                    elif rag_backend == "auto":
                        rag_ids = _vector_search_ids(rag_index_name, chunk_text, rag_top_k)
                        if rag_ids:
                            rag_backend_eff = "vector_search"
                            rag_used = True
                        else:
                            rag_backend_eff = "delta_similarity"
                            rag_ids = _neighbor_ids(call_chunks, idx, rag_top_k)
                            rag_used = len(rag_ids) > 0
                    elif rag_backend in {"skip", "none"}:
                        rag_backend_eff = rag_backend
                        rag_used = False

                rag_id_set = set(rag_ids)
                rag_snips = [c["chunk_text"][:800] for c in call_chunks if c["chunk_id"] in rag_id_set and c["chunk_text"]]

                prompt = _build_prompt(chunk_text, taxonomy_version, prompt_version, allowed, rag_snips)
                llm_raw = llm_fn(prompt)
                parsed = _validate_output(llm_raw, allowed)

                call_out_rows.append(
                    {
                        "call_id": chunk["call_id"],
                        "chunk_id": chunk["chunk_id"],
                        "chunk_text_hash": _hash_text(chunk_text),
                        "taxonomy_version": taxonomy_version,
                        "candidate_driver_label": parsed["candidate_driver_label"],
                        "candidate_driver_confidence": parsed["candidate_driver_confidence"],
                        "candidate_issue_label": parsed["candidate_issue_label"],
                        "candidate_issue_confidence": parsed["candidate_issue_confidence"],
                        "candidate_intent_label": parsed["candidate_intent_label"],
                        "candidate_intent_confidence": parsed["candidate_intent_confidence"],
                        "pii_possible_remaining_flag": parsed["pii_possible_remaining_flag"],
                        "pii_notes": parsed["pii_notes"],
                        "chunk_summary": parsed["chunk_summary"],
                        "sentiment_signal": parsed["sentiment_signal"],
                        "sentiment_confidence": parsed["sentiment_confidence"],
                        "customer_emotion_signal": parsed["customer_emotion_signal"],
                        "agent_emotion_signal": parsed["agent_emotion_signal"],
                        "llm_model_name": llm_model_name,
                        "llm_provider": llm_provider or llm_backend_resolved,
                        "llm_backend": llm_backend_resolved,
                        "llm_prompt_version": prompt_version,
                        "extraction_version": extraction_version,
                        "rag_used_flag": bool(rag_used),
                        "rag_backend": rag_backend_eff,
                        "rag_index_name": rag_index_name or None,
                        "rag_top_k": int(rag_top_k),
                        "rag_retrieved_chunk_ids": [str(x) for x in rag_ids],
                        "llm_response_hash": _hash_text(llm_raw),
                        "run_id": run_id,
                        "updated_at": ts,
                    }
                )
                chunk_ok += 1
            except Exception as exc:
                chunk_fail += 1
                errs.append(f"{chunk['chunk_id']}:{_truncate(exc, 200)}")

        if chunk_ok > 0:
            success_calls += 1
            out_rows.extend(call_out_rows)
            status_rows.append(
                {
                    "call_id": str(call_id),
                    "stage_name": STAGE_NAME,
                    "status": "SUCCESS",
                    "error_message": None if chunk_fail == 0 else f"partial_failures={chunk_fail}; " + "; ".join(errs[:3]),
                    "run_id": run_id,
                    "updated_at": ts,
                }
            )
        else:
            failed_calls += 1
            status_rows.append(
                {
                    "call_id": str(call_id),
                    "stage_name": STAGE_NAME,
                    "status": "FAILED",
                    "error_message": f"all_chunks_failed={chunk_fail}; " + "; ".join(errs[:3]),
                    "run_id": run_id,
                    "updated_at": ts,
                }
            )

    if out_rows:
        row_schema = T.StructType([
            T.StructField("call_id", T.StringType(), False),
            T.StructField("chunk_id", T.StringType(), False),
            T.StructField("chunk_text_hash", T.StringType(), False),
            T.StructField("taxonomy_version", T.StringType(), False),
            T.StructField("candidate_driver_label", T.StringType(), False),
            T.StructField("candidate_driver_confidence", T.DoubleType(), False),
            T.StructField("candidate_issue_label", T.StringType(), False),
            T.StructField("candidate_issue_confidence", T.DoubleType(), False),
            T.StructField("candidate_intent_label", T.StringType(), False),
            T.StructField("candidate_intent_confidence", T.DoubleType(), False),
            T.StructField("pii_possible_remaining_flag", T.BooleanType(), False),
            T.StructField("pii_notes", T.StringType(), True),
            T.StructField("chunk_summary", T.StringType(), False),
            T.StructField("sentiment_signal", T.StringType(), False),
            T.StructField("sentiment_confidence", T.DoubleType(), False),
            T.StructField("customer_emotion_signal", T.StringType(), False),
            T.StructField("agent_emotion_signal", T.StringType(), False),
            T.StructField("llm_model_name", T.StringType(), False),
            T.StructField("llm_provider", T.StringType(), False),
            T.StructField("llm_backend", T.StringType(), False),
            T.StructField("llm_prompt_version", T.StringType(), False),
            T.StructField("extraction_version", T.StringType(), False),
            T.StructField("rag_used_flag", T.BooleanType(), False),
            T.StructField("rag_backend", T.StringType(), False),
            T.StructField("rag_index_name", T.StringType(), True),
            T.StructField("rag_top_k", T.IntegerType(), False),
            T.StructField("rag_retrieved_chunk_ids", T.ArrayType(T.StringType()), False),
            T.StructField("llm_response_hash", T.StringType(), False),
            T.StructField("run_id", T.StringType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ])
        out_df = spark.createDataFrame(out_rows, schema=row_schema)

        key_df = out_df.select("call_id", "chunk_id").dropDuplicates()
        key_df.createOrReplaceTempView("tmp_insights_06_keys")
        spark.sql(
            f"""
            MERGE INTO {out_table} AS t
            USING tmp_insights_06_keys AS s
            ON t.call_id = s.call_id
               AND t.chunk_id = s.chunk_id
               AND t.extraction_version = {_lit(extraction_version)}
            WHEN MATCHED THEN DELETE
            """
        )

        out_df.write.format("delta").mode("append").saveAsTable(out_table)
        rows_written = len(out_rows)

    if status_rows:
        status_schema = T.StructType([
            T.StructField("call_id", T.StringType(), False),
            T.StructField("stage_name", T.StringType(), False),
            T.StructField("status", T.StringType(), False),
            T.StructField("error_message", T.StringType(), True),
            T.StructField("run_id", T.StringType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ])
        status_df = spark.createDataFrame(status_rows, schema=status_schema)
        status_df.createOrReplaceTempView("tmp_insights_06_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file} AS t
            USING tmp_insights_06_status AS s
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

    success_like = success_calls + skipped_calls
    if eligible_count > 0 and success_like == 0:
        final_status = "FAILED"
        error_summary = "Zero eligible calls completed."
        raise RuntimeError(error_summary)

    if failed_calls > 0 and success_like > 0:
        final_status = "WARN"
        error_summary = f"{failed_calls} call(s) failed."
    elif failed_calls > 0:
        final_status = "FAILED"
        error_summary = f"{failed_calls} call(s) failed."
    elif eligible_count == 0:
        final_status = "SUCCESS"
        error_summary = f"No eligible calls for {STAGE_NAME}."
    elif not enable_llm or llm_backend_resolved == "skip":
        final_status = "SUCCESS"
        error_summary = "LLM extraction skipped."
    else:
        final_status = "SUCCESS"
        error_summary = llm_backend_note

except Exception as exc:
    final_status = "FAILED"
    if error_summary is None:
        error_summary = _truncate(exc)
    raise
finally:
    _upsert_run_final(
        ops_runs,
        run_id=run_id,
        status=final_status,
        total_files=eligible_count,
        success_count=success_calls + skipped_calls,
        failed_count=failed_calls,
        error_summary=error_summary,
    )
    print(
        f"[{STAGE_NAME}] eligible={eligible_count} success={success_calls} "
        f"skipped={skipped_calls} failed={failed_calls} rows_written={rows_written} "
        f"status={final_status}"
    )
