# Databricks notebook source
# MAGIC %md
# MAGIC # INSIGHTS_07_LLM_CONSOLIDATE_CALL_INSIGHTS

# COMMAND ----------

import json
import os
import re
from collections import defaultdict
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T

WORKFLOW_NAME = "insights"
STAGE_NAME = "insights_07_llm_consolidate_call_insights"
OUTPUT_TABLE_NAME = "gold_speech_insights"

ALLOWED_RUN_MODES = {"sample", "incremental", "full"}
ALLOWED_LLM_BACKENDS = {"auto", "databricks_endpoint", "http_service", "openai", "skip"}
ALLOWED_RAG_BACKENDS = {"auto", "vector_search", "delta_similarity", "skip", "none"}
ALLOWED_RESOLUTION = {"Resolved", "Not resolved"}
ALLOWED_EFFORT = {"High", "Low"}
ALLOWED_SENTIMENT = {"Positive", "Neutral", "Negative"}


def _is_dbx():
    return "dbutils" in globals()


def _lit(v):
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


def _truncate(msg, n=1000):
    return (str(msg).strip() or "Unknown error")[:n]


def _valid_ident(name, value):
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


def _to_float(name, value, lo=None, hi=None):
    try:
        out = float(value)
    except Exception as exc:
        raise ValueError(f"{name} must be numeric.") from exc
    if lo is not None and out < lo:
        raise ValueError(f"{name} must be >= {lo}.")
    if hi is not None and out > hi:
        raise ValueError(f"{name} must be <= {hi}.")
    return out


def _to_int(name, value, lo=None, hi=None):
    try:
        out = int(value)
    except Exception as exc:
        raise ValueError(f"{name} must be integer.") from exc
    if lo is not None and out < lo:
        raise ValueError(f"{name} must be >= {lo}.")
    if hi is not None and out > hi:
        raise ValueError(f"{name} must be <= {hi}.")
    return out


def _s(value):
    return str(value or "").strip()


def _arr_str(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [x.strip() for x in text.split(",") if x.strip()]


def _json_obj(text):
    raw = str(text or "").strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*```$", "", raw)
    i, j = raw.find("{"), raw.rfind("}")
    if i < 0 or j <= i:
        raise ValueError("LLM response missing JSON object.")
    payload = json.loads(raw[i : j + 1])
    if not isinstance(payload, dict):
        raise ValueError("LLM response must be a JSON object.")
    return payload


def _payload_text(payload):
    if payload is None:
        raise ValueError("Empty payload.")
    if isinstance(payload, str):
        return payload.strip()
    if isinstance(payload, dict):
        if "choices" in payload and payload["choices"]:
            c0 = payload["choices"][0]
            if isinstance(c0, dict):
                if isinstance(c0.get("message"), dict):
                    return str(c0["message"].get("content", "")).strip()
                return str(c0.get("text", "")).strip()
        for k in ("text", "output_text", "content", "response", "result", "outputs", "predictions"):
            if k in payload and payload[k] is not None:
                return _payload_text(payload[k])
        return json.dumps(payload, ensure_ascii=True)
    if isinstance(payload, list) and payload:
        return _payload_text(payload[0])
    return str(payload).strip()


def _fq(catalog, schema, table):
    return f"`{catalog}`.`{schema}`.`{table}`"


def _exists(catalog, schema, table):
    return bool(spark.catalog.tableExists(f"{catalog}.{schema}.{table}"))


def _ensure_tables(gold_table, ops_file, ops_runs):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
          call_id STRING,
          summary_text STRING,
          contact_driver_label STRING,
          contact_driver_confidence DOUBLE,
          issue_label STRING,
          issue_confidence DOUBLE,
          intent_label STRING,
          intent_confidence DOUBLE,
          resolution STRING,
          resolution_confidence DOUBLE,
          effort STRING,
          effort_confidence DOUBLE,
          sentiment STRING,
          sentiment_confidence DOUBLE,
          customer_emotion_start STRING,
          customer_emotion_end STRING,
          agent_emotion_start STRING,
          agent_emotion_end STRING,
          customer_emotion_start_score DOUBLE,
          customer_emotion_end_score DOUBLE,
          agent_emotion_start_score DOUBLE,
          agent_emotion_end_score DOUBLE,
          customer_emotion_shift_score DOUBLE,
          agent_emotion_shift_score DOUBLE,
          agent_love_score_1_10 INT,
          brand_love_score_1_10 INT,
          pii_possible_remaining_flag BOOLEAN,
          pii_notes STRING,
          recommended_next_action STRING,
          risk_flags ARRAY<STRING>,
          compliance_flags ARRAY<STRING>,
          taxonomy_version STRING,
          metrics_version STRING,
          consolidation_version STRING,
          insights_version STRING,
          llm_model_name STRING,
          llm_provider STRING,
          llm_backend STRING,
          llm_prompt_version STRING,
          rag_enabled_flag BOOLEAN,
          rag_used_flag BOOLEAN,
          rag_backend STRING,
          rag_index_name STRING,
          rag_top_k INT,
          rag_retrieved_chunk_ids ARRAY<STRING>,
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
        f"""
        MERGE INTO {ops_runs} AS t
        USING (
          SELECT
            {_lit(run_id)} AS run_id,
            {_lit(WORKFLOW_NAME)} AS workflow_name,
            current_timestamp() AS started_at,
            CAST(NULL AS TIMESTAMP) AS ended_at,
            'RUNNING' AS status,
            {_lit(params_json)} AS parameters,
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
          run_id, workflow_name, started_at, ended_at, status, trigger_type, parameters,
          error_summary, total_files, success_count, failed_count, updated_at
        ) VALUES (
          s.run_id, s.workflow_name, s.started_at, s.ended_at, s.status, NULL, s.parameters,
          s.error_summary, s.total_files, s.success_count, s.failed_count, s.updated_at
        )
        """
    )


def _upsert_run_final(ops_runs, run_id, status, total_files, success_count, failed_count, error_summary):
    spark.sql(
        f"""
        MERGE INTO {ops_runs} AS t
        USING (
          SELECT
            {_lit(run_id)} AS run_id,
            {_lit(WORKFLOW_NAME)} AS workflow_name,
            {_lit(status)} AS status,
            CAST({int(total_files)} AS BIGINT) AS total_files,
            CAST({int(success_count)} AS BIGINT) AS success_count,
            CAST({int(failed_count)} AS BIGINT) AS failed_count,
            {_lit(error_summary)} AS error_summary,
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
          run_id, workflow_name, started_at, ended_at, status, trigger_type, parameters,
          error_summary, total_files, success_count, failed_count, updated_at
        ) VALUES (
          s.run_id, s.workflow_name, current_timestamp(), s.ended_at, s.status, NULL, NULL,
          s.error_summary, s.total_files, s.success_count, s.failed_count, s.updated_at
        )
        """
    )


def _resolve_backend(raw_backend, endpoint_name, service_url):
    if raw_backend not in ALLOWED_LLM_BACKENDS:
        raise ValueError(f"Invalid llm_backend {raw_backend!r}")
    if raw_backend == "auto":
        if endpoint_name:
            return "databricks_endpoint", "auto->databricks_endpoint"
        if service_url:
            return "http_service", "auto->http_service"
        return "skip", "auto->skip (missing endpoint/service)"
    if raw_backend == "openai":
        return "http_service", "openai alias -> http_service"
    return raw_backend, None


def _build_llm_fn(backend, model_name, endpoint_name, service_url, service_key, temp, max_tokens, timeout_sec):
    if backend == "skip":
        return "skip", None, "LLM backend skip."
    if backend == "databricks_endpoint":
        if not endpoint_name:
            return None, None, "llm_endpoint_name is required."
        try:
            import mlflow.deployments  # type: ignore

            client = mlflow.deployments.get_deploy_client("databricks")

            def _call(prompt):
                payload = {
                    "messages": [
                        {"role": "system", "content": "Return exactly one JSON object."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": float(temp),
                    "max_tokens": int(max_tokens),
                    "model": model_name,
                }
                return _payload_text(client.predict(endpoint=endpoint_name, inputs=payload))

            return "databricks_endpoint", _call, None
        except Exception as exc:
            return None, None, _truncate(exc, 500)
    if backend == "http_service":
        if not service_url:
            return None, None, "llm_service_url is required."
        try:
            import requests  # type: ignore
        except Exception as exc:
            return None, None, _truncate(exc, 500)

        def _call(prompt):
            headers = {"Content-Type": "application/json"}
            if service_key:
                headers["Authorization"] = f"Bearer {service_key}"
            body = {
                "model": model_name,
                "messages": [
                    {"role": "system", "content": "Return exactly one JSON object."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": float(temp),
                "max_tokens": int(max_tokens),
            }
            resp = requests.post(service_url, headers=headers, json=body, timeout=int(timeout_sec))
            resp.raise_for_status()
            if "application/json" in (resp.headers.get("Content-Type") or "").lower():
                return _payload_text(resp.json())
            return str(resp.text or "").strip()

        return "http_service", _call, None
    return None, None, f"Unsupported backend {backend!r}"


def _resolve_taxonomy_version(catalog, schema, requested):
    req = _s(requested)
    if req:
        return req
    rows = (
        spark.table(_fq(catalog, schema, "dim_contact_driver"))
        .select(F.col("taxonomy_version").cast("string").alias("taxonomy_version"))
        .where("taxonomy_version IS NOT NULL")
        .orderBy(F.col("taxonomy_version").desc())
        .limit(1)
        .collect()
    )
    if not rows:
        raise RuntimeError("Unable to resolve taxonomy_version from dim_contact_driver.")
    return _s(rows[0]["taxonomy_version"])


def _taxonomy_sets(catalog, schema, taxonomy_version):
    drivers = {
        _s(r["label"])
        for r in spark.table(_fq(catalog, schema, "dim_contact_driver"))
        .where((F.col("taxonomy_version") == taxonomy_version) & (F.col("active_flag") == F.lit(True)))
        .select(F.col("label").cast("string").alias("label"))
        .collect()
        if _s(r["label"])
    }
    issues = {
        _s(r["label"])
        for r in spark.table(_fq(catalog, schema, "dim_issue"))
        .where((F.col("taxonomy_version") == taxonomy_version) & (F.col("active_flag") == F.lit(True)))
        .select(F.col("label").cast("string").alias("label"))
        .collect()
        if _s(r["label"])
    }
    intents = {
        _s(r["label"])
        for r in spark.table(_fq(catalog, schema, "dim_intent"))
        .where((F.col("taxonomy_version") == taxonomy_version) & (F.col("active_flag") == F.lit(True)))
        .select(F.col("label").cast("string").alias("label"))
        .collect()
        if _s(r["label"])
    }
    emos = set()
    emo_polarity = {}
    for r in (
        spark.table(_fq(catalog, schema, "dim_emotion_catalog"))
        .where(
            ((F.col("taxonomy_version") == taxonomy_version) | (F.col("catalog_version") == taxonomy_version))
            & (F.col("active_flag") == F.lit(True))
        )
        .select(
            F.col("emotion_name").cast("string").alias("emotion_name"),
            F.col("polarity_score").cast("double").alias("polarity_score"),
        )
        .collect()
    ):
        e = _s(r["emotion_name"])
        if e:
            emos.add(e)
            if r["polarity_score"] is not None:
                emo_polarity[e] = float(r["polarity_score"])
    allowed = {"driver": drivers, "issue": issues, "intent": intents, "emotion": emos}
    for k, vals in allowed.items():
        if not vals:
            raise RuntimeError(f"No active taxonomy values for {k} and taxonomy_version={taxonomy_version!r}.")
    return allowed, emo_polarity


def _top(rows, label_key, conf_key):
    stats = defaultdict(lambda: {"n": 0.0, "c": 0.0})
    for r in rows:
        lbl = _s(r.get(label_key))
        if not lbl:
            continue
        stats[lbl]["n"] += 1.0
        stats[lbl]["c"] += float(r.get(conf_key) or 0.0)
    if not stats:
        return {"label": "", "count": 0, "avg_conf": 0.0}
    ordered = sorted(stats.items(), key=lambda kv: (-kv[1]["n"], -(kv[1]["c"] / max(kv[1]["n"], 1.0)), kv[0]))
    label, agg = ordered[0]
    return {"label": label, "count": int(agg["n"]), "avg_conf": float(agg["c"] / max(agg["n"], 1.0))}


def _counts(rows, key):
    out = defaultdict(int)
    for r in rows:
        token = _s(r.get(key))
        if token:
            out[token] += 1
    return dict(out)


def _validate_chunk_labels(rows, allowed):
    for r in rows:
        d, i, t = _s(r.get("candidate_driver_label")), _s(r.get("candidate_issue_label")), _s(r.get("candidate_intent_label"))
        if d and d not in allowed["driver"]:
            raise ValueError(f"Chunk driver outside taxonomy: {d!r}")
        if i and i not in allowed["issue"]:
            raise ValueError(f"Chunk issue outside taxonomy: {i!r}")
        if t and t not in allowed["intent"]:
            raise ValueError(f"Chunk intent outside taxonomy: {t!r}")


def _build_prompt(evidence, allowed, taxonomy_version, metrics_version, llm_prompt_version):
    payload = {
        "instruction": "Return exactly one JSON object only. No markdown.",
        "constraints": {
            "taxonomy_version": taxonomy_version,
            "metrics_version": metrics_version,
            "llm_prompt_version": llm_prompt_version,
            "no_raw_transcript_text": True,
            "allowed_contact_driver_labels": sorted(allowed["driver"]),
            "allowed_issue_labels": sorted(allowed["issue"]),
            "allowed_intent_labels": sorted(allowed["intent"]),
            "allowed_emotion_labels": sorted(allowed["emotion"]),
            "allowed_resolution": sorted(ALLOWED_RESOLUTION),
            "allowed_effort": sorted(ALLOWED_EFFORT),
            "allowed_sentiment": sorted(ALLOWED_SENTIMENT),
        },
        "required_schema": {
            "summary_text": "string",
            "contact_driver_label": "string",
            "contact_driver_confidence": "float in [0,1]",
            "issue_label": "string",
            "issue_confidence": "float in [0,1]",
            "intent_label": "string",
            "intent_confidence": "float in [0,1]",
            "resolution": "Resolved | Not resolved",
            "resolution_confidence": "float in [0,1]",
            "effort": "High | Low",
            "effort_confidence": "float in [0,1]",
            "sentiment": "Positive | Neutral | Negative",
            "sentiment_confidence": "float in [0,1]",
            "customer_emotion_start": "allowed emotion label",
            "customer_emotion_end": "allowed emotion label",
            "agent_emotion_start": "allowed emotion label",
            "agent_emotion_end": "allowed emotion label",
            "agent_love_score_1_10": "int 1..10",
            "brand_love_score_1_10": "int 1..10",
            "pii_possible_remaining_flag": "boolean",
            "pii_notes": "string",
            "recommended_next_action": "string",
            "risk_flags": "array<string>",
            "compliance_flags": "array<string>",
        },
        "evidence_pack": evidence,
    }
    return json.dumps(payload, ensure_ascii=True, sort_keys=True)


def _validate_llm_output(payload, allowed):
    out = {
        "summary_text": _s(payload.get("summary_text") or payload.get("overall_summary")),
        "contact_driver_label": _s(payload.get("contact_driver_label") or payload.get("final_contact_driver_label")),
        "issue_label": _s(payload.get("issue_label") or payload.get("final_issue_label")),
        "intent_label": _s(payload.get("intent_label") or payload.get("final_intent_label")),
        "contact_driver_confidence": _to_float("contact_driver_confidence", payload.get("contact_driver_confidence"), 0.0, 1.0),
        "issue_confidence": _to_float("issue_confidence", payload.get("issue_confidence"), 0.0, 1.0),
        "intent_confidence": _to_float("intent_confidence", payload.get("intent_confidence"), 0.0, 1.0),
        "resolution": _s(payload.get("resolution")),
        "resolution_confidence": _to_float("resolution_confidence", payload.get("resolution_confidence"), 0.0, 1.0),
        "effort": _s(payload.get("effort")),
        "effort_confidence": _to_float("effort_confidence", payload.get("effort_confidence"), 0.0, 1.0),
        "sentiment": _s(payload.get("sentiment") or payload.get("sentiment_overall")),
        "sentiment_confidence": _to_float("sentiment_confidence", payload.get("sentiment_confidence"), 0.0, 1.0),
        "customer_emotion_start": _s(payload.get("customer_emotion_start")),
        "customer_emotion_end": _s(payload.get("customer_emotion_end") or payload.get("customer_emotion_overall")),
        "agent_emotion_start": _s(payload.get("agent_emotion_start")),
        "agent_emotion_end": _s(payload.get("agent_emotion_end") or payload.get("agent_emotion_overall")),
        "agent_love_score_1_10": _to_int("agent_love_score_1_10", payload.get("agent_love_score_1_10"), 1, 10),
        "brand_love_score_1_10": _to_int("brand_love_score_1_10", payload.get("brand_love_score_1_10"), 1, 10),
        "pii_possible_remaining_flag": _parse_bool("pii_possible_remaining_flag", str(payload.get("pii_possible_remaining_flag") or "false"), False),
        "pii_notes": _s(payload.get("pii_notes")),
        "recommended_next_action": _s(payload.get("recommended_next_action")),
        "risk_flags": _arr_str(payload.get("risk_flags")),
        "compliance_flags": _arr_str(payload.get("compliance_flags")),
    }
    if not out["summary_text"]:
        raise ValueError("summary_text is required.")
    if not out["recommended_next_action"]:
        raise ValueError("recommended_next_action is required.")
    if out["contact_driver_label"] not in allowed["driver"]:
        raise ValueError(f"Invalid contact_driver_label {out['contact_driver_label']!r}")
    if out["issue_label"] not in allowed["issue"]:
        raise ValueError(f"Invalid issue_label {out['issue_label']!r}")
    if out["intent_label"] not in allowed["intent"]:
        raise ValueError(f"Invalid intent_label {out['intent_label']!r}")
    if out["resolution"] not in ALLOWED_RESOLUTION:
        raise ValueError(f"Invalid resolution {out['resolution']!r}")
    if out["effort"] not in ALLOWED_EFFORT:
        raise ValueError(f"Invalid effort {out['effort']!r}")
    if out["sentiment"] not in ALLOWED_SENTIMENT:
        raise ValueError(f"Invalid sentiment {out['sentiment']!r}")
    for k in ("customer_emotion_start", "customer_emotion_end", "agent_emotion_start", "agent_emotion_end"):
        if out[k] not in allowed["emotion"]:
            raise ValueError(f"Invalid {k} {out[k]!r}")
    return out


if _is_dbx():
    dbutils.widgets.text("catalog", "")
    dbutils.widgets.text("schema", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("run_mode", "incremental")
    dbutils.widgets.text("max_files_per_run", "10")
    dbutils.widgets.text("enable_llm_consolidation", "true")
    dbutils.widgets.text("enable_llm", "")
    dbutils.widgets.text("enable_llm_insights", "")
    dbutils.widgets.text("llm_backend", "auto")
    dbutils.widgets.text("llm_model_name", "")
    dbutils.widgets.text("llm_temperature", "0.2")
    dbutils.widgets.text("llm_max_tokens", "800")
    dbutils.widgets.text("taxonomy_version", "")
    dbutils.widgets.text("metrics_version", "v1")
    dbutils.widgets.text("consolidation_version", "v1")
    dbutils.widgets.text("insights_version", "")
    dbutils.widgets.text("prompt_version", "v1")
    dbutils.widgets.text("llm_prompt_version", "")
    dbutils.widgets.text("rag_backend", "auto")
    dbutils.widgets.text("rag_index_name", "")
    dbutils.widgets.text("rag_top_k", "8")
    dbutils.widgets.text("llm_endpoint_name", "")
    dbutils.widgets.text("llm_service_url", "")
    dbutils.widgets.text("llm_service_api_key", "")
    dbutils.widgets.text("llm_timeout_sec", "60")

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    run_id = dbutils.widgets.get("run_id").strip()
    run_mode = dbutils.widgets.get("run_mode").strip().lower()
    max_files_per_run_raw = dbutils.widgets.get("max_files_per_run").strip()
    enable_llm_raw = (
        dbutils.widgets.get("enable_llm_consolidation").strip()
        or dbutils.widgets.get("enable_llm").strip()
        or dbutils.widgets.get("enable_llm_insights").strip()
        or "true"
    )
    llm_backend = dbutils.widgets.get("llm_backend").strip().lower()
    llm_model_name = dbutils.widgets.get("llm_model_name").strip()
    llm_temperature = float(dbutils.widgets.get("llm_temperature").strip() or "0.2")
    llm_max_tokens = int(dbutils.widgets.get("llm_max_tokens").strip() or "800")
    taxonomy_version = dbutils.widgets.get("taxonomy_version").strip()
    metrics_version = dbutils.widgets.get("metrics_version").strip() or "v1"
    consolidation_version = (
        dbutils.widgets.get("consolidation_version").strip()
        or dbutils.widgets.get("insights_version").strip()
        or "v1"
    )
    llm_prompt_version = (
        dbutils.widgets.get("llm_prompt_version").strip()
        or dbutils.widgets.get("prompt_version").strip()
        or "v1"
    )
    rag_backend = dbutils.widgets.get("rag_backend").strip().lower() or "auto"
    rag_index_name = dbutils.widgets.get("rag_index_name").strip()
    rag_top_k = int(dbutils.widgets.get("rag_top_k").strip() or "8")
    llm_endpoint_name = dbutils.widgets.get("llm_endpoint_name").strip()
    llm_service_url = dbutils.widgets.get("llm_service_url").strip()
    llm_service_api_key = dbutils.widgets.get("llm_service_api_key").strip()
    llm_timeout_sec = int(dbutils.widgets.get("llm_timeout_sec").strip() or "60")
else:
    catalog = os.getenv("CATALOG", "").strip()
    schema = os.getenv("SCHEMA", "").strip()
    run_id = os.getenv("RUN_ID", "").strip()
    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()
    max_files_per_run_raw = os.getenv("MAX_FILES_PER_RUN", "10").strip()
    enable_llm_raw = (
        os.getenv("ENABLE_LLM_CONSOLIDATION", "").strip()
        or os.getenv("ENABLE_LLM", "").strip()
        or os.getenv("ENABLE_LLM_INSIGHTS", "").strip()
        or "true"
    )
    llm_backend = os.getenv("LLM_BACKEND", "auto").strip().lower()
    llm_model_name = os.getenv("LLM_MODEL_NAME", "").strip()
    llm_temperature = float(os.getenv("LLM_TEMPERATURE", "0.2").strip())
    llm_max_tokens = int(os.getenv("LLM_MAX_TOKENS", "800").strip())
    taxonomy_version = os.getenv("TAXONOMY_VERSION", "").strip()
    metrics_version = os.getenv("METRICS_VERSION", "v1").strip() or "v1"
    consolidation_version = (
        os.getenv("CONSOLIDATION_VERSION", "").strip()
        or os.getenv("INSIGHTS_VERSION", "").strip()
        or "v1"
    )
    llm_prompt_version = (
        os.getenv("LLM_PROMPT_VERSION", "").strip()
        or os.getenv("PROMPT_VERSION", "v1").strip()
        or "v1"
    )
    rag_backend = os.getenv("RAG_BACKEND", "auto").strip().lower() or "auto"
    rag_index_name = os.getenv("RAG_INDEX_NAME", "").strip()
    rag_top_k = int(os.getenv("RAG_TOP_K", "8").strip() or "8")
    llm_endpoint_name = os.getenv("LLM_ENDPOINT_NAME", "").strip()
    llm_service_url = os.getenv("LLM_SERVICE_URL", "").strip()
    llm_service_api_key = os.getenv("LLM_SERVICE_API_KEY", "").strip()
    llm_timeout_sec = int(os.getenv("LLM_TIMEOUT_SEC", "60").strip() or "60")

catalog = _valid_ident("catalog", catalog)
schema = _valid_ident("schema", schema)
if not run_id:
    raise ValueError("Parameter `run_id` is required.")
if run_mode not in ALLOWED_RUN_MODES:
    raise ValueError(f"Invalid `run_mode`: {run_mode!r}")
if llm_backend not in ALLOWED_LLM_BACKENDS:
    raise ValueError(f"Invalid `llm_backend`: {llm_backend!r}")
if rag_backend not in ALLOWED_RAG_BACKENDS:
    raise ValueError(f"Invalid `rag_backend`: {rag_backend!r}")
if llm_max_tokens <= 0:
    raise ValueError("`llm_max_tokens` must be > 0.")
if llm_timeout_sec <= 0:
    raise ValueError("`llm_timeout_sec` must be > 0.")
if rag_top_k <= 0:
    raise ValueError("`rag_top_k` must be > 0.")
if not consolidation_version:
    raise ValueError("`consolidation_version` must not be empty.")
if not metrics_version:
    raise ValueError("`metrics_version` must not be empty.")
if not llm_prompt_version:
    raise ValueError("`llm_prompt_version` must not be empty.")

enable_llm_consolidation = _parse_bool("enable_llm_consolidation", enable_llm_raw, default=True)
if enable_llm_consolidation and not llm_model_name:
    raise ValueError("`llm_model_name` is required when enable_llm_consolidation=true.")

max_files_per_run = int(max_files_per_run_raw) if max_files_per_run_raw else None
if run_mode == "sample" and (max_files_per_run is None or max_files_per_run <= 0):
    raise ValueError("In sample mode, `max_files_per_run` must be > 0.")

chunks_table = _fq(catalog, schema, "silver_llm_chunk_insights")
metrics_table = _fq(catalog, schema, "gold_conversation_metrics")
gold_table = _fq(catalog, schema, OUTPUT_TABLE_NAME)
ops_file = _fq(catalog, schema, "ops_file_status")
ops_runs = _fq(catalog, schema, "ops_pipeline_runs")

params_json = json.dumps(
    {
        "catalog": catalog,
        "schema": schema,
        "run_id": run_id,
        "run_mode": run_mode,
        "max_files_per_run": max_files_per_run,
        "enable_llm_consolidation": enable_llm_consolidation,
        "llm_backend": llm_backend,
        "llm_model_name": llm_model_name or None,
        "taxonomy_version": taxonomy_version or None,
        "metrics_version": metrics_version,
        "consolidation_version": consolidation_version,
        "llm_prompt_version": llm_prompt_version,
        "rag_backend": rag_backend,
        "rag_index_name": rag_index_name or None,
        "rag_top_k": rag_top_k,
    },
    sort_keys=True,
)

print(
    f"[{STAGE_NAME}] start run_id={run_id} mode={run_mode} "
    f"enable_llm={enable_llm_consolidation} metrics_version={metrics_version} "
    f"consolidation_version={consolidation_version}"
)

_ensure_tables(gold_table, ops_file, ops_runs)
_upsert_run_running(ops_runs, run_id, params_json)

eligible_count = 0
success_calls = 0
failed_calls = 0
skipped_calls = 0
rows_written = 0
final_status = "SUCCESS"
error_summary = None

try:
    required_tables = [
        "silver_llm_chunk_insights",
        "gold_conversation_metrics",
        "dim_contact_driver",
        "dim_issue",
        "dim_intent",
        "dim_emotion_catalog",
    ]
    missing_tbls = [t for t in required_tables if not _exists(catalog, schema, t)]
    if missing_tbls:
        raise RuntimeError(
            "Missing required table(s): " + ", ".join([f"{catalog}.{schema}.{t}" for t in missing_tbls])
        )

    chunks_df = spark.table(chunks_table)
    metrics_df = spark.table(metrics_table)

    missing_chunk_cols = sorted(
        {
            "call_id",
            "chunk_id",
            "chunk_text_hash",
            "candidate_driver_label",
            "candidate_driver_confidence",
            "candidate_issue_label",
            "candidate_issue_confidence",
            "candidate_intent_label",
            "candidate_intent_confidence",
            "pii_possible_remaining_flag",
            "pii_notes",
            "chunk_summary",
            "sentiment_signal",
            "sentiment_confidence",
            "customer_emotion_signal",
            "agent_emotion_signal",
            "taxonomy_version",
        }
        - set(chunks_df.columns)
    )
    if missing_chunk_cols:
        raise RuntimeError(f"{chunks_table} missing columns: {missing_chunk_cols}")

    missing_metrics_cols = sorted(
        {"call_id", "metrics_version", "total_duration_sec", "agent_talk_time_sec", "customer_talk_time_sec"}
        - set(metrics_df.columns)
    )
    if missing_metrics_cols:
        raise RuntimeError(f"{metrics_table} missing columns: {missing_metrics_cols}")

    taxonomy_version = _resolve_taxonomy_version(catalog, schema, taxonomy_version)
    allowed, emo_polarity = _taxonomy_sets(catalog, schema, taxonomy_version)

    llm_backend_resolved, llm_backend_note = _resolve_backend(llm_backend, llm_endpoint_name, llm_service_url)
    llm_provider, llm_fn, llm_build_error = _build_llm_fn(
        llm_backend_resolved,
        llm_model_name,
        llm_endpoint_name,
        llm_service_url,
        llm_service_api_key,
        llm_temperature,
        llm_max_tokens,
        llm_timeout_sec,
    )

    stage_df = (
        spark.table(ops_file)
        .where(F.col("stage_name") == STAGE_NAME)
        .select(F.col("call_id").cast("string").alias("call_id"), F.upper(F.col("status")).alias("stage_status"))
    )

    calls_df = (
        chunks_df.select(
            F.col("call_id").cast("string").alias("call_id"),
            F.col("taxonomy_version").cast("string").alias("taxonomy_version"),
        )
        .where("call_id IS NOT NULL")
    )
    if taxonomy_version:
        calls_df = calls_df.where(
            F.col("taxonomy_version").isNull() | (F.col("taxonomy_version") == taxonomy_version)
        )
    calls_df = calls_df.select("call_id").distinct().orderBy("call_id")

    eligible_df = calls_df.join(stage_df, on="call_id", how="left")
    if run_mode in {"sample", "incremental"}:
        eligible_df = eligible_df.where(F.col("stage_status").isNull() | (F.col("stage_status") == "FAILED"))
    if run_mode == "sample" and max_files_per_run is not None:
        eligible_df = eligible_df.limit(max_files_per_run)

    eligible_calls = [str(r["call_id"]) for r in eligible_df.select("call_id").collect()]
    eligible_count = len(eligible_calls)
    print(f"[{STAGE_NAME}] eligible_calls={eligible_count}")

    metric_rows = (
        metrics_df.where(F.col("metrics_version") == metrics_version)
        .select(
            F.col("call_id").cast("string").alias("call_id"),
            F.col("total_duration_sec").cast("double").alias("total_duration_sec"),
            F.col("agent_talk_time_sec").cast("double").alias("agent_talk_time_sec"),
            F.col("customer_talk_time_sec").cast("double").alias("customer_talk_time_sec"),
            (F.col("unknown_talk_time_sec").cast("double") if "unknown_talk_time_sec" in metrics_df.columns else F.lit(0.0)).alias("unknown_talk_time_sec"),
            (F.col("silence_time_sec").cast("double") if "silence_time_sec" in metrics_df.columns else F.lit(0.0)).alias("silence_time_sec"),
            (F.col("overlap_time_sec").cast("double") if "overlap_time_sec" in metrics_df.columns else F.lit(0.0)).alias("overlap_time_sec"),
            (F.col("turn_count_total").cast("bigint") if "turn_count_total" in metrics_df.columns else F.lit(0)).alias("turn_count_total"),
            (F.col("turn_count_agent").cast("bigint") if "turn_count_agent" in metrics_df.columns else F.lit(0)).alias("turn_count_agent"),
            (F.col("turn_count_customer").cast("bigint") if "turn_count_customer" in metrics_df.columns else F.lit(0)).alias("turn_count_customer"),
            (F.col("turn_count_unknown").cast("bigint") if "turn_count_unknown" in metrics_df.columns else F.lit(0)).alias("turn_count_unknown"),
            (F.col("avg_turn_length_sec").cast("double") if "avg_turn_length_sec" in metrics_df.columns else F.lit(0.0)).alias("avg_turn_length_sec"),
            (F.col("first_turn_ts_sec").cast("double") if "first_turn_ts_sec" in metrics_df.columns else F.lit(0.0)).alias("first_turn_ts_sec"),
            (F.col("last_turn_ts_sec").cast("double") if "last_turn_ts_sec" in metrics_df.columns else F.lit(0.0)).alias("last_turn_ts_sec"),
            (F.col("overlap_warning_flag").cast("boolean") if "overlap_warning_flag" in metrics_df.columns else F.lit(False)).alias("overlap_warning_flag"),
        )
        .collect()
    )
    metrics_by_call = {str(r["call_id"]): r.asDict(recursive=True) for r in metric_rows}

    chunk_rows_df = (
        chunks_df.select(
            F.col("call_id").cast("string").alias("call_id"),
            F.col("chunk_id").cast("string").alias("chunk_id"),
            F.col("chunk_text_hash").cast("string").alias("chunk_text_hash"),
            F.col("taxonomy_version").cast("string").alias("taxonomy_version"),
            F.col("candidate_driver_label").cast("string").alias("candidate_driver_label"),
            F.col("candidate_driver_confidence").cast("double").alias("candidate_driver_confidence"),
            F.col("candidate_issue_label").cast("string").alias("candidate_issue_label"),
            F.col("candidate_issue_confidence").cast("double").alias("candidate_issue_confidence"),
            F.col("candidate_intent_label").cast("string").alias("candidate_intent_label"),
            F.col("candidate_intent_confidence").cast("double").alias("candidate_intent_confidence"),
            F.col("pii_possible_remaining_flag").cast("boolean").alias("pii_possible_remaining_flag"),
            F.col("pii_notes").cast("string").alias("pii_notes"),
            F.col("chunk_summary").cast("string").alias("chunk_summary"),
            F.col("sentiment_signal").cast("string").alias("sentiment_signal"),
            F.col("sentiment_confidence").cast("double").alias("sentiment_confidence"),
            F.col("customer_emotion_signal").cast("string").alias("customer_emotion_signal"),
            F.col("agent_emotion_signal").cast("string").alias("agent_emotion_signal"),
            (F.col("rag_used_flag").cast("boolean") if "rag_used_flag" in chunks_df.columns else F.lit(False)).alias("rag_used_flag"),
            (
                F.col("rag_retrieved_chunk_ids").cast(T.ArrayType(T.StringType()))
                if "rag_retrieved_chunk_ids" in chunks_df.columns
                else F.array().cast(T.ArrayType(T.StringType()))
            ).alias("rag_retrieved_chunk_ids"),
        )
        .where("call_id IS NOT NULL")
    )
    if taxonomy_version:
        chunk_rows_df = chunk_rows_df.where(
            F.col("taxonomy_version").isNull() | (F.col("taxonomy_version") == taxonomy_version)
        )

    chunks_by_call = defaultdict(list)
    for r in chunk_rows_df.collect():
        chunks_by_call[str(r["call_id"])].append(r.asDict(recursive=True))

    status_rows = []
    out_rows = []
    ts = datetime.utcnow()

    for call_id in eligible_calls:
        try:
            call_chunks = chunks_by_call.get(call_id, [])
            metric = metrics_by_call.get(call_id)
            if not call_chunks:
                raise RuntimeError("Eligibility gate failed: no chunk rows for call_id.")
            if metric is None:
                raise RuntimeError(
                    f"Eligibility gate failed: no metrics row for metrics_version={metrics_version!r}."
                )

            _validate_chunk_labels(call_chunks, allowed)

            if not enable_llm_consolidation:
                skipped_calls += 1
                status_rows.append(
                    {
                        "call_id": call_id,
                        "stage_name": STAGE_NAME,
                        "status": "SKIPPED",
                        "error_message": "LLM consolidation disabled.",
                        "run_id": run_id,
                        "updated_at": ts,
                    }
                )
                continue

            if llm_backend_resolved == "skip":
                raise RuntimeError(llm_backend_note or "LLM backend skip.")
            if llm_fn is None:
                raise RuntimeError(llm_build_error or "LLM backend unavailable.")

            pii_any = any(bool(x.get("pii_possible_remaining_flag")) for x in call_chunks)
            pii_notes = sorted({_s(x.get("pii_notes")) for x in call_chunks if _s(x.get("pii_notes"))})
            rag_ids = sorted(
                {
                    _s(cid)
                    for x in call_chunks
                    for cid in (x.get("rag_retrieved_chunk_ids") or [])
                    if _s(cid)
                }
            )
            evidence = {
                "call_id": call_id,
                "chunk_count": len(call_chunks),
                "top_driver": _top(call_chunks, "candidate_driver_label", "candidate_driver_confidence"),
                "top_issue": _top(call_chunks, "candidate_issue_label", "candidate_issue_confidence"),
                "top_intent": _top(call_chunks, "candidate_intent_label", "candidate_intent_confidence"),
                "sentiment_counts": _counts(call_chunks, "sentiment_signal"),
                "customer_emotion_counts": _counts(call_chunks, "customer_emotion_signal"),
                "agent_emotion_counts": _counts(call_chunks, "agent_emotion_signal"),
                "pii_possible_remaining_flag_any": pii_any,
                "pii_notes_concat": "; ".join(pii_notes[:8])[:800],
                "chunk_summaries": [_s(x.get("chunk_summary")) for x in call_chunks if _s(x.get("chunk_summary"))][:20],
                "rag_used_any": any(bool(x.get("rag_used_flag")) for x in call_chunks),
                "rag_retrieved_chunk_ids": rag_ids,
                "metrics": {
                    "total_duration_sec": float(metric.get("total_duration_sec") or 0.0),
                    "agent_talk_time_sec": float(metric.get("agent_talk_time_sec") or 0.0),
                    "customer_talk_time_sec": float(metric.get("customer_talk_time_sec") or 0.0),
                    "unknown_talk_time_sec": float(metric.get("unknown_talk_time_sec") or 0.0),
                    "silence_time_sec": float(metric.get("silence_time_sec") or 0.0),
                    "overlap_time_sec": float(metric.get("overlap_time_sec") or 0.0),
                    "turn_count_total": int(metric.get("turn_count_total") or 0),
                    "turn_count_agent": int(metric.get("turn_count_agent") or 0),
                    "turn_count_customer": int(metric.get("turn_count_customer") or 0),
                    "turn_count_unknown": int(metric.get("turn_count_unknown") or 0),
                    "avg_turn_length_sec": float(metric.get("avg_turn_length_sec") or 0.0),
                    "first_turn_ts_sec": float(metric.get("first_turn_ts_sec") or 0.0),
                    "last_turn_ts_sec": float(metric.get("last_turn_ts_sec") or 0.0),
                    "overlap_warning_flag": bool(metric.get("overlap_warning_flag")),
                },
            }

            prompt = _build_prompt(evidence, allowed, taxonomy_version, metrics_version, llm_prompt_version)
            llm_payload = _json_obj(llm_fn(prompt))
            parsed = _validate_llm_output(llm_payload, allowed)

            c_start = float(emo_polarity.get(parsed["customer_emotion_start"], 0.0))
            c_end = float(emo_polarity.get(parsed["customer_emotion_end"], 0.0))
            a_start = float(emo_polarity.get(parsed["agent_emotion_start"], 0.0))
            a_end = float(emo_polarity.get(parsed["agent_emotion_end"], 0.0))
            rag_used_flag = bool(evidence["rag_used_any"] or len(rag_ids) > 0)
            rag_backend_eff = rag_backend if rag_backend != "auto" else ("vector_search" if rag_index_name and rag_used_flag else "none")

            out_rows.append(
                {
                    "call_id": call_id,
                    "summary_text": parsed["summary_text"],
                    "contact_driver_label": parsed["contact_driver_label"],
                    "contact_driver_confidence": float(parsed["contact_driver_confidence"]),
                    "issue_label": parsed["issue_label"],
                    "issue_confidence": float(parsed["issue_confidence"]),
                    "intent_label": parsed["intent_label"],
                    "intent_confidence": float(parsed["intent_confidence"]),
                    "resolution": parsed["resolution"],
                    "resolution_confidence": float(parsed["resolution_confidence"]),
                    "effort": parsed["effort"],
                    "effort_confidence": float(parsed["effort_confidence"]),
                    "sentiment": parsed["sentiment"],
                    "sentiment_confidence": float(parsed["sentiment_confidence"]),
                    "customer_emotion_start": parsed["customer_emotion_start"],
                    "customer_emotion_end": parsed["customer_emotion_end"],
                    "agent_emotion_start": parsed["agent_emotion_start"],
                    "agent_emotion_end": parsed["agent_emotion_end"],
                    "customer_emotion_start_score": c_start,
                    "customer_emotion_end_score": c_end,
                    "agent_emotion_start_score": a_start,
                    "agent_emotion_end_score": a_end,
                    "customer_emotion_shift_score": c_end - c_start,
                    "agent_emotion_shift_score": a_end - a_start,
                    "agent_love_score_1_10": int(parsed["agent_love_score_1_10"]),
                    "brand_love_score_1_10": int(parsed["brand_love_score_1_10"]),
                    "pii_possible_remaining_flag": bool(parsed["pii_possible_remaining_flag"] or pii_any),
                    "pii_notes": (parsed["pii_notes"] or evidence["pii_notes_concat"])[:800],
                    "recommended_next_action": parsed["recommended_next_action"],
                    "risk_flags": parsed["risk_flags"],
                    "compliance_flags": parsed["compliance_flags"],
                    "taxonomy_version": taxonomy_version,
                    "metrics_version": metrics_version,
                    "consolidation_version": consolidation_version,
                    "insights_version": consolidation_version,
                    "llm_model_name": llm_model_name,
                    "llm_provider": llm_provider or llm_backend_resolved,
                    "llm_backend": llm_backend_resolved,
                    "llm_prompt_version": llm_prompt_version,
                    "rag_enabled_flag": rag_backend not in {"skip", "none"},
                    "rag_used_flag": rag_used_flag,
                    "rag_backend": rag_backend_eff,
                    "rag_index_name": rag_index_name or None,
                    "rag_top_k": int(rag_top_k),
                    "rag_retrieved_chunk_ids": rag_ids[: max(1, int(rag_top_k))],
                    "run_id": run_id,
                    "updated_at": ts,
                }
            )
            success_calls += 1
            status_rows.append(
                {
                    "call_id": call_id,
                    "stage_name": STAGE_NAME,
                    "status": "SUCCESS",
                    "error_message": None,
                    "run_id": run_id,
                    "updated_at": ts,
                }
            )
        except Exception as exc:
            failed_calls += 1
            status_rows.append(
                {
                    "call_id": call_id,
                    "stage_name": STAGE_NAME,
                    "status": "FAILED",
                    "error_message": _truncate(exc),
                    "run_id": run_id,
                    "updated_at": ts,
                }
            )

    if out_rows:
        out_schema = T.StructType(
            [
                T.StructField("call_id", T.StringType(), False),
                T.StructField("summary_text", T.StringType(), False),
                T.StructField("contact_driver_label", T.StringType(), False),
                T.StructField("contact_driver_confidence", T.DoubleType(), False),
                T.StructField("issue_label", T.StringType(), False),
                T.StructField("issue_confidence", T.DoubleType(), False),
                T.StructField("intent_label", T.StringType(), False),
                T.StructField("intent_confidence", T.DoubleType(), False),
                T.StructField("resolution", T.StringType(), False),
                T.StructField("resolution_confidence", T.DoubleType(), False),
                T.StructField("effort", T.StringType(), False),
                T.StructField("effort_confidence", T.DoubleType(), False),
                T.StructField("sentiment", T.StringType(), False),
                T.StructField("sentiment_confidence", T.DoubleType(), False),
                T.StructField("customer_emotion_start", T.StringType(), False),
                T.StructField("customer_emotion_end", T.StringType(), False),
                T.StructField("agent_emotion_start", T.StringType(), False),
                T.StructField("agent_emotion_end", T.StringType(), False),
                T.StructField("customer_emotion_start_score", T.DoubleType(), False),
                T.StructField("customer_emotion_end_score", T.DoubleType(), False),
                T.StructField("agent_emotion_start_score", T.DoubleType(), False),
                T.StructField("agent_emotion_end_score", T.DoubleType(), False),
                T.StructField("customer_emotion_shift_score", T.DoubleType(), False),
                T.StructField("agent_emotion_shift_score", T.DoubleType(), False),
                T.StructField("agent_love_score_1_10", T.IntegerType(), False),
                T.StructField("brand_love_score_1_10", T.IntegerType(), False),
                T.StructField("pii_possible_remaining_flag", T.BooleanType(), False),
                T.StructField("pii_notes", T.StringType(), True),
                T.StructField("recommended_next_action", T.StringType(), False),
                T.StructField("risk_flags", T.ArrayType(T.StringType()), False),
                T.StructField("compliance_flags", T.ArrayType(T.StringType()), False),
                T.StructField("taxonomy_version", T.StringType(), False),
                T.StructField("metrics_version", T.StringType(), False),
                T.StructField("consolidation_version", T.StringType(), False),
                T.StructField("insights_version", T.StringType(), False),
                T.StructField("llm_model_name", T.StringType(), False),
                T.StructField("llm_provider", T.StringType(), False),
                T.StructField("llm_backend", T.StringType(), False),
                T.StructField("llm_prompt_version", T.StringType(), False),
                T.StructField("rag_enabled_flag", T.BooleanType(), False),
                T.StructField("rag_used_flag", T.BooleanType(), False),
                T.StructField("rag_backend", T.StringType(), False),
                T.StructField("rag_index_name", T.StringType(), True),
                T.StructField("rag_top_k", T.IntegerType(), False),
                T.StructField("rag_retrieved_chunk_ids", T.ArrayType(T.StringType()), False),
                T.StructField("run_id", T.StringType(), False),
                T.StructField("updated_at", T.TimestampType(), False),
            ]
        )
        out_df = spark.createDataFrame(out_rows, schema=out_schema)

        keys_df = (
            out_df.select("call_id")
            .dropDuplicates()
            .withColumn("metrics_version", F.lit(metrics_version))
            .withColumn("consolidation_version", F.lit(consolidation_version))
        )
        keys_df.createOrReplaceTempView("tmp_insights_07_keys")
        spark.sql(
            f"""
            MERGE INTO {gold_table} AS t
            USING tmp_insights_07_keys AS s
            ON t.call_id = s.call_id
               AND t.metrics_version = s.metrics_version
               AND t.consolidation_version = s.consolidation_version
            WHEN MATCHED THEN DELETE
            """
        )
        out_df.write.format("delta").mode("append").saveAsTable(gold_table)
        rows_written = len(out_rows)

    if status_rows:
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
        status_df = spark.createDataFrame(status_rows, schema=status_schema)
        status_df.createOrReplaceTempView("tmp_insights_07_status")
        spark.sql(
            f"""
            MERGE INTO {ops_file} AS t
            USING tmp_insights_07_status AS s
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
        error_summary = f"{failed_calls} call(s) failed in {STAGE_NAME}."
    elif failed_calls > 0:
        final_status = "FAILED"
        error_summary = f"{failed_calls} call(s) failed in {STAGE_NAME}."
    elif eligible_count == 0:
        final_status = "SUCCESS"
        error_summary = f"No eligible calls for stage {STAGE_NAME}."
    elif not enable_llm_consolidation:
        final_status = "SUCCESS"
        error_summary = "LLM consolidation disabled; eligible calls marked SKIPPED."
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
        ops_runs=ops_runs,
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
