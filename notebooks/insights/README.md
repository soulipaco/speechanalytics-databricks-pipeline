# Insights Runbook

Insights notebooks are implemented and aligned to canonical stage IDs. This repo workflow is static-only right now (no Databricks job execution from repository operations).

## Execution guidance
- Recommended order: `01 -> 08`
- Start with `run_mode=sample`
- Core widgets appear in all stages: `catalog`, `schema`, `run_id`, `run_mode`, `max_files_per_run`

## Execution order + kill switches
Smoke-test order (no external services):
1. `insights_01_compute_conversation_metrics`
2. `insights_02_load_taxonomies_to_dim_tables`
3. `insights_03_build_text_chunks`
4. `insights_08_quality_gates_and_finalize`

Optional/external-service dependent stages:
- `insights_04_embed_chunks.py` (embedding endpoint/service/local model runtime)
- `insights_05_build_vector_search_rag_index.py` (Vector Search backend)
- `insights_06_llm_extract_chunk_insights.py` (LLM backend; optional RAG)
- `insights_07_llm_consolidate_call_insights.py` (LLM backend)

Disable these for first smoke test:
- `enable_embeddings=false` (04)
- `enable_rag_index=false` (05)
- `enable_llm=false` (06)
- `enable_llm_consolidation=false` (07)
- `translation_enabled=false` (01, 03) to avoid translated-table dependency

## Notebook index

### 01) `insights_01_compute_conversation_metrics.py`
- Purpose: Compute non-LLM conversation metrics per call.
- Inputs: `gold_turns_redacted` (and optional `gold_turns_translated` based on config).
- Outputs: `gold_conversation_metrics`, `ops_file_status`, `ops_pipeline_runs`.
- Stage name: `insights_01_compute_conversation_metrics`.
- Idempotency key: overwrite by `call_id + metrics_version`.
- Key widgets: `translation_enabled`, `metrics_version`, `silence_gap_threshold_sec`, `overlap_handling_policy`.

### 02) `insights_02_load_taxonomies_to_dim_tables.py`
- Purpose: Load taxonomy YAML files into dimension tables.
- Inputs: `taxonomies/contact_drivers.yml`, `issues.yml`, `intents.yml`, `emotions.yml`.
- Outputs: `dim_contact_driver`, `dim_issue`, `dim_intent`, `dim_emotion_catalog`, ops tables.
- Stage name: `insights_02_load_taxonomies_to_dim_tables`.
- Idempotency key: taxonomy-level overwrite by table + `taxonomy_version` (or full replace mode).
- Key widgets: `taxonomy_version`, `taxonomies_root`, `taxonomy_load_mode`, `enable_dim_tables`.

### 03) `insights_03_build_text_chunks.py`
- Purpose: Build deterministic chunk units for embedding and LLM extraction.
- Inputs: `gold_turns_translated` (preferred when enabled) else `gold_turns_redacted`.
- Outputs: `silver_text_chunks`, ops tables.
- Stage name: `insights_03_build_text_chunks`.
- Idempotency key: overwrite by `call_id + chunking_version`.
- Key widgets: `translation_enabled`, `chunking_version`, `chunking_policy`, chunk size/overlap parameters.

### 04) `insights_04_embed_chunks.py`
- Purpose: Generate embeddings for chunk rows.
- Inputs: `silver_text_chunks`.
- Outputs: `silver_embeddings`, ops tables.
- Stage name: `insights_04_embed_chunks`.
- Idempotency key: overwrite by `call_id + embedding_version + embedding_model` (plus filters when configured).
- Key widgets: `enable_embeddings`, `embedding_model`, `embedding_version`, `embedding_backend`, `chunking_version_filter`.

### 05) `insights_05_build_vector_search_rag_index.py`
- Purpose: Build/sync retrieval index resources.
- Inputs: `silver_embeddings`, `silver_text_chunks`.
- Outputs: retrieval index artifact(s) and ops updates.
- Stage name: `insights_05_build_vector_search_rag_index`.
- Idempotency key: deterministic index identity (`index_name` + selected embedding/chunk scope).
- Key widgets: `enable_rag_index`, `index_backend`, `index_name`, `embedding_model_filter`, `embedding_version_filter`, `chunking_version_filter`.

### 06) `insights_06_llm_extract_chunk_insights.py`
- Purpose: Extract chunk-level structured insights with taxonomy constraints.
- Inputs: `silver_text_chunks`, taxonomy dims, optional retrieval context.
- Outputs: `silver_llm_chunk_insights`, ops tables.
- Stage name: `insights_06_llm_extract_chunk_insights`.
- Idempotency key: overwrite by `call_id + chunk_id + extraction_version`.
- Key widgets: `enable_llm`, `llm_backend`, `llm_model_name`, `taxonomy_version`, `extraction_version`, `prompt_version`.
- Important: persists `llm_prompt_version` (not `prompt_version`) and stores `chunk_text_hash` (no raw chunk text persisted).

### 07) `insights_07_llm_consolidate_call_insights.py`
- Purpose: Consolidate chunk-level signals into one call-level insights record.
- Inputs: `silver_llm_chunk_insights`, `gold_conversation_metrics`, taxonomy dims.
- Outputs: `gold_speech_insights`, ops tables.
- Stage name: `insights_07_llm_consolidate_call_insights`.
- Idempotency key: overwrite by `call_id + metrics_version + consolidation_version`.
- Key widgets: `enable_llm_consolidation`, `llm_backend`, `llm_model_name`, `metrics_version`, `consolidation_version`, `llm_prompt_version`.
- Important: writes `gold_speech_insights`.

### 08) `insights_08_quality_gates_and_finalize.py`
- Purpose: Run quality gates and finalize Insights run status.
- Inputs: `gold_conversation_metrics`, `gold_speech_insights`, `ops_file_status` (taxonomy dims for membership checks).
- Outputs: stage statuses in `ops_file_status`, terminal run status in `ops_pipeline_runs`, optional latest views.
- Stage name: `insights_08_quality_gates_and_finalize`.
- Idempotency key: MERGE by `call_id + stage_name` (file status), `run_id + workflow_name` (run status).
- Key widgets: `enable_quality_gates`, `finalize_policy`, `allow_warn`, `metrics_version`, `consolidation_version`, `taxonomy_version`, `min_calls_required`.
- Important: this is the quality/finalize stage for Insights.
