# INSIGHTS_06_LLM_EXTRACT_CHUNK_INSIGHTS (docs/tasks/insights_06_llm_extract_chunk_insights.md)

## Task ID
- **ID**: INSIGHTS_06_LLM_EXTRACT_CHUNK_INSIGHTS

## Title
Run LLM extraction per chunk (optional intermediate layer)

## Objective
Improve accuracy and long-call handling by extracting structured signals at chunk level (summary, candidate labels, sentiment/emotion signals). These outputs are later consolidated into a call-level row.

## Scope
### In Scope
- Select eligible chunks.
- Build chunk-level prompts (taxonomy constrained).
- Optionally use RAG to retrieve relevant chunks for each chunk prompt (usually unnecessary; chunk already small).
- Write outputs to `silver_llm_chunk_insights`.
- Update `ops_file_status` stage `insights_06_llm_extract_chunk_insights`.

### Out of Scope
- Final call-level consolidation (separate task).

## Inputs
### Data Inputs
- `silver_text_chunks`
- `dim_contact_driver`, `dim_issue`, `dim_intent`, `dim_emotion_catalog`
- Optional: retrieval backend outputs/logs

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `run_mode` (string, required)
- `max_files_per_run` (int, optional)
- `enable_llm_chunk` (bool, default false)
- `llm_model_name` (string, required when enabled)
- `llm_prompt_version` (string, default `v1`)
- `taxonomy_version` (string, required)
- `enable_rag` (bool, default false)
- `rag_top_k` (int, default 5)

## Outputs
### Data Outputs
- Table: `<catalog>.<schema>.silver_llm_chunk_insights`
  - **Grain**: one row per call_id + chunk_id
  - **Key fields**:
    - chunk_summary
    - candidate_driver_label + confidence
    - candidate_issue_label + confidence
    - candidate_intent_label + confidence
    - sentiment_signal (optional)
    - emotion_signal (optional)
    - llm_model_name, llm_prompt_version, taxonomy_version
    - run_id

### Operational Outputs
- `ops_file_status` stage `insights_06_llm_extract_chunk_insights` SUCCESS/FAILED/SKIPPED per call_id

## Business Rules / Logic
- If `enable_llm_chunk=false`, stage is SKIPPED (and the pipeline can jump to call-level extraction).
- Chunk prompt must restrict labels to active taxonomy values for the selected version.
- Outputs must be schema-valid; invalid outputs are retried or marked failed (policy-defined).

## Error Handling & Failure Isolation
- Per-chunk failures should not necessarily fail the call (policy-defined), but must be recorded.
- Task fails only if all eligible chunks for all calls fail.

## Data Quality Checks
CRITICAL:
- labels must be in taxonomy active set for given version
- confidence in [0,1]
WARNING:
- low confidence across all candidates

## Acceptance Criteria
- For one call_id with multiple chunks, chunk insights exist with valid labels and confidences.
