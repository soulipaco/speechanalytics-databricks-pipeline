# INSIGHTS_07_LLM_CONSOLIDATE_CALL_INSIGHTS (docs/tasks/insights_07_llm_consolidate_call_insights.md)

## Task ID
- **ID**: INSIGHTS_07_LLM_CONSOLIDATE_CALL_INSIGHTS

## Title
Generate final call-level insights (LLM) with schema + taxonomy constraints

## Objective
Produce one consolidated, structured insights record per call in `gold_speech_insights`. This record is the main output for reporting and downstream speech analytics.

## Scope
### In Scope
- Select eligible call_ids.
- Gather input context:
  - preferred: chunk summaries and candidates (if chunk layer exists)
  - else: retrieve top-k chunks from the call (RAG) or feed bounded context
- Execute final extraction and consolidation.
- Enforce strict schema contract and controlled vocabularies.
- Write to `gold_speech_insights`.
- Update `ops_file_status` stage `insights_07_llm_consolidate_call_insights`.

### Out of Scope
- Training or fine-tuning models.
- Producing per-utterance “reasoning traces” (keep outputs compact and safe).

## Inputs
### Data Inputs
- Preferred:
  - `silver_llm_chunk_insights` (if enabled)
  - `silver_text_chunks`
- Always:
  - taxonomy dims: `dim_contact_driver`, `dim_issue`, `dim_intent`, `dim_emotion_catalog`
  - conversation metrics: `gold_conversation_metrics` (optional but useful)
- Retrieval (optional):
  - `silver_embeddings` and backend retrieval

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `run_mode` (string, required)
- `max_files_per_run` (int, optional)
- `enable_llm_insights` (bool, default true)
- `llm_model_name` (string, required when enabled)
- `llm_prompt_version` (string, default `v1`)
- `taxonomy_version` (string, required)
- `enable_rag` (bool, default false)
- `rag_top_k` (int, default 5)
- `insights_version` (string, default `v1`)

## Outputs
### Data Outputs
- Table: `<catalog>.<schema>.gold_speech_insights`
  - **Grain**: one row per call_id
  - **Required fields** (minimum):
    - pii_possible_remaining_flag, pii_notes
    - summary_text
    - contact_driver_label + confidence
    - issue_label + confidence
    - intent_label + confidence
    - resolution + confidence
    - effort + confidence
    - sentiment + confidence
    - customer_emotion_start, customer_emotion_end
    - agent_emotion_start, agent_emotion_end
    - agent_love_score_1_10, brand_love_score_1_10
    - llm_model_name, llm_prompt_version, taxonomy_version
    - rag_enabled_flag, rag_top_k
    - insights_version, run_id

### Operational Outputs
- `ops_file_status` stage `insights_07_llm_consolidate_call_insights` SUCCESS/FAILED/SKIPPED per call_id

## Business Rules / Logic
- Must choose exactly one contact driver/issue/intent from active taxonomy list.
- Must choose enums strictly:
  - resolution: Resolved / Not resolved
  - effort: High / Low
  - sentiment: Positive / Neutral / Negative
- Emotions must come from active emotion catalog.
- Confidence fields must be [0,1].
- Love scores must be integer 1–10.

Fallback if chunk layer is disabled:
- Use top-k chunks as context (or bounded full text) and run a single call-level extraction.

Idempotency:
- upsert by call_id + insights_version (policy-defined)
- do not duplicate rows on rerun

## Error Handling & Failure Isolation
- Invalid schema output is CRITICAL:
  - retry with stricter prompt (optional)
  - else mark FAILED for that call_id
- Per-call failure does not stop the batch.

## Data Quality Checks
CRITICAL:
- all required fields present
- labels exist in taxonomy dims for taxonomy_version
- enums valid
- numeric ranges valid

## Acceptance Criteria
- For at least one call_id, `gold_speech_insights` exists and passes taxonomy and enum validations.
- Re-run does not create duplicates.
