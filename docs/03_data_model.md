# Data Model — Speech Analytics Lakehouse on Databricks

## 1. Purpose

This document defines the **table contracts** for the Speech Analytics Lakehouse. The pipeline’s primary deliverables are **Delta tables** organized into Bronze, Silver, and Gold layers. Each table below includes:

- **Purpose**: why the table exists
- **Grain**: what one row represents
- **Key columns**: minimal required fields
- **Upstream lineage**: which tables/stages produce it
- **Quality expectations**: key constraints and sanity checks

The goal is to ensure the project produces analytics-ready, auditable outputs with clear ownership and reproducibility.

---

## 2. Naming & Conventions

### 2.1 Layer conventions
- **Bronze**: raw inputs + ingestion manifest
- **Silver**: structured intermediate artifacts (segments, turns, chunks, embeddings)
- **Gold**: curated analytics products (redacted/translated turns, metrics, LLM insights)

### 2.2 Identifiers
- `call_id`: stable identifier for an audio call (recommended: derived from file hash + optional filename component)
- `run_id`: identifier for a pipeline execution (workflow run)
- `*_id`: unique identifiers within a call (segment_id, turn_id, chunk_id)

### 2.3 Language columns
- `language_hint`: user supplied or derived from metadata/folder name
- `language_detected`: model output (ASR)
- `language_final`: resolved language chosen by rule

### 2.4 Time unit conventions
- All speech timing fields use **seconds** (float) relative to start of call:
  - `start_sec`, `end_sec`
- Rule: `0 <= start_sec < end_sec <= total_duration_sec`

---

## 3. Operational Tracking (Ops Tables)

These tables provide auditability and run status tracking.

### 3.1 `ops_pipeline_runs`
**Purpose**: track pipeline executions and provide a high-level run history  
**Grain**: one row per workflow run  

**Key columns**
- `run_id`
- `workflow_name` (foundation / insights)
- `started_at`, `ended_at`
- `status` (RUNNING / SUCCESS / WARN / FAILED)
- `trigger_type` (manual / schedule)
- `parameters` (serialized map/json)
- `error_summary` (nullable)
- `total_files`
- `success_count`
- `failed_count`
- `updated_at`

**Quality expectations**
- `ended_at` must be present when status is SUCCESS/WARN/FAILED
- `WARN` means the pipeline completed with non-zero warnings or partial non-critical failures
- `FAILED` means no eligible items succeeded OR acceptance-critical checks failed
- status must match a controlled set

---

### 3.2 `ops_file_status`
**Purpose**: file-level status per pipeline stage (failure isolation and incremental behavior)  
**Grain**: one row per `call_id` + `stage_name` (latest status)  

**Key columns**
- `call_id`
- `stage_name` (controlled enum, examples:
  - `ingest_audio`
  - `preprocess_audio`
  - `diarize_audio`
  - `transcribe_audio`
  - `align_turns`
  - `redact_pii`
  - `translate_turns`
  - `publish_and_finalize`
  - `insights_01_compute_conversation_metrics`
  - `insights_02_load_taxonomies_to_dim_tables`
  - `insights_03_build_text_chunks`
  - `insights_04_embed_chunks`
  - `insights_05_build_vector_search_rag_index`
  - `insights_06_llm_extract_chunk_insights`
  - `insights_07_llm_consolidate_call_insights`
  - `insights_08_quality_gates_and_finalize`)
- `status` (SUCCESS / WARN / FAILED / SKIPPED)
- `updated_at`
- `error_message` (nullable)
- `run_id` (last run that updated this stage)

**Quality expectations**
- each stage_name is from a controlled enum
- `updated_at` is always increasing for a given `call_id, stage_name`

---

## 4. Bronze Layer

### 4.1 `bronze_audio_files`
**Purpose**: authoritative manifest of audio inputs and processing eligibility  
**Grain**: one row per audio file / call  

**Key columns**
- `call_id`
- `file_path` (Volume path)
- `ingested_at`
- `file_hash` (content hash; primary dedup key)
- `duration_sec`
- `sample_rate`
- `channels`
- `language_hint` (nullable)
- `source_type` (public_dataset / synthetic_tts / elevenlabs / other)
- `status` (NEW / PROCESSED / FAILED)
- `error_message` (nullable)

**Upstream lineage**
- Produced by ingestion stage scanning `Volumes/.../bronze/audio_raw/`

**Quality expectations**
- `file_hash` must be unique (or unique per file_path with de-dup rule)
- `duration_sec > 0`
- sample_rate and channels must be non-null if audio is valid

---

## 5. Silver Layer (Core Speech Artifacts)

### 5.1 `silver_audio_preprocessed` (optional)
**Purpose**: track standardized audio conversion (mono/16kHz)  
**Grain**: one row per `call_id`  

**Key columns**
- `call_id`
- `input_path`, `output_path`
- `output_sample_rate` (expected 16000)
- `output_channels` (expected 1)
- `preprocess_version`
- `run_id`
- `status`, `error_message`

**Quality expectations**
- if status SUCCESS: output file exists and is readable

---

### 5.2 `silver_diarization_segments`
**Purpose**: speaker segmentation produced by diarization or fallback logic  
**Grain**: one row per speaker segment within a call  

**Key columns**
- `call_id`
- `segment_id`
- `speaker_label` (e.g., SPEAKER_00)
- `start_sec`, `end_sec`
- `method` (pyannote / vad_fallback)
- `confidence` (nullable)
- `diarization_model`
- `diarization_version`
- `run_id`

**Quality expectations**
- `start_sec < end_sec`
- no segments outside call duration
- segments should not have extreme overlaps unless method supports it

---

### 5.3 `silver_asr_segments`
**Purpose**: speech-to-text segments produced by ASR model  
**Grain**: one row per ASR segment within a call  

**Key columns**
- `call_id`
- `asr_segment_id`
- `start_sec`, `end_sec`
- `text`
- `language_detected`
- `asr_model_name` (e.g., faster-whisper large)
- `compute_type` (float16 / int8 / etc.)
- `avg_logprob` (nullable)
- `no_speech_prob` (nullable)
- `run_id`

**Quality expectations**
- `text` non-empty for speech segments (allow empty only if explicitly tagged as no-speech)
- `start_sec < end_sec`

---

### 5.4 `silver_turns_aligned`
**Purpose**: aligned conversation turns combining diarization segments and ASR segments  
**Grain**: one row per “turn” (speaker-attributed text span)  

**Key columns**
- `call_id`
- `turn_id`
- `speaker_label`
- `role` (Agent / Customer / Unknown)
- `start_sec`, `end_sec`
- `text_original`
- `language_hint` (nullable)
- `language_detected` (nullable)
- `language_final`
- `alignment_version`
- `run_id`

**Upstream lineage**
- Produced by aligning `silver_diarization_segments` with `silver_asr_segments`

**Quality expectations**
- `start_sec < end_sec`
- `text_original` may be empty for silence segments only if explicitly allowed (prefer: exclude silence turns)
- roles must be from controlled enum

---

## 6. Gold Layer (Compliance + Analytics Products)

### 6.1 `gold_turns_redacted`
**Purpose**: compliance-safe turn text for analytics and LLM consumption  
**Grain**: one row per turn  

**Key columns**
- `call_id`
- `turn_id`
- `role`
- `start_sec`, `end_sec`
- `text_redacted`
- `pii_found_flag` (boolean)
- `pii_entities` (structured: list/map; may include offsets, types)
- `pii_entity_counts` (map: entity_type → count)
- `pii_residual_risk_flag` (boolean)
- `redaction_method` (presidio + regex)
- `redaction_version`
- `run_id`
- `updated_at`

**Upstream lineage**
- Produced from `silver_turns_aligned`

**Quality expectations**
- if `pii_found_flag=true` then `pii_entity_counts` not empty
- if `pii_residual_risk_flag=true` then flagged for review or stricter masking policy

---

### 6.2 `gold_turns_translated` (conditional)
**Purpose**: normalized language representation for analytics (default English)  
**Grain**: one row per turn  

**Key columns**
- `call_id`
- `turn_id`
- `role`
- `start_sec`, `end_sec`
- `language_final`
- `translation_target_language` (default `en`)
- `translation_skipped_flag` (boolean)
- `text_redacted_source`
- `text_translated` (nullable if skipped)
- `translation_model`
- `translation_version`
- `run_id`
- `updated_at`

**Upstream lineage**
- Produced from `gold_turns_redacted`

**Quality expectations**
- if skipped_flag=true: translated text may be null or equal to source (policy-defined)
- if skipped_flag=false: translated text must be non-null and non-empty

---

### 6.3 `gold_conversation_metrics`
**Purpose**: non-LLM speech analytics metrics per call  
**Grain**: one row per `call_id` per `metrics_version`  

**Key columns**
- `call_id`
- `total_duration_sec`
- `agent_talk_time_sec`
- `customer_talk_time_sec`
- `unknown_talk_time_sec`
- `silence_time_sec`
- `overlap_time_sec` (optional)
- `turn_count_total`
- `turn_count_agent`
- `turn_count_customer`
- `turn_count_unknown`
- `num_turns_agent`
- `num_turns_customer`
- `avg_turn_length_sec`
- `avg_turn_duration_agent_sec`
- `avg_turn_duration_customer_sec`
- `first_turn_ts_sec`
- `last_turn_ts_sec`
- `overlap_warning_flag`
- `metrics_version`
- `run_id`
- `source_turns_table`
- `overlap_handling_policy`
- `silence_gap_threshold_sec`
- `updated_at`

**Upstream lineage**
- Derived from `silver_diarization_segments` and/or `silver_turns_aligned`

**Quality expectations**
- talk_time values >= 0
- (agent + customer + silence) approx equals total (tolerance allowed)
- number of turns consistent with turns table

---

## 7. Silver/Gold Layer (RAG / Vector Retrieval Assets)

### 7.1 `silver_text_chunks`
**Purpose**: chunked text units for embeddings and retrieval  
**Grain**: one row per chunk per call  

**Key columns**
- `call_id`
- `chunk_id`
- `start_sec`, `end_sec` (nullable if chunk is token-based)
- `chunk_text` (redacted or translated depending on configuration)
- `chunk_source` (redacted / translated)
- `chunking_strategy` (by_turn_count / time_window / token_window)
- `chunking_version`
- `run_id`

**Quality expectations**
- chunk_text not empty
- chunks cover the conversation without major gaps (policy-defined)

---

### 7.2 `silver_embeddings`
**Purpose**: embeddings for chunks used in similarity search  
**Grain**: one row per chunk per embedding model  

**Key columns**
- `call_id`
- `chunk_id`
- `embedding_model`
- `embedding_vector`
- `embedding_version`
- `run_id`

**Quality expectations**
- embedding_vector has consistent dimension for a given model
- no null vectors for SUCCESS rows

---

### 7.3 `silver_llm_chunk_insights` (optional)
**Purpose**: chunk-level structured LLM extraction output used as staging input for call-level consolidation  
**Grain**: one row per `call_id` + `chunk_id` + `extraction_version`  

**Key columns**
- `call_id`
- `chunk_id`
- `chunk_text_hash`
- `taxonomy_version`
- `candidate_driver_label`
- `candidate_driver_confidence`
- `candidate_issue_label`
- `candidate_issue_confidence`
- `candidate_intent_label`
- `candidate_intent_confidence`
- `pii_possible_remaining_flag`
- `pii_notes`
- `chunk_summary`
- `sentiment_signal`
- `sentiment_confidence`
- `customer_emotion_signal`
- `agent_emotion_signal`
- `llm_model_name`
- `llm_provider`
- `llm_backend`
- `llm_prompt_version`
- `extraction_version`
- `rag_used_flag`
- `rag_backend`
- `rag_index_name`
- `rag_top_k`
- `rag_retrieved_chunk_ids`
- `llm_response_hash`
- `run_id`
- `updated_at`

**Quality expectations**
- no raw transcript text persisted; use hashed references
- labels must belong to active taxonomy values for selected `taxonomy_version`
- confidence fields must be in [0,1]
- one output row per successful chunk extraction per version

---

## 8. Gold Layer (LLM Insights)

### 8.1 `gold_speech_insights`
**Purpose**: structured speech analytics outputs per call  
**Grain**: one row per `call_id` + `metrics_version` + `consolidation_version`  

**Key columns**
- `call_id`

**Summary**
- `summary_text` (short, structured)

**Taxonomy labels**
- `contact_driver_label`
- `issue_label`
- `intent_label`
- `contact_driver_confidence`
- `issue_confidence`
- `intent_confidence`
- `taxonomy_version`

**Outcomes**
- `resolution` (Resolved / Not resolved)
- `resolution_confidence`
- `effort` (High / Low)
- `effort_confidence`
- `sentiment` (Positive / Neutral / Negative)
- `sentiment_confidence`

**Emotion timeline**
- `customer_emotion_start`, `customer_emotion_end`
- `agent_emotion_start`, `agent_emotion_end`
- `customer_emotion_start_score`, `customer_emotion_end_score` (e.g., -1..+1)
- `agent_emotion_start_score`, `agent_emotion_end_score`
- `customer_emotion_shift_score` (end - start)
- `agent_emotion_shift_score`

**Love scores**
- `agent_love_score_1_10`
- `brand_love_score_1_10`

**Compliance check (LLM-assisted, secondary)**
- `pii_possible_remaining_flag`
- `pii_notes` (short)
- `recommended_next_action`
- `risk_flags`
- `compliance_flags`

**Provenance**
- `llm_model_name`
- `llm_provider`
- `llm_backend`
- `llm_prompt_version`
- `rag_enabled_flag`
- `rag_used_flag`
- `rag_backend`
- `rag_index_name`
- `rag_top_k`
- `rag_retrieved_chunk_ids`
- `metrics_version`
- `consolidation_version`
- `insights_version`
- `run_id`
- `updated_at`

**Upstream lineage**
- Derived from `gold_turns_redacted` or `gold_turns_translated`
- May use `silver_embeddings` for retrieval augmentation

**Quality expectations**
- labels must belong to active taxonomy values
- required fields not null (except optional notes)
- confidence values in [0,1] (or controlled enum if using categorical confidence)

---

## 9. Dimension / Configuration Tables (Taxonomies)

These may be loaded from YAML or maintained as Delta dimension tables.

### 9.1 `dim_contact_driver`
**Grain**: one row per label per version  
**Key columns**
- `label`
- `definition`
- `examples` (array)
- `synonyms` (array)
- `active_flag`
- `taxonomy_version`

### 9.2 `dim_issue` / `dim_intent`
Same structure as above.

### 9.3 `dim_emotion_catalog`
**Purpose**: controlled emotion vocabulary and sentiment mapping  
**Key columns**
- `emotion_name`
- `sentiment_group` (Positive / Neutral / Negative)
- `polarity_score` (-1..+1)
- `definition`
- `examples` (array)
- `active_flag`
- `catalog_version`

---

## 10. Cross-Table Lineage (Simplified)

1. Audio files → `bronze_audio_files`
2. Diarization → `silver_diarization_segments`
3. ASR → `silver_asr_segments`
4. Align → `silver_turns_aligned`
5. Redact → `gold_turns_redacted`
6. Translate (optional) → `gold_turns_translated`
7. Metrics → `gold_conversation_metrics`
8. Chunk + Embed → `silver_text_chunks` + `silver_embeddings`
9. Chunk LLM Extraction (optional) → `silver_llm_chunk_insights`
10. LLM Insights → `gold_speech_insights`

---

## 11. Data Quality Rules (Minimum Set)

These rules should be checked per run and reported.

### 11.1 Timing consistency
- `start_sec < end_sec` for all segments/turns
- segments/turns must be within call duration (tolerance allowed)

### 11.2 Referential integrity
- Every `call_id` in Silver/Gold exists in `bronze_audio_files`

### 11.3 Translation logic
- If `language_final == translation_target_language` then `translation_skipped_flag=true`

### 11.4 Taxonomy constraints
- Taxonomy labels must be from active `dim_*` tables for that version

### 11.5 Compliance
- If `pii_found_flag=true`, redacted text must contain at least one placeholder token
- If residual risk flag true, call is tagged for review

---
