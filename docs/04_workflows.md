# Workflows — Speech Analytics Lakehouse on Databricks

## 1. Purpose

This document defines the **Databricks Workflows** (orchestrated pipelines) used by the project. The Workflows are designed to be:

- **Modular**: one stage per task (no monolithic notebook).
- **Auditable**: each stage writes outputs to Delta tables and updates run/status tables.
- **Incremental**: only new or unprocessed calls are processed.
- **Fault-isolated**: a single bad call should not fail the entire batch.
- **Parameter-driven**: behavior is controlled via workflow parameters (catalog, schema, translation target, feature toggles).

Two workflows are defined:

1. **Foundation Pipeline**: Audio → diarization → ASR → alignment → PII redaction → translation → publish
2. **Insights Pipeline**: metrics → taxonomy → embeddings/RAG → LLM insights → publish → quality checks

---

## 2. Shared Workflow Design Principles

### 2.1 Stage boundaries and “table as contract”
Each task must:
- read from one or more upstream tables/paths
- produce a well-defined downstream table (or update an existing one)
- update `ops_pipeline_runs` and `ops_file_status`

This ensures each stage is independently verifiable.

### 2.2 Idempotency (safe re-runs)
All tasks must be safe to re-run without duplicating results:
- use `call_id` as primary key
- write with upsert/merge semantics where appropriate
- update statuses deterministically

### 2.3 Incremental processing
Each task should operate on a set of `call_id`s derived from:
- `bronze_audio_files` where status indicates NEW or partially processed
- `ops_file_status` stage-level statuses

### 2.4 Failure isolation policy
Batch-oriented tasks must not fail the entire run for single-file errors. The standard policy is:

- For each `call_id`, attempt processing.
- On exception:
  - mark that `call_id` as FAILED for that stage in `ops_file_status`
  - store a concise error message
  - continue to next `call_id`

At the end of the stage:
- if *all* files failed → stage fails
- if some succeeded → stage succeeds with warnings (documented in run summary)

### 2.5 Provenance and versioning
Every output table row must record:
- `run_id`
- model identifiers (asr model, diarization model, translation model, llm model)
- configuration version references (prompt_version, taxonomy_version, redaction_version)

### 2.6 Retry strategy (workflow-level)
Retries should be applied to:
- transient infra errors
- transient download errors
- temporary API timeouts

Retries should not be applied blindly to deterministic failures (e.g., corrupt WAV):
- those should fail fast at file level and be marked FAILED.

---

## 3. Shared Parameters (All Workflows)

All workflows accept a consistent parameter set.

### 3.1 Required parameters
- `catalog`: Unity Catalog catalog name
- `schema`: schema name
- `volume_root`: base Volume path for storage (bronze/silver artifacts)
- `run_mode`: `sample | incremental | full`
- `run_id`: unique identifier for the workflow run (may be auto-generated and passed to tasks)

### 3.2 Feature toggles
- `enable_diarization`: true/false
- `enable_pii_redaction`: true/false (default true)
- `enable_translation`: true/false (default true)
- `translation_target_language`: default `en`
- `enable_embeddings`: true/false
- `enable_llm_insights`: true/false

### 3.3 Operational parameters
- `max_files_per_run`: limit for free tier / testing
- `allowed_languages`: whitelist for language_final (optional)
- `fail_run_if_quality_checks_fail`: true/false

---

## 4. Workflow #1 — Foundation Pipeline (Audio → Clean Transcript)

### 4.1 Objective
Produce analytics-ready, compliance-safe text datasets:
- diarization segments
- ASR segments
- aligned turns
- redacted turns (default)
- translated turns (optional)

### 4.2 Task DAG (logical order)

1) `foundation_01_ingest_audio`  
2) `foundation_02_preprocess_audio` (optional)  
3) `foundation_03_diarize_audio` (optional or fallback enabled)  
4) `foundation_04_transcribe_audio`  
5) `foundation_05_align_turns`  
6) `foundation_06_redact_pii`  
7) `foundation_07_translate_turns`  
8) `foundation_08_publish_and_finalize`  

### 4.3 Task definitions

---

#### Task: foundation_01_ingest_audio
**Purpose**: discover `.wav` files and update `bronze_audio_files` manifest

**Inputs**
- Volume path: `.../bronze/audio_raw/`
- Existing `bronze_audio_files`

**Outputs**
- Upsert into `bronze_audio_files`:
  - new files inserted with status `NEW`
  - existing files unchanged unless metadata refresh is enabled

**Status updates**
- `ops_file_status` stage `ingest_audio` set to SUCCESS per discovered call_id
- `ops_pipeline_runs` updated for task completion

**Acceptance checks**
- New `.wav` file → appears in `bronze_audio_files` with non-null duration/hash
- Re-run ingestion does not create duplicates

---

#### Task: foundation_02_preprocess_audio (optional)
**Purpose**: standardize audio format (mono/16kHz) if required

**Inputs**
- `bronze_audio_files` where status eligible
- raw audio path

**Outputs**
- `silver_audio_preprocessed` (optional table) and/or processed file outputs
- update metadata with processed file path

**Status updates**
- stage `preprocess_audio`

**Acceptance checks**
- processed output exists for each SUCCESS file
- sample_rate == 16000, channels == 1 (policy-defined)

---

#### Task: foundation_03_diarize_audio
**Purpose**: produce speaker segments via diarization or fallback segmentation

**Inputs**
- eligible call_ids (NEW or not diarized)
- raw or preprocessed audio path

**Outputs**
- `silver_diarization_segments`

**Behavior rules**
- If diarization is enabled and resources permit:
  - primary method: pyannote
- If diarization fails (token missing, model failure, resource constraints):
  - fallback method: VAD segmentation (coarser, no true speaker identity)
- Store `method` per segment (pyannote / vad_fallback)

**Status updates**
- stage `diarize_audio`

**Acceptance checks**
- segments have valid start/end
- method populated
- at least one segment exists for calls with speech

---

#### Task: foundation_04_transcribe_audio
**Purpose**: produce ASR segments via Whisper

**Inputs**
- eligible call_ids (NEW or not transcribed)
- audio path (raw or preprocessed)

**Outputs**
- `silver_asr_segments`

**Behavior rules**
- track model_name and compute_type used
- store language_detected per call/segment if available

**Status updates**
- stage `transcribe_audio`

**Acceptance checks**
- non-empty `silver_asr_segments` for at least one call
- start/end exist and are valid

---

#### Task: foundation_05_align_turns
**Purpose**: align ASR text with diarization segments and generate turn-level structure

**Inputs**
- `silver_diarization_segments`
- `silver_asr_segments`

**Outputs**
- `silver_turns_aligned`

**Alignment policy (v1)**
- assign ASR segments to the speaker segment that best overlaps (or midpoint mapping)
- produce turns as contiguous spans per speaker_label
- assign role using a baseline heuristic:
  - Agent/Customer/Unknown (configurable, replaceable)

**Language resolution policy**
- store:
  - language_hint (from bronze)
  - language_detected (from ASR)
  - language_final (resolution rule)

**Status updates**
- stage `align_turns`

**Acceptance checks**
- turn rows exist with speaker_label, start/end, text_original
- language_final populated

---

#### Task: foundation_06_redact_pii
**Purpose**: detect and redact PII in turn text

**Inputs**
- `silver_turns_aligned`

**Outputs**
- `gold_turns_redacted`

**Redaction policy**
- primary detection: Presidio analyzers where applicable
- secondary detection: regex rules (email, phone, IBAN-like patterns, urls)
- post-redaction residual scan:
  - set `pii_residual_risk_flag`

**Security boundary**
- downstream analytics and LLM should use `text_redacted` by default

**Status updates**
- stage `redact_pii`

**Acceptance checks**
- for calls containing known test PII, placeholders appear in redacted text
- pii_found_flag and entity counts are populated appropriately

---

#### Task: foundation_07_translate_turns
**Purpose**: translate redacted turns into target language (default EN)

**Inputs**
- `gold_turns_redacted`
- workflow parameter `translation_target_language`
- `language_final`

**Outputs**
- `gold_turns_translated`

**Translation rules**
- if translation disabled → stage sets SKIPPED and exits
- if `language_final == translation_target_language` → translation_skipped_flag = true, no translation executed
- else translate and store translated text

**Status updates**
- stage `translate_turns`

**Acceptance checks**
- English calls are skipped
- non-English calls have translated text

---

#### Task: foundation_08_publish_and_finalize
**Purpose**: finalize run artifacts, update manifest statuses, and publish stable views

**Inputs**
- downstream tables from prior tasks

**Outputs**
- updates `bronze_audio_files.status` to PROCESSED for completed call_ids
- updates `ops_pipeline_runs` summary fields (counts success/failed)

**Optional deliverables**
- create “latest views” or helper summary tables for demos (policy-defined)

**Acceptance checks**
- each processed call_id shows completed stage statuses
- manifest reflects final state

---

## 5. Workflow #2 - Insights Pipeline (Clean Transcript -> Analytics Insights)

### 5.1 Objective
Generate analytics features and structured LLM insights supported by taxonomies and vector retrieval.

### 5.2 Task DAG

1) `insights_01_compute_conversation_metrics`  
2) `insights_02_load_taxonomies_to_dim_tables`  
3) `insights_03_build_text_chunks`  
4) `insights_04_embed_chunks` (optional)  
5) `insights_05_build_vector_search_rag_index`  
6) `insights_06_llm_extract_chunk_insights`  
7) `insights_07_llm_consolidate_call_insights`  
8) `insights_08_quality_gates_and_finalize`  

### 5.3 Task definitions

---

#### Task: insights_01_compute_conversation_metrics
**Purpose**: compute call-level speech metrics without LLM

**Inputs**
- `silver_diarization_segments` and/or `silver_turns_aligned`
- optionally `gold_turns_redacted` for role-aware metrics

**Outputs**
- `gold_conversation_metrics`

**Metrics policy (v1 minimum)**
- total duration
- agent talk time, customer talk time
- silence time
- overlap time (optional for v1)
- turn counts per role
- average turn duration per role

**Acceptance checks**
- metrics satisfy sanity rules (non-negative, talk time within duration tolerance)

---

#### Task: insights_02_load_taxonomies_to_dim_tables
**Purpose**: load controlled label sets (drivers, issues, intents, emotions) and versions

**Inputs**
- `taxonomies/*.yml` (repo-managed) OR existing dim tables

**Outputs**
- `dim_contact_driver`, `dim_issue`, `dim_intent`, `dim_emotion_catalog`
- a recorded `taxonomy_version` used for the run

**Acceptance checks**
- active labels exist
- each label has definition and at least one example

---

#### Task: insights_03_build_text_chunks
**Purpose**: create chunked text units for retrieval and LLM context management

**Inputs**
- preferred: `gold_turns_translated` if translation enabled
- otherwise: `gold_turns_redacted`

**Outputs**
- `silver_text_chunks`

**Chunking strategy options**
- by time window (e.g., 30-90 seconds)
- by number of turns (e.g., 8-15 turns)
- by approximate token window (if available)

**Acceptance checks**
- chunks cover most of the conversation
- chunk_text is non-empty and compliance-safe (redacted/translated)

---

#### Task: insights_04_embed_chunks (optional)
**Purpose**: create embeddings for chunks to support similarity search (RAG)

**Inputs**
- `silver_text_chunks`

**Outputs**
- `silver_embeddings`

**Acceptance checks**
- embedding vectors are non-null and consistent dimension per model

**Note on free tier**
- If vector search service is unavailable, store embeddings in Delta and document retrieval backend as pluggable.

---

#### Task: insights_05_build_vector_search_rag_index
**Purpose**: build and validate retrieval index resources for RAG over chunk embeddings

**Inputs**
- `silver_text_chunks`
- `silver_embeddings`
- taxonomy tables (optional for hybrid retrieval)

**Outputs**
- vector search index and/or registered retrieval endpoint metadata

**Acceptance checks**
- index is queryable
- top-k retrieval returns expected schema and call/chunk identifiers

---

#### Task: insights_06_llm_extract_chunk_insights
**Purpose**: generate chunk-level structured insight candidates using LLM with taxonomy constraints

**Inputs**
- chunked text from `silver_text_chunks`
- taxonomy tables
- optional retrieval context from vector index

**Outputs**
- chunk-level insights staging output

**Acceptance checks**
- output rows produced for eligible chunks
- required structured fields are present

---

#### Task: insights_07_llm_consolidate_call_insights
**Purpose**: consolidate chunk-level insight candidates into call-level analytics outputs

**Inputs**
- chunk-level insight staging output
- taxonomy tables and consolidation policy/version

**Outputs**
- `gold_speech_insights`

**Acceptance checks**
- one final record per call
- labels are valid taxonomy values for the selected version
- confidence fields and provenance fields are populated

---

#### Task: insights_08_quality_gates_and_finalize
**Purpose**: run quality gates, finalize outputs, and write terminal run status

**Inputs**
- `gold_speech_insights`
- `gold_conversation_metrics`
- optionally `gold_turns_translated` / `gold_turns_redacted`

**Outputs**
- quality summary artifacts (table or ops summary fields)
- final demo/consumer views (optional)
- updated `ops_pipeline_runs` terminal status

**Quality gate examples**
- taxonomy labels valid
- no null summary_text
- translation rules obeyed
- timing sanity checks pass
- pii_residual_risk flagged calls reported

**Acceptance checks**
- if `fail_run_if_quality_checks_fail=true`, workflow fails only when critical checks fail
- otherwise workflow completes with warnings recorded

---

## 6. Run Modes

### 6.1 `sample`
- Process only `max_files_per_run` calls (e.g., 3–10)
- Intended for rapid iteration and free tier compute limits

### 6.2 `incremental`
- Process calls where:
  - bronze status is NEW, or
  - required stage status is missing/FAILED and retry conditions allow
- Intended for day-to-day use

### 6.3 `full`
- Re-process all calls (rare)
- Intended for model version upgrade or major logic changes
- Must record new version identifiers and preserve previous outputs if desired

---

## 7. Operational Expectations

### 7.1 Logging
Each task should emit:
- call counts (attempted/success/failed)
- top error types (by message category)
- timing metrics (optional)
Summaries should be stored in `ops_pipeline_runs`.

### 7.2 Status propagation
A call is considered “foundation complete” when:
- align_turns SUCCESS
- redact_pii SUCCESS
- translate_turns SUCCESS or SKIPPED (depending on configuration)

A call is considered “insights complete” when:
- insights_01_compute_conversation_metrics SUCCESS
- insights_06_llm_extract_chunk_insights SUCCESS
- insights_08_quality_gates_and_finalize SUCCESS

---

## 8. Implementation Notes (Workflow-First Design)

- Workflow tasks should remain small and purpose-specific.
- Intermediate outputs must be durable (tables), not only in-memory.
- “Publish” tasks should be responsible for final state transitions (manifest status updates).
- The Insights Workflow must depend on the Foundation Workflow outputs; it should not redo ASR/diarization.

---
