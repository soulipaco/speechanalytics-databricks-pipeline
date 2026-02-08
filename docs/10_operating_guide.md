# Operating Guide — Speech Analytics Lakehouse on Databricks (docs/10_operating_guide.md)

## Status Update

Documentation completed so far (draft-ready):
- `docs/00_roadmap.md`
- `docs/01_problem_statement.md`
- `docs/02_architecture.md`
- `docs/03_data_model.md`
- `docs/04_workflows.md`
- `docs/05_security_and_pii.md`
- `docs/09_testing_strategy.md`
- `docs/06_llm_insights_design.md`
- `docs/10_operating_guide.md` (this document)

Taxonomies created (starter):
- `taxonomies/contact_drivers.yml`
- `taxonomies/issues.yml`
- `taxonomies/intents.yml`
- `taxonomies/emotions.yml`

This guide explains how to **operate** the project in Databricks: how to set it up, run workflows, choose parameters, verify outputs, and troubleshoot.

---

## 1. Purpose

This guide defines the operational steps required to:
- set up storage (Unity Catalog Volumes)
- place sample audio in Bronze
- configure pipeline parameters (translation, diarization, RAG)
- run Foundation and Insights Workflows
- validate outputs (tables + quality checks)
- troubleshoot common failures (free-tier constraints, model downloads, tokens)

This project is intended for batch demos and portfolio showcasing, not production SLAs.

---

## 2. Prerequisites

### 2.1 Databricks platform prerequisites
- Databricks workspace access (personal/free tier supported for small sample sets)
- Ability to:
  - create schemas and tables
  - create or use a Unity Catalog catalog (recommended)
  - create a Volume (recommended for governed files)

### 2.2 Data prerequisites
- Synthetic or licensed `.wav` call recordings
- Recommended test set:
  - 1 English call
  - 1 non-English call
  - 1 multi-speaker call
  - 1 call containing **fictional** test PII (email/phone) to validate redaction

### 2.3 Optional model prerequisites
Depending on diarization/translation/LLM choices:
- Hugging Face token (if required by diarization model downloads)
- LLM endpoint access (if using a hosted LLM)
- Embedding model availability (local or endpoint)

All secrets must be managed outside the repo.

---

## 3. Storage Setup (Unity Catalog + Volumes)

### 3.1 Recommended Volume layout
Create or ensure a Volume structure like:

- `Volumes/<catalog>/<schema>/bronze/audio_raw/`
- `Volumes/<catalog>/<schema>/silver/audio_preprocessed/` (optional)
- `Volumes/<catalog>/<schema>/silver/artifacts/` (optional)

**Operating rule:** raw audio is immutable after ingestion; changes are handled as new files.

### 3.2 Bronze data placement
Place `.wav` files into:
- `.../bronze/audio_raw/`

Recommended file naming (for clarity):
- `<language>_<call_id>_<date>.wav`
Examples:
- `en_demo001_2026-02-01.wav`
- `tr_demo002_2026-02-01.wav`

The system must not rely on filenames for correctness, but names help debugging.

---

## 4. Database Objects Setup (Tables)

### 4.1 Minimum required tables (created by pipeline or bootstrap)
Operational tables:
- `ops_pipeline_runs`
- `ops_file_status`

Data tables:
- `bronze_audio_files`
- `silver_asr_segments`
- `silver_diarization_segments` (if diarization enabled)
- `silver_turns_aligned`
- `gold_turns_redacted`
- `gold_turns_translated` (if translation enabled)
- `gold_conversation_metrics` (insights pipeline)
- `gold_speech_insights` (insights pipeline)

Taxonomy tables are either:
- loaded from `taxonomies/*.yml` into `dim_*` tables, OR
- directly maintained as `dim_*` tables

See `docs/03_data_model.md` for table contracts.

---

## 5. Configuration and Parameters

### 5.1 Standard workflow parameters
All workflow runs should accept a consistent parameter set:

**Core**
- `catalog`: UC catalog name
- `schema`: schema name
- `volume_root`: base volume directory
- `run_mode`: `sample | incremental | full`
- `max_files_per_run`: integer (recommended in free tier)

**Feature toggles**
- `enable_diarization`: true/false
- `enable_pii_redaction`: true/false (recommended true)
- `enable_translation`: true/false
- `translation_target_language`: default `en`
- `enable_embeddings`: true/false
- `enable_llm_insights`: true/false

**Quality gates**
- `fail_run_if_quality_checks_fail`: true/false

### 5.2 Recommended settings for free tier demos
- `run_mode=sample`
- `max_files_per_run=3` (or lower if running Whisper large)
- `enable_diarization=true` (if resources permit; otherwise keep fallback enabled)
- `enable_translation=true`, `translation_target_language=en`
- `enable_embeddings=false` for first successful run
- `enable_llm_insights=false` for first successful run
Then enable embeddings/LLM incrementally.

---

## 6. How to Run (Operational Sequence)

### 6.1 Step 1 — Run Foundation Workflow
**Goal:** Produce `gold_turns_redacted` and optionally `gold_turns_translated`.

Run order:
1) Ensure `.wav` files exist in Bronze volume path
2) Execute Foundation Workflow with sample mode

Expected outputs:
- `bronze_audio_files` updated
- `silver_asr_segments` populated
- `silver_turns_aligned` populated
- `gold_turns_redacted` populated
- `gold_turns_translated` populated (if enabled)

### 6.2 Step 2 — Validate Foundation Outputs
Validate:
- row existence for each call_id
- translation skip logic
- PII redaction placeholders for test PII calls
- ops status tables updated for each stage

Refer to `docs/09_testing_strategy.md` for required checks.

### 6.3 Step 3 — Run Insights Workflow
**Goal:** Produce call metrics and structured LLM outputs.

Run order:
1) Ensure foundation outputs exist
2) Execute Insights Workflow
   - optionally start with metrics only (LLM disabled)
   - then enable embeddings and LLM extraction

Expected outputs:
- `gold_conversation_metrics` populated
- `gold_speech_insights` populated (if enabled)
- taxonomy dimension tables loaded/available

### 6.4 Step 4 — Validate Insights Outputs
Validate:
- taxonomy labels belong to active dimension values
- required fields are present
- confidence and score ranges valid
- workflow quality gates recorded

---

## 7. Verification Checklist (Per Run)

### 7.1 Foundation success criteria
For each processed `call_id`:
- present in `bronze_audio_files`
- present in `gold_turns_redacted`
- if translation enabled:
  - present in `gold_turns_translated`
  - skip rule respected (if source==target)
- `ops_file_status` shows SUCCESS for:
  - ingest_audio
  - transcribe_audio
  - align_turns
  - redact_pii
  - translate_turns (SUCCESS or SKIPPED)

### 7.2 Insights success criteria
For each eligible `call_id`:
- metrics row exists in `gold_conversation_metrics`
- if LLM enabled:
  - insights row exists in `gold_speech_insights`
  - labels valid
  - required enums valid
  - score ranges valid

---

## 8. Troubleshooting Guide

### 8.1 Audio ingestion issues
**Symptoms**
- file not detected
- duration/hash missing

**Typical causes**
- file not under expected Volume path
- unsupported format or corrupted file

**Operational actions**
- confirm path and file extension
- try a known-good sample `.wav`
- ensure Volume permissions and mount path are correct

---

### 8.2 Whisper performance and runtime limits
**Symptoms**
- job takes too long
- OOM / resource errors

**Typical causes**
- Whisper large model too heavy for cluster size
- too many files per run
- long audio duration

**Operational actions**
- reduce `max_files_per_run`
- start with shorter calls
- document a staged approach:
  - small model for dev, large model for “final demo run”
- cache model artifacts where possible

---

### 8.3 Diarization failures (pyannote)
**Symptoms**
- diarization task fails
- no segments produced

**Typical causes**
- missing Hugging Face token
- model download blocked
- resource constraints

**Operational actions**
- confirm secret/token setup
- enable fallback segmentation
- proceed with ASR-only alignment and document limitation

---

### 8.4 PII redaction not catching test PII
**Symptoms**
- test email/phone remains visible

**Typical causes**
- Presidio language limitations
- PII format not matched
- ASR altered text

**Operational actions**
- ensure regex layer is enabled
- adjust placeholder rules and increment `redaction_version`
- use multiple synthetic PII formats in test calls

---

### 8.5 Translation output missing or incorrect skip logic
**Symptoms**
- translated text null when not skipped
- translation performed even when source==target

**Typical causes**
- language_final resolution incorrect
- translation parameters missing

**Operational actions**
- inspect `language_hint`, `language_detected`, `language_final`
- confirm `translation_target_language` passed to workflow
- enforce skip rule as a CRITICAL quality check

---

### 8.6 LLM outputs invalid (schema/taxonomy)
**Symptoms**
- missing fields
- labels not in taxonomy
- invalid enums

**Typical causes**
- prompt drift
- taxonomy not loaded correctly
- retrieval context missing or noisy

**Operational actions**
- enforce strict schema validation (fail fast)
- re-run with lower complexity: no RAG, smaller prompt
- ensure `dim_*` tables contain active labels and correct version
- increment `prompt_version` when instructions change

---

### 8.7 Embeddings/RAG not available (free tier limitations)
**Symptoms**
- vector search service unavailable
- embedding stage slow

**Operational actions**
- store embeddings in Delta only
- implement retrieval as a pluggable module (documented)
- run LLM without RAG in v1 and add RAG later

---

## 9. Safe Demo Practices (Public Portfolio)

- Never publish real calls or real transcripts.
- Use synthetic calls created from fictional scripts.
- Ensure PII in test calls is fictional and still redacted.
- `legacy_private/` is intentionally excluded from Git history.
- Legacy notebooks may exist locally for reference only.
- Prefer publishing:
  - architecture diagram
  - table schemas
  - sample *aggregated* insights (labels, sentiment, metrics) without verbatim transcript text

See `docs/05_security_and_pii.md`.

---

## 10. Runbook Summary (Minimal)

1. Put synthetic `.wav` files into Bronze Volume path.
2. Run Foundation Workflow in sample mode.
3. Validate `gold_turns_redacted` and translation behavior.
4. Run Insights Workflow (metrics only first).
5. Enable embeddings/RAG and LLM insights.
6. Validate `gold_speech_insights` schema and taxonomy constraints.
7. Capture screenshots/exports for portfolio assets.

---
