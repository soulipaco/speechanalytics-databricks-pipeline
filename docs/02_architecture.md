# Architecture — Speech Analytics Lakehouse on Databricks (docs/02_architecture.md)

## Status Update

Documentation completed so far (draft-ready):
- `docs/00_roadmap.md`
- `docs/01_problem_statement.md`
- `docs/03_data_model.md`
- `docs/04_workflows.md`
- `docs/05_security_and_pii.md`
- `docs/09_testing_strategy.md`
- `docs/06_llm_insights_design.md`

This document (`docs/02_architecture.md`) defines the overall system architecture and how all components fit together.

---

## 1. Purpose

This document describes the end-to-end architecture of a Databricks-native Speech Analytics Lakehouse, including:

- storage and governance (Unity Catalog + Volumes)
- pipeline layering (Bronze/Silver/Gold Delta tables)
- orchestration (Databricks Workflows)
- compliance boundary (PII redaction)
- multilingual normalization (translation with skip logic)
- LLM insights layer with taxonomies and RAG (vector retrieval)

The architecture is designed to be understandable, auditable, and production-shaped even when executed on limited resources (personal free tier).

---

## 2. Architecture Overview (Narrative)

The system is composed of two pipelines:

1. **Foundation Pipeline (Speech-to-Text Foundation)**
   - Ingest raw `.wav` audio into governed storage
   - Derive diarization segments (who spoke when)
   - Derive ASR segments (what was said when)
   - Align diarization and ASR into conversation turns
   - Detect and redact PII (Presidio + rules)
   - Optionally translate to a target language (default English)
   - Publish compliance-safe, analytics-ready turn data

2. **Insights Pipeline (Speech Analytics & LLM Insights)**
   - Compute speech metrics (talk time, silence, overlap, turns)
   - Load controlled taxonomies (contact drivers/issues/intents/emotions)
   - Chunk and embed text for retrieval (RAG)
   - Run LLM extraction with taxonomy constraints and chunk-level context
   - Publish call-level insights (labels, summary, outcomes, emotions, love scores)
   - Apply quality gates and produce run reports

The separation allows frequent re-analysis (Insights) without re-running expensive ASR (Foundation).

---

## 3. Platform and Governance Architecture

### 3.1 Unity Catalog (UC)
Unity Catalog provides:
- governed data objects (tables, schemas)
- permission boundaries (conceptual, even in personal environment)
- consistent naming and lineage

### 3.2 Volumes
Volumes are used for file-based assets:
- raw audio (`bronze/audio_raw`)
- optional processed audio (`silver/audio_preprocessed`)
- optional intermediate artifacts (JSON outputs, logs)

This prevents “random DBFS files” and demonstrates governed storage discipline.

### 3.3 Separation of sensitive data
Sensitive artifacts are segmented by design:
- raw audio: most sensitive
- unredacted text: sensitive
- redacted text: default analytics surface
- insights: derived outputs, still handled carefully in demos

---

## 4. Lakehouse Layering (Bronze/Silver/Gold)

### 4.1 Bronze (Raw + Manifest)
Bronze stores raw inputs and the authoritative manifest:
- raw `.wav` files in Volumes
- `bronze_audio_files` Delta table that tracks:
  - file metadata, language hints, source type
  - status for incremental processing
  - errors for failure isolation

### 4.2 Silver (Structured Speech Artifacts)
Silver stores structured intermediate results:
- diarization segments (`silver_diarization_segments`)
- ASR segments (`silver_asr_segments`)
- aligned turns (`silver_turns_aligned`)
- chunks and embeddings for RAG (`silver_text_chunks`, `silver_embeddings`)

Silver enables reuse: insights can be recalculated without redoing upstream work.

### 4.3 Gold (Curated Analytics Products)
Gold tables are the “data products” for BI and analytics:
- `gold_turns_redacted` (default text surface)
- `gold_turns_translated` (optional normalized language surface)
- `gold_conversation_metrics` (non-LLM speech metrics)
- `gold_speech_insights` (LLM outputs + provenance)

---

## 5. Orchestration Architecture (Databricks Workflows)

### 5.1 Workflow 1 — Foundation Pipeline
A DAG of stages that transforms audio into redacted/translated turn data. Key properties:
- incremental processing based on manifest status
- file-level failure isolation
- deterministic outputs with provenance fields

### 5.2 Workflow 2 — Insights Pipeline
A DAG that transforms turn data into metrics and structured insights. Key properties:
- chunking to handle long calls
- optional RAG to improve accuracy and consistency
- taxonomy constraints to prevent hallucinated categories
- quality gates to enforce schema validity

### 5.3 Shared operational tables
Both workflows write to operational tracking tables:
- `ops_pipeline_runs`
- `ops_file_status`

These provide run history, debugging visibility, and reproducibility.

---

## 6. Multilingual Handling Architecture

### 6.1 Language fields and resolution
Each call records:
- `language_hint` (provided/metadata)
- `language_detected` (ASR output)
- `language_final` (rule-resolved)

Both hint and detected values are retained for auditability.

### 6.2 Translation strategy
Translation is applied after PII redaction:
- default target language is English (`en`)
- translation is configurable
- skip rule:
  - if `language_final == translation_target_language`, translation is skipped

Outputs include:
- translated text
- model identifiers
- skipped flag
- provenance

---

## 7. Compliance Architecture (PII Boundary)

### 7.1 Layered PII defense
1. Presidio detection + anonymization
2. Regex rule-based patterns (email, phone, ID-like patterns)
3. Residual risk scan after redaction

### 7.2 Default safe surface
- LLM and analytics stages consume **redacted** text by default
- raw/unredacted text is treated as restricted and optional

### 7.3 LLM-assisted compliance check
LLM may output a secondary flag (“possible remaining PII”), but it is not treated as the primary compliance control.

---

## 8. LLM + RAG Architecture

### 8.1 Why RAG is needed
Long calls and complex topics reduce LLM reliability. RAG improves:
- relevance of context provided to the LLM
- consistency of taxonomy classification
- reduction of hallucination and drift

### 8.2 Retrieval sources
Two recommended retrieval channels:
- **In-call retrieval**: retrieve the most relevant chunks from the same call
- **Taxonomy example retrieval**: retrieve the most relevant taxonomy examples (drivers/issues/intents) to anchor classification

### 8.3 Chunking and aggregation
- chunk-level insights are produced first (optional to store)
- call-level outputs are consolidated:
  - by deterministic rules and/or a second consolidation prompt
  - weighted emphasis on late-call chunks for resolution and end-state sentiment
  - early vs late chunk focus for “emotion start” vs “emotion end”

### 8.4 Output contract enforcement
LLM outputs must follow:
- strict schema
- controlled enums
- taxonomy label constraints
- confidence and score ranges
- prompt/model/taxonomy versioning

---

## 9. Reference Architecture Diagram (Text Specification)

### 9.1 Data flow diagram (conceptual)
Bronze:
- Volume: `audio_raw/*.wav`
- Table: `bronze_audio_files`

Silver:
- `silver_diarization_segments`
- `silver_asr_segments`
- `silver_turns_aligned`
- `silver_text_chunks`
- `silver_embeddings`

Gold:
- `gold_turns_redacted`
- `gold_turns_translated` (optional)
- `gold_conversation_metrics`
- `gold_speech_insights`

Workflows:
- Foundation Pipeline: Bronze → Silver → Gold (turns)
- Insights Pipeline: Gold(turns) → Silver(chunks/embeddings) → Gold(metrics/insights)

Ops:
- `ops_pipeline_runs`, `ops_file_status` updated throughout

### 9.2 Suggested visual (for repo assets)
Optionally create an image under `assets/architecture.png` (recommended for demos) with:
- two swimlanes: Foundation and Insights
- arrows between tables and stages
- a “PII boundary” line before LLM stage
- taxonomy + vector retrieval feeding into LLM task

---

## 10. Operational Considerations

### 10.1 Incremental processing and idempotency
- manifest-driven eligibility (NEW/FAILED/retry)
- upsert semantics to prevent duplicate rows
- stage-level statuses per call_id

### 10.2 Versioning and drift
Store versions for:
- diarization model + config
- ASR model + compute type
- redaction rules + presidio config version
- translation model version
- embedding model version
- LLM model + prompt version + taxonomy version

### 10.3 Quality gates
Quality checks are applied to:
- timing constraints
- translation skip logic
- taxonomy validity
- schema compliance for LLM output

---

## 11. Definition of Done (Architecture Level)

The architecture is considered implemented when:
- both workflows exist and run end-to-end on a multilingual sample dataset
- Gold tables can be queried to produce:
  - redacted/translated turn data
  - conversation metrics
  - structured LLM insights
- provenance fields and ops tracking are populated per run
- documentation matches implemented behavior

---
