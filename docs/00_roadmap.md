# Speech Analytics Lakehouse on Databricks — Roadmap

## 1. Overview

This project delivers an end-to-end **Speech Analytics Lakehouse** implemented on **Databricks (Unity Catalog + Volumes + Delta Lake + Workflows)**.

The system ingests multilingual call audio (`.wav`) into governed storage, produces structured datasets through diarization and transcription, enforces compliance through PII detection and redaction, optionally translates transcripts into a target language (default: English), and generates speech-analytics insights using an LLM with **taxonomy-driven classification** and **vector retrieval (RAG)** for improved consistency and accuracy.

The end product is a set of **Gold Delta tables** that enable downstream BI and analytics (Dashboards / SQL / notebooks).

---

## 2. Goals

### 2.1 Primary Goals
- Provide an end-to-end Databricks project that is **production-shaped** (modular stages, orchestration, tracking, auditable outputs).
- Support **multilingual** audio processing and analytics.
- Produce compliance-safe outputs via **PII detection + redaction** (primary method: Microsoft Presidio; secondary methods: regex-based rules and residual risk checks).
- Enable default translation to **English**, with configurable target language and a skip rule when source and target match.
- Provide a speech insights layer that outputs:
  - Summary of the transcript
  - Contact Driver classification (taxonomy-based)
  - Issue classification (taxonomy-based)
  - Intent classification (taxonomy-based)
  - Resolution (Resolved / Not resolved)
  - Effort (High / Low)
  - Sentiment (Positive / Neutral / Negative)
  - Emotion start/end for Agent and Customer based on an Emotion Catalog
  - Agent Love Score (1–10)
  - Brand Love Score (1–10)
- Provide vector-based retrieval (RAG) so that the LLM uses **retrieved taxonomy examples and/or similar call snippets** for improved classification and reduced hallucination.

### 2.2 Non-Goals (Explicitly Out of Scope for v1)
- Real-time streaming ingestion (v1 is batch-oriented).
- Automatic speaker role identification using supervised models (v1 uses a configurable heuristic; later versions may improve this).
- Voice biometrics or identity recognition.
- Compliance certification; this is a technical demo and reference implementation.

---

## 3. Constraints & Assumptions

### 3.1 Constraints
- Databricks environment is the **personal free tier** (resource limits may apply).
- Sample audio must be **public / licensed / synthetic** to avoid handling real PII.
- Secrets (HF token, API keys) must never be committed to GitHub; use secret scopes or environment variables.

### 3.2 Assumptions
- Audio input is `.wav` format; support for additional formats is optional.
- The pipeline is designed for “call-center style” audio where diarization is relevant (multi-speaker).
- The project prioritizes clarity, governance, and reproducibility over maximum performance in v1.

---

## 4. Architecture Summary (High Level)

### 4.1 Storage & Governance
- Audio files stored in **Unity Catalog Volumes** under Bronze.
- Processed artifacts and intermediate outputs stored in Silver.
- Analytics-ready datasets stored in Gold as **Delta tables**.

### 4.2 Orchestration
Two Databricks Workflows:
1. **Foundation Pipeline**: audio → diarization → ASR → alignment → PII redaction → translation → publish
2. **Insights Pipeline**: metrics → embeddings/vector index → LLM extraction → publish insights → quality checks

### 4.3 Data Products (Gold)
- `gold_turns_redacted` and optional `gold_turns_translated`
- `gold_conversation_metrics`
- `gold_speech_insights`
- `dim_*` taxonomy and emotion catalog tables (or YAML sources mirrored into tables)

---

## 5. Roadmap Strategy

The roadmap follows a **thin vertical slice approach**:
- Build a minimal end-to-end path first (1 file → Gold output).
- Add one capability at a time (diarization, PII, translation, metrics, RAG, LLM insights).
- Ensure every stage writes persistent outputs and updates run status tables.

Each milestone includes:
- Deliverables (artifacts produced)
- Acceptance criteria (verifiable outcomes)
- Risks and mitigations
- “Codex task slicing” hints (how to break into small tasks)

---

## 6. Milestones

### Milestone 0 — Repository & Documentation Foundation
**Objective:** Establish a professional repository structure and “single source of truth” documentation so implementation can be delegated safely to a coding agent (Codex).

**Deliverables**
- `README.md` outline (problem, architecture diagram placeholder, quickstart placeholder)
- `docs/` initialized with:
  - `roadmap.md` (this document)
  - `problem_statement.md`
  - `architecture.md` (high-level only)
  - `data_model.md` (table list + key columns)
  - `workflows.md` (task DAG definitions)
  - `security_and_pii.md` (PII boundary + redaction policy)
  - `testing_strategy.md` (smoke + data quality checks)
- `taxonomies/` with initial YAML stubs:
  - `contact_drivers.yml`, `issues.yml`, `intents.yml`, `emotions.yml`
- `.env.example` and `.gitignore` (no secrets, no audio by default)

**Acceptance Criteria**
- A reviewer can understand the whole project from docs alone.
- Taxonomy files define labels + descriptions + examples (even if minimal).
- Repo contains no tokens, no private audio, no private transcripts.

**Risks**
- Over-documentation without progress.
**Mitigation**
- Keep docs concise but complete; implementation milestones start immediately after.

---

### Milestone 1 — Databricks Workspace + Workflow Skeleton (No ML)
**Objective:** Create Databricks Workflows and tables that prove orchestration, status tracking, and end-to-end run control.

**Deliverables**
- Workflow #1 (Foundation) with placeholder tasks:
  1. ingest_audio
  2. preprocess_audio
  3. diarize_audio
  4. transcribe_audio
  5. align_turns
  6. redact_pii
  7. translate_turns
  8. publish_outputs
- Workflow #2 (Insights) with placeholder tasks:
  1. build_metrics
  2. load_taxonomies
  3. build_embeddings
  4. llm_extract_insights
  5. publish_insights
  6. quality_checks

Tables created (empty or minimal):
- `ops_pipeline_runs` (run_id, workflow, start_ts, end_ts, status)
- `ops_file_status` (call_id, stage, status, error_message, updated_ts)

**Acceptance Criteria**
- Both workflows can run end-to-end and complete successfully.
- Status tables are updated per run and per stage.
- All tasks are parameterized at least with `catalog`, `schema`, and `input_path`.

**Risks**
- CLI/Workspace synchronization friction.
**Mitigation**
- Use minimal artifacts first (single notebook per task) and avoid complexity.

---

### Milestone 2 — Bronze Audio Ingestion + Manifest
**Objective:** Ingest audio into governed storage and maintain a manifest that drives processing.

**Deliverables**
- Bronze storage layout (Volumes):
  - `.../bronze/audio_raw/`
- `bronze_audio_files` table:
  - call_id, file_path, ingested_at, file_hash, duration_sec, sample_rate, channels
  - language_hint, source_type
  - status (NEW/PROCESSED/FAILED), error_message
- Ingestion logic supports:
  - new file discovery
  - de-duplication by file_hash
  - incremental processing based on status

**Acceptance Criteria**
- Adding a new `.wav` file results in a new row with status `NEW`.
- Re-running ingestion does not duplicate existing entries.

**Risks**
- Inconsistent filenames, language metadata.
**Mitigation**
- Standardize `call_id` generation rules and allow a language_hint override map.

---

### Milestone 3 — ASR Baseline (Whisper) → `silver_asr_segments`
**Objective:** Produce stable transcriptions for a small multilingual sample set.

**Deliverables**
- `silver_asr_segments`:
  - call_id, asr_segment_id, start_sec, end_sec, text
  - language_detected, model_name, compute_type, run_id
- Update file status:
  - stage `transcribe_audio`: success/fail

**Acceptance Criteria**
- For 3–5 sample calls across languages, transcription completes reliably.
- ASR outputs are queryable and time-aligned (start/end exist).

**Risks**
- Runtime limits, model download time.
**Mitigation**
- Cache model artifacts; start with smaller model for initial milestone if necessary and document later upgrade.

---

### Milestone 4 — Diarization + Alignment → `silver_turns_aligned`
**Objective:** Attribute text to speakers and produce turn-level conversation structure.

**Deliverables**
- `silver_diarization_segments`:
  - call_id, segment_id, speaker_label, start_sec, end_sec, method, confidence, run_id
- `silver_turns_aligned`:
  - call_id, turn_id, speaker_label, role, start_sec, end_sec, text_original, language_final, run_id
- Role assignment heuristic (v1):
  - configurable baseline; output role can be Agent/Customer/Unknown

**Acceptance Criteria**
- At least 2 multi-speaker calls produce plausible speaker separation.
- Alignment yields turns with speaker labels and text.
- `method` indicates whether pyannote or fallback was used.

**Risks**
- pyannote dependency/token management.
**Mitigation**
- Support fallback VAD segmentation; keep diarization primary optional.

---

### Milestone 5 — PII Detection + Redaction (Presidio + Rules)
**Objective:** Enforce compliance-safe outputs.

**Deliverables**
- `gold_turns_redacted`:
  - call_id, turn_id, role, start_sec, end_sec
  - text_redacted
  - pii_found_flag, pii_entities, pii_entity_counts
  - pii_residual_risk_flag, redaction_version, run_id
- Redaction policy:
  - Default downstream text is redacted text.
  - Raw text access restricted (documented; actual enforcement depends on UC permissions).

**Acceptance Criteria**
- PII placeholders appear in redacted output when PII is present in sample transcripts.
- Post-redaction residual scan can flag risky cases.

**Risks**
- Multilingual PII detection accuracy.
**Mitigation**
- Layer regex rules; treat LLM as secondary compliance detector only.

---

### Milestone 6 — Translation (Default EN, Configurable Target, Skip Rule)
**Objective:** Provide translated transcripts as analytics standard.

**Deliverables**
- `gold_turns_translated`:
  - includes redacted text + translated text
  - translation_target_language (default `en`)
  - translation_skipped_flag
  - translation_model, run_id
- Translation logic:
  - if `language_final == target_language` then skip

**Acceptance Criteria**
- For English calls: translation is skipped.
- For non-English calls: translated text exists and is stored.

**Risks**
- Translation model size/latency.
**Mitigation**
- Allow translation to be disabled; document compute requirements.

---

### Milestone 7 — Metrics Layer (No LLM)
**Objective:** Produce speech analytics features from segments and turns.

**Deliverables**
- `gold_conversation_metrics`:
  - total duration, role talk times, silence time, overlap time
  - turns per role, average turn length
  - optional interruption counts
  - run_id

**Acceptance Criteria**
- Metrics are internally consistent (talk time <= duration, etc.).
- Data quality rules are applied and reported.

**Risks**
- Complex overlap calculations.
**Mitigation**
- Start with conservative estimates; document assumptions; refine later.

---

### Milestone 8 — Vector Retrieval (RAG) Foundation
**Objective:** Improve LLM accuracy by providing relevant retrieved context.

**Deliverables**
- `silver_text_chunks`:
  - call_id, chunk_id, start_sec, end_sec, chunk_text (redacted/translated)
- `silver_embeddings`:
  - call_id, chunk_id, embedding_vector, embedding_model, run_id
- Retrieval strategy:
  - retrieve top-k similar chunks and top-k taxonomy examples per label (configurable)

**Acceptance Criteria**
- Retrieval returns relevant chunks for a query and can be used to augment prompts.

**Risks**
- Vector search availability on free tier.
**Mitigation**
- Store embeddings in Delta; document vector backend as pluggable.

---

### Milestone 9 — LLM Insights Output (Taxonomy + Structured Extraction)
**Objective:** Generate structured speech insights in a controlled, auditable way.

**Deliverables**
- Taxonomy tables or loaded YAML mirrored into Delta:
  - `dim_contact_driver`, `dim_issue`, `dim_intent`, `dim_emotion_catalog`
- `gold_speech_insights`:
  - call_id
  - summary
  - contact_driver_label, issue_label, intent_label (+ confidences)
  - resolution, effort, sentiment (+ confidences)
  - customer_emotion_start/end, agent_emotion_start/end (+ polarity scores)
  - agent_love_score_1_10, brand_love_score_1_10
  - model_name, prompt_version, taxonomy_version, run_id
- Guardrails:
  - outputs must belong to allowed labels
  - include confidence fields
  - store prompt and taxonomy versions for reproducibility

**Acceptance Criteria**
- Outputs are valid against a schema contract.
- Labels are always chosen from active taxonomies.
- Runs are reproducible (same input → same schema, with versioned metadata).

**Risks**
- LLM hallucinations and inconsistent labels.
**Mitigation**
- Use RAG + strict label constraints + schema validation.

---

## 7. Work Breakdown Guidance for Implementation Agents (Codex)

The implementation must be executed in small, verifiable units:
- one milestone at a time
- within each milestone, one task per notebook/stage
- every task must specify:
  - inputs
  - outputs
  - tables affected
  - acceptance checks

A task is considered complete only if:
- workflow runs for at least one sample call
- output tables contain expected rows
- status tables are updated
- data quality checks pass or are explained/documented

---

## 8. Risk Register (Summary)

- **Resource limitations (free tier)**: mitigate by small datasets, caching, optional features toggles.
- **Secrets management**: mitigate by secret scopes and `.env.example`.
- **Multilingual PII coverage**: mitigate by layered rules and residual risk flags.
- **LLM variability**: mitigate by taxonomies + RAG + schema constraints.
- **Diarization quality**: mitigate by fallback segmentation and documenting known limitations.

---

## 9. Definition of Done (Project Level)

The project is considered “complete” when:
- Foundation pipeline runs end-to-end producing:
  - redacted turns (and translated turns when enabled)
  - conversation metrics
- Insights pipeline runs end-to-end producing:
  - taxonomy-driven labels
  - summary + outcomes + emotions + love scores
  - versioned metadata (model/prompt/taxonomy)
- Documentation explains:
  - architecture
  - data model
  - how to run workflows
  - security/PII approach
  - testing strategy
- Repo contains sample non-sensitive inputs and/or clear instructions to generate synthetic calls.
