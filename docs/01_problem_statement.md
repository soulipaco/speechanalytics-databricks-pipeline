# Problem Statement — Speech Analytics Lakehouse on Databricks

## 1. Context

Call centers generate large volumes of audio that contain valuable operational and customer-experience signals. However, raw audio is not directly usable for analytics, monitoring, or business intelligence. Organizations typically need structured conversation data (speaker-attributed turns, timestamps, language normalization, and compliance-safe text) before meaningful metrics and insights can be produced.

This project implements an end-to-end Speech Analytics pipeline on Databricks, transforming multilingual `.wav` call recordings into governed, analytics-ready Delta tables. The pipeline also supports an LLM-based insights layer with controlled taxonomies and vector retrieval (RAG) to improve consistency and accuracy for long, complex conversations.

The solution is designed as a portfolio-grade reference architecture that follows Lakehouse best practices (Bronze/Silver/Gold) and emphasizes reproducibility, auditability, and compliance boundaries.

---

## 2. Problem

Raw call audio has several challenges that prevent direct analytics:

### 2.1 Lack of Structure
Audio must be converted into:
- time-aligned transcript segments (ASR)
- speaker segments (diarization)
- turn-level conversation structure (aligned diarization + ASR)
- role mapping (Agent vs Customer)

### 2.2 Multilingual Complexity
Calls may occur across multiple languages. Analytics requires:
- reliable language attribution per call/turn
- optional translation to a standardized target language (default English)
- consistent behavior when translation target equals the original language (skip rule)

### 2.3 Compliance and PII Risk
Call transcripts often contain personally identifiable information (PII). To be safe for analysis and for downstream LLM usage, the pipeline must:
- detect PII using Microsoft Presidio where possible
- apply anonymization/redaction policies
- provide residual-risk checks to flag potential misses
- ensure redacted outputs are the default “analytics product”

### 2.4 Long and Complex Conversations
Even after transcription, long calls are difficult for AI systems to process reliably. LLMs may struggle with:
- long-context limitations
- category consistency across large datasets
- hallucinations and non-deterministic outputs

To mitigate this, the project requires:
- chunking strategies (turn-based or time window based)
- retrieval of relevant context using embeddings and similarity search (RAG)
- taxonomy-driven classification to constrain outputs

---

## 3. Objectives

### 3.1 Functional Objectives (What the system must do)
1. Ingest `.wav` files into governed storage (Unity Catalog Volumes).
2. Maintain a Bronze manifest to track audio files, metadata, and processing status.
3. Standardize audio preprocessing when required (optional stage).
4. Perform diarization (pyannote as preferred, fallback segmentation supported).
5. Perform transcription (Whisper).
6. Align diarization and ASR outputs into turn-level conversation data.
7. Detect and redact PII (Presidio + rule-based protections).
8. Translate transcripts into a target language (default English) with a skip rule:
   - if source language equals target language, translation is skipped.
9. Produce speech metrics without LLM (talk time, silence time, overlap, turns).
10. Generate LLM-driven insights using controlled taxonomies:
   - Contact Driver
   - Issue
   - Intent
   - Summary
   - Resolution, Effort, Sentiment
   - Emotion timeline (start/end) for Agent and Customer
   - Agent Love Score and Brand Love Score
11. Support vector retrieval (RAG) to improve insight extraction accuracy.

### 3.2 Non-Functional Objectives (How it must behave)
- Modular and orchestrated with Databricks Workflows.
- Auditable outputs: model versions, prompt versions, taxonomy versions stored per run.
- Incremental execution: new files processed without duplicating old ones.
- Failure isolation: one bad file should not break a full batch run.
- Reproducible: results can be traced to the exact pipeline version and configuration.

---

## 4. Scope

### 4.1 In-Scope (v1)
- Batch processing of sample multilingual calls (public or synthetic).
- Bronze/Silver/Gold Delta tables as primary data products.
- Presidio-based redaction and residual risk checks.
- Translation to target language with skip logic.
- Deterministic, non-LLM metrics.
- LLM insights with taxonomy constraints and RAG augmentation.

### 4.2 Out-of-Scope (v1)
- Real-time streaming ingestion.
- Fully automated role detection beyond baseline heuristics.
- Enterprise-grade authentication/authorization integration beyond Databricks UC controls.
- Production SLAs and large-scale performance tuning.

---

## 5. Stakeholders and Users (Portfolio Context)

This project is designed to demonstrate capabilities relevant to:
- BI and Analytics stakeholders (metrics, dashboards, distribution shifts)
- Data Engineering reviewers (Bronze/Silver/Gold design, orchestration, governance)
- Applied ML/AI reviewers (ASR, diarization, PII, RAG, structured LLM extraction)
- Compliance and operational stakeholders (PII handling, traceability, safety boundaries)

---

## 6. Key Design Decisions (Problem → Approach)

### 6.1 Lakehouse Layers
- Bronze stores raw audio and ingestion metadata.
- Silver stores diarization segments, ASR segments, aligned turns.
- Gold stores redacted (and optionally translated) turns, metrics, and insights.

### 6.2 Language Handling
- Store three language fields:
  - language_hint
  - language_detected
  - language_final
- Preserve both hint and detection for auditability and corrections.

### 6.3 Compliance Boundary
- Redacted text becomes the default input for analytics and LLM.
- Raw text is treated as restricted and optional for portfolio usage.

### 6.4 LLM Reliability
- Taxonomies constrain outputs.
- Vector retrieval provides relevant context and label examples.
- Structured output contracts prevent free-form hallucinated categories.

---

## 7. Success Criteria

The project is considered successful when:
- The foundation pipeline produces `gold_turns_redacted` (and translated turns when enabled) for a multilingual sample dataset.
- The insights pipeline produces `gold_speech_insights` with taxonomy-driven labels and required fields.
- Every run stores traceable metadata (model versions, prompt version, taxonomy version).
- Documentation fully describes architecture, data model, workflows, and testing.

---

## 8. Risks and Mitigations (Problem-Oriented)

- **Limited compute in free tier**: mitigate by small sample set, caching, optional stage toggles.
- **Multilingual PII detection gaps**: mitigate by layered regex rules and residual risk flags.
- **Diarization quality variance**: mitigate by fallback segmentation and documenting limitations.
- **LLM output inconsistency**: mitigate by taxonomies, RAG, schema validation, confidence scoring.

---
