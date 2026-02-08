# Security and Privacy Note

## Purpose
This document summarizes practical security/privacy controls for the SpeechAnalytics demo pipeline. It is a technical implementation note, not a legal/compliance attestation.

## Data Classes
1. Audio (`.wav`): source call recordings.
2. Transcript text: model-generated spoken content (pre-redaction).
3. Redacted text: PII-masked turn text for downstream analytics.
4. Derived metrics: non-LLM aggregates (duration, talk time, silence, counts).
5. Embeddings: vectorized chunk representations for retrieval.
6. Insights: structured call-level outcomes, labels, and summaries.

## Storage by Layer
### Bronze
- Raw ingestion metadata and audio file manifest (`bronze_audio_files`).
- Contains source-location metadata and processing status.

### Silver
- Intermediate artifacts:
  - diarization/asr/aligned turns
  - `silver_text_chunks`
  - `silver_embeddings`
  - `silver_llm_chunk_insights` (optional)
- May include redacted chunk text in intermediate tables where required for processing.

### Gold
- Curated outputs:
  - `gold_turns_redacted`
  - `gold_turns_translated` (optional)
  - `gold_conversation_metrics`
  - `gold_speech_insights`

## Explicit Transcript Safety Boundary
- `gold_speech_insights` is designed to avoid raw transcript persistence.
- Chunk-level LLM extraction stores `chunk_text_hash` in `silver_llm_chunk_insights`, not raw chunk text.
- `insights_08_quality_gates_and_finalize` enforces a guard that fails/warns if transcript-like/raw text columns appear in `gold_speech_insights`.

## Threats and Mitigations
### 1) Secret leakage
Threat:
- API keys/tokens accidentally committed to git or written to logs.

Mitigations:
- `.gitignore` excludes `.env*` (except `.env.example`) and key/cert files.
- Keep `.env.example` placeholders only.
- Use Databricks secret scopes for runtime credentials.
- Avoid printing secret-bearing config values.

### 2) PII leakage
Threat:
- Unredacted text or sensitive fields move into analytics outputs.

Mitigations:
- Foundation redaction stage produces `gold_turns_redacted`.
- Insights stages are intended to consume redacted/translated safe surfaces.
- Gold insights table avoids transcript/raw-text fields.
- Final quality gates include raw-text column checks.

### 3) Re-identification risk from embeddings
Threat:
- Embeddings can leak semantic signals and may enable membership inference in weakly controlled environments.

Mitigations:
- Treat embeddings as sensitive derived data.
- Restrict access with Unity Catalog permissions.
- Limit index exposure and downstream export scope.
- Apply retention and access-review policies.

### 4) Prompt injection / taxonomy spoofing
Threat:
- Malicious input or model drift generates labels outside controlled taxonomy.

Mitigations:
- Taxonomy-constrained validation against dim tables.
- Strict JSON parsing/validation in LLM stages.
- Reject/fail outputs with invalid labels or schema mismatch.

### 5) Access control and governance gaps
Threat:
- Over-broad table permissions expose data beyond intended personas.

Mitigations:
- Use Unity Catalog grants at catalog/schema/table/view levels.
- Separate dev/test/prod catalogs and service principals.
- Prefer curated views for consumer access.

## Operational Controls
- `ops_file_status`: per-call/per-stage traceability with error context.
- `ops_pipeline_runs`: run-level status and counts (`RUNNING/SUCCESS/WARN/FAILED`).
- Idempotent rerun patterns reduce manual backfill risk and improve reproducibility.

Recommended secret handling:
1. Store credentials only in Databricks secret scopes.
2. Resolve secrets at runtime, not from committed files.
3. Rotate credentials and avoid broad-scope tokens.

## Non-goals
- This repository is not a formal SOC2/ISO/HIPAA certification artifact.
- This document is not legal advice.
- Threat coverage is practical and implementation-oriented, not exhaustive.
