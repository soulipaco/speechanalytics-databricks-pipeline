# Speech Analytics Lakehouse (Databricks, Contract-First)

## What this repo is
This repository contains a Databricks-native speech analytics pipeline implemented as notebook-source Python scripts.

Design principles:
- Contract-first tables (`docs/03_data_model.md`)
- Idempotent reruns (`sample | incremental | full`)
- Per-call failure isolation
- Operational auditability via `ops_file_status` and `ops_pipeline_runs`

## Demo / Portfolio Mode
- No real customer data is included in this repository.
- Notebooks are notebook-source scripts; execution requires a Databricks workspace with Unity Catalog (`catalog`/`schema`) and a readable Volume containing WAV files.
- Run instructions are in `docs/13_how_to_run.md`.
- Security posture: no raw transcript text is persisted in gold outputs; downstream insights use hashed references (for example `chunk_text_hash`) plus structured summaries.

## 60-second tour
- Foundation 01-08 converts raw audio into aligned, redacted, analytics-safe turns with run-level traceability.
- Insights 01-08 builds metrics, chunking/embedding/RAG assets, and consolidated call-level speech insights.
- The pipeline is contract-first: data model, workflow stages, and ops semantics are explicitly documented and versioned.

Best entry points:
1. `docs/02_architecture.md`
2. `docs/03_data_model.md`
3. `docs/04_workflows.md`
4. `docs/13_how_to_run.md`
5. `docs/14_github_readiness_audit.md`

If you only read one notebook: `notebooks/insights/insights_07_llm_consolidate_call_insights.py`, because it shows taxonomy-constrained call-level synthesis and final gold output shaping in one stage.

## Quickstart (static-only)
This repo is currently used for script/docs generation and static validation.

1. Review architecture and contracts:
   - `docs/02_architecture.md`
   - `docs/03_data_model.md`
   - `docs/04_workflows.md`
2. Review notebook implementations:
   - `notebooks/foundation/`
   - `notebooks/insights/`
3. Run static compile checks locally:
   - `python -m py_compile notebooks/foundation/*.py`
   - `python -m py_compile notebooks/insights/*.py`

Note: No Databricks jobs are executed in this repo workflow yet. Scripts only.

## How to run
First-run execution guidance is documented in:
- `docs/13_how_to_run.md`

## Quick start smoke test (no external services)
Run these stages in order:

Foundation:
1. `foundation_01_ingest_audio`
2. `foundation_02_preprocess_audio`
3. `foundation_03_diarize_audio`
4. `foundation_04_transcribe_audio`
5. `foundation_05_align_turns`
6. `foundation_06_redact_pii`
7. `foundation_08_publish_and_finalize`

Insights:
1. `insights_01_compute_conversation_metrics`
2. `insights_02_load_taxonomies_to_dim_tables`
3. `insights_03_build_text_chunks`
4. `insights_08_quality_gates_and_finalize`

Recommended smoke-test kill switches:
- `enable_preprocess_audio=false`
- `enable_diarization=false` (or keep fallback enabled)
- `enable_translation=false`
- `translation_enabled=false`
- `enable_embeddings=false`
- `enable_rag_index=false`
- `enable_llm=false`
- `enable_llm_consolidation=false`
- `enable_quality_gates=true` and `allow_warn=true`

## Pipeline overview

### Foundation 01–08 (implemented)
1. `foundation_01_ingest_audio.py`
2. `foundation_02_preprocess_audio.py`
3. `foundation_03_diarize_audio.py`
4. `foundation_04_transcribe_audio.py`
5. `foundation_05_align_turns.py`
6. `foundation_06_redact_pii.py`
7. `foundation_07_translate_turns.py`
8. `foundation_08_publish_and_finalize.py`

### Insights 01–08 (implemented)
1. `insights_01_compute_conversation_metrics.py`
2. `insights_02_load_taxonomies_to_dim_tables.py`
3. `insights_03_build_text_chunks.py`
4. `insights_04_embed_chunks.py`
5. `insights_05_build_vector_search_rag_index.py`
6. `insights_06_llm_extract_chunk_insights.py`
7. `insights_07_llm_consolidate_call_insights.py`
8. `insights_08_quality_gates_and_finalize.py`

## Data products
- `gold_turns_redacted`
- `gold_turns_translated` (optional)
- `gold_conversation_metrics`
- `gold_speech_insights`

## Safety policy
- No raw transcript text is intended in downstream Insights outputs.
- LLM chunk extraction uses hashed chunk references (`chunk_text_hash`) and controlled fields.
- Redacted text is the compliance-safe default surface.
- Never commit secrets or private credentials.

## Where to look
- Architecture: `docs/02_architecture.md`
- Data model contracts: `docs/03_data_model.md`
- Workflow/DAG and stage semantics: `docs/04_workflows.md`
- Taxonomies: `taxonomies/contact_drivers.yml`, `taxonomies/issues.yml`, `taxonomies/intents.yml`, `taxonomies/emotions.yml`

## Known issues
- No blocker found in the latest static audit for first Databricks run.
- Non-blocking warning: `notebooks/insights/insights_06_llm_extract_chunk_insights.py` uses `DELETE + INSERT` for `ops_pipeline_runs` where most notebooks use `MERGE`.
