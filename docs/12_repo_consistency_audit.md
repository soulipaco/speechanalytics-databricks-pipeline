# Repo Consistency Audit (Static)

Date: 2026-02-08  
Scope: `docs/`, `notebooks/`, `taxonomies/`, `configs/`, root README files  
Execution mode: static only (no Databricks job runs)

## A) PASSED checks

1. Stage-name drift in task docs is resolved to canonical enum/code stage IDs.
   - Evidence:
     - `docs/tasks/insights_01_compute_conversation_metrics.md:17`
     - `docs/tasks/insights_03_build_text_chunks.md:17`
     - `docs/tasks/insights_04_embed_chunks.md:17`
     - `docs/tasks/insights_06_llm_extract_chunk_insights.md:18`
     - `docs/tasks/insights_07_llm_consolidate_call_insights.md:21`
     - Canonical enum reference: `docs/03_data_model.md:81`, `docs/03_data_model.md:88`

2. `gold_conversation_metrics` grain is aligned with versioned contract.
   - Evidence:
     - `docs/tasks/insights_01_compute_conversation_metrics.md:44`
     - `docs/03_data_model.md:278`

3. Formal `silver_llm_chunk_insights` contract exists in data model.
   - Evidence:
     - New section: `docs/03_data_model.md:357`
     - Grain: `docs/03_data_model.md:359`
     - Required column included: `docs/03_data_model.md:382` (`llm_prompt_version`)

4. Prompt-version naming mismatch is resolved docs-side for Insights 06/07 to `llm_prompt_version`.
   - Evidence:
     - `docs/tasks/insights_06_llm_extract_chunk_insights.md:37`
     - `docs/tasks/insights_06_llm_extract_chunk_insights.md:53`
     - `docs/tasks/insights_07_llm_consolidate_call_insights.md:46`
     - `docs/tasks/insights_07_llm_consolidate_call_insights.md:68`

5. `docs/04_workflows.md` insights completion criteria now uses canonical stage IDs.
   - Evidence:
     - `docs/04_workflows.md:524`
     - `docs/04_workflows.md:525`
     - `docs/04_workflows.md:526`
     - `docs/04_workflows.md:527`

6. Roadmap dim naming now uses `dim_emotion_catalog`.
   - Evidence:
     - `docs/00_roadmap.md:322`

7. Missing asset is now present.
   - Evidence:
     - `assets/architecture.png` exists (1200x700 placeholder image)
     - Reference location: `docs/02_architecture.md:234`

8. Legacy stage ID grep verification in `docs/tasks` passes (none found as standalone IDs).
   - Checked patterns:
     - `compute_metrics`, `build_chunks`, `embed_chunks`, `llm_chunk_insights`, `llm_call_insights`

## B) WARNINGS (non-blocking)

1. Task docs still contain legacy parameter naming where code supports canonical + alias:
   - `chunk_version` remains in `docs/tasks/insights_03_build_text_chunks.md`
   - `embedding_model_name` and `chunk_version` remain in `docs/tasks/insights_04_embed_chunks.md`
   - These do not block Insights 07, but can be harmonized later with code-facing canonical terms (`chunking_version`, `embedding_model`).

2. Insights 06 notebook still uses delete+insert run-status update style (not merge-based parity with other notebooks).
   - This is code-behavior consistency debt, not a docs blocker.

## C) BLOCKERS (must fix before Insights 07)

- None remaining.

## D) Exact fixes

### Docs-only safe fixes (applied)

1. Stage-name replacements in task docs:
   - `compute_metrics` -> `insights_01_compute_conversation_metrics`
   - `build_chunks` -> `insights_03_build_text_chunks`
   - `embed_chunks` -> `insights_04_embed_chunks`
   - `llm_chunk_insights` -> `insights_06_llm_extract_chunk_insights`
   - `llm_call_insights` -> `insights_07_llm_consolidate_call_insights`

2. Grain alignment:
   - `docs/tasks/insights_01_compute_conversation_metrics.md`
   - Updated grain to `one row per call_id per metrics_version`.

3. Data model contract addition:
   - Added `silver_llm_chunk_insights` section with purpose, grain, key columns, quality expectations.

4. Prompt field naming alignment:
   - Updated task docs to use `llm_prompt_version` in Insights 06 and Insights 07.

5. Workflow completion criteria alignment:
   - Updated insights-complete criteria to canonical stage IDs.

6. Roadmap naming cleanup:
   - Replaced `dim_emotion` with `dim_emotion_catalog`.

7. Missing asset resolved:
   - Added `assets/architecture.png` placeholder image.

### Code/behavior fixes (not applied)

1. Optional future consistency cleanup:
   - Harmonize task docs for `chunking_version` / `embedding_model` naming to match current canonical notebook outputs.

2. Optional run-metadata consistency cleanup:
   - Align Insights 06 `ops_pipeline_runs` upsert pattern with merge-based style used by other notebooks.

## Changes applied

Modified files:
- `docs/tasks/insights_01_compute_conversation_metrics.md`
- `docs/tasks/insights_03_build_text_chunks.md`
- `docs/tasks/insights_04_embed_chunks.md`
- `docs/tasks/insights_06_llm_extract_chunk_insights.md`
- `docs/tasks/insights_07_llm_consolidate_call_insights.md`
- `docs/03_data_model.md`
- `docs/04_workflows.md`
- `docs/00_roadmap.md`
- `docs/12_repo_consistency_audit.md`

Added files:
- `assets/architecture.png`

## Ready for Insights 07?

YES.
