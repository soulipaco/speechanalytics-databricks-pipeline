# GitHub Readiness Audit

Date: 2026-02-08  
Mode: Static checks only (no Databricks auth, no Databricks job runs)

## Scope
- `README.md`
- `docs/**/*.md`
- `notebooks/foundation/*.py`
- `notebooks/insights/*.py`
- `taxonomies/*.yml`
- `configs/*`
- `workflows/*.json`
- `.env.example`
- `.gitignore`

## Repo Overview Snapshot

### Top-level tree
- `assets/`
- `configs/`
- `docs/`
- `legacy_private/`
- `notebooks/`
- `taxonomies/`
- `workflows/`
- `.env.example`
- `.gitignore`
- `CHANGELOG.md`
- `CONTRIBUTING.md`
- `LICENSE`
- `README.md`

### Foundation notebooks
1. `notebooks/foundation/foundation_01_ingest_audio.py`
2. `notebooks/foundation/foundation_02_preprocess_audio.py`
3. `notebooks/foundation/foundation_03_diarize_audio.py`
4. `notebooks/foundation/foundation_04_transcribe_audio.py`
5. `notebooks/foundation/foundation_05_align_turns.py`
6. `notebooks/foundation/foundation_06_redact_pii.py`
7. `notebooks/foundation/foundation_07_translate_turns.py`
8. `notebooks/foundation/foundation_08_publish_and_finalize.py`

### Insights notebooks
1. `notebooks/insights/insights_01_compute_conversation_metrics.py`
2. `notebooks/insights/insights_02_load_taxonomies_to_dim_tables.py`
3. `notebooks/insights/insights_03_build_text_chunks.py`
4. `notebooks/insights/insights_04_embed_chunks.py`
5. `notebooks/insights/insights_05_build_vector_search_rag_index.py`
6. `notebooks/insights/insights_06_llm_extract_chunk_insights.py`
7. `notebooks/insights/insights_07_llm_consolidate_call_insights.py`
8. `notebooks/insights/insights_08_quality_gates_and_finalize.py`

---

## Static Validation Results

### 1) Python syntax compile
Command:
- `python -m py_compile` on all `notebooks/foundation/*.py` and `notebooks/insights/*.py`

Result:
- PASS (`PY_COMPILE_OK:16`)

### 2) Secret pattern scan (outside `legacy_private/`)
Patterns checked:
- `TODO: secret`
- `sk-...`
- `hf_...`
- `AKIA...`
- `Bearer ...`

Result:
- PASS (no matches)

### 3) Transcript safety check
Result:
- PASS for `gold_speech_insights`: no raw transcript/chunk text fields persisted in output table schema.
  - Evidence: `notebooks/insights/insights_07_llm_consolidate_call_insights.py:143` (DDL starts)
  - Evidence: `notebooks/insights/insights_07_llm_consolidate_call_insights.py:182` (`llm_prompt_version` present)
  - Evidence: `notebooks/insights/insights_07_llm_consolidate_call_insights.py:1063` (write schema)
- PASS: explicit guard in Insights 08 rejects transcript-like columns in gold table.
  - Evidence: `notebooks/insights/insights_08_quality_gates_and_finalize.py:573`
  - Evidence: `notebooks/insights/insights_08_quality_gates_and_finalize.py:590`
- PASS: Insights 06 persists `chunk_text_hash` (not raw chunk text) in output.
  - Evidence: `notebooks/insights/insights_06_llm_extract_chunk_insights.py:106`
  - Evidence: `notebooks/insights/insights_06_llm_extract_chunk_insights.py:791`

### 4) Controlled enums + stage/workflow consistency
Result:
- PASS: `ops_pipeline_runs` status enum supports `RUNNING/SUCCESS/WARN/FAILED` in model and code.
  - Evidence: `docs/03_data_model.md:53`
  - Evidence: `notebooks/foundation/foundation_08_publish_and_finalize.py:547`
  - Evidence: `notebooks/insights/insights_08_quality_gates_and_finalize.py:808`
- PASS: `ops_file_status` enum now aligned to `SUCCESS/WARN/FAILED/SKIPPED`.
  - Evidence: `docs/03_data_model.md:93`
  - Evidence: `notebooks/insights/insights_08_quality_gates_and_finalize.py:714`
  - Evidence: `notebooks/insights/insights_08_quality_gates_and_finalize.py:734`
- PASS: canonical `STAGE_NAME` constants match expected 01-08 foundation + 01-08 insights.
  - Evidence: `notebooks/foundation/foundation_01_ingest_audio.py:37`
  - Evidence: `notebooks/foundation/foundation_08_publish_and_finalize.py:37`
  - Evidence: `notebooks/insights/insights_01_compute_conversation_metrics.py:37`
  - Evidence: `notebooks/insights/insights_08_quality_gates_and_finalize.py:20`
- PASS: workflow names are correctly split.
  - Evidence: `notebooks/foundation/foundation_01_ingest_audio.py:36`
  - Evidence: `notebooks/insights/insights_01_compute_conversation_metrics.py:36`

### 5) Table contract alignment (docs vs notebook DDL/write)
Result summary:
- PASS (aligned):
  - `gold_conversation_metrics`
  - `silver_llm_chunk_insights`
  - `gold_speech_insights` (after docs fix in this audit)
  - `ops_file_status`
- WARN (under-documented extras in docs key columns; code has richer schema):
  - `silver_text_chunks`
  - `silver_embeddings`
  - `ops_pipeline_runs` (code includes operational counters and `updated_at`, now documented as key columns but some docs describe minimal subset)

Evidence (table DDLs):
- `notebooks/insights/insights_01_compute_conversation_metrics.py:102`
- `notebooks/insights/insights_03_build_text_chunks.py:181`
- `notebooks/insights/insights_04_embed_chunks.py:95`
- `notebooks/insights/insights_06_llm_extract_chunk_insights.py:103`
- `notebooks/insights/insights_07_llm_consolidate_call_insights.py:143`
- `notebooks/foundation/foundation_06_redact_pii.py:151`
- `notebooks/foundation/foundation_07_translate_turns.py:99`

### 6) Workflow docs alignment (`docs/04_workflows.md`)
Result:
- PASS: task headings match canonical task names.
  - Evidence: `docs/04_workflows.md:122` ... `docs/04_workflows.md:464`
  - Automated check: 16 headings found, 16 canonical matches, 0 missing, 0 extra.

### 7) Taxonomy loader readiness
Checks:
- YAML parse with `yaml.safe_load`
- Supported shape (`items` list)
- Required keys by taxonomy type
- Case-insensitive label uniqueness within file

Result:
- PASS:
  - `taxonomies/contact_drivers.yml` (9 items, 0 errors)
  - `taxonomies/issues.yml` (15 items, 0 errors)
  - `taxonomies/intents.yml` (13 items, 0 errors)
  - `taxonomies/emotions.yml` (10 items, 0 errors)

### 8) Workflows JSON readiness
Checks:
- JSON parse success
- Task graph present (16 tasks each)
- Notebook path placeholders detected

Result:
- PASS: both JSON files parse.
  - `workflows/smoke_test_job.json` (16 tasks)
  - `workflows/full_job.json` (16 tasks)
- WARN: notebook paths intentionally include `<user>` placeholders.
  - Evidence: `workflows/smoke_test_job.json:20`
  - Evidence: `workflows/full_job.json:20`
- WARN: `foundation_01_ingest_audio` requires `volume_root`, but workflow task base parameters do not provide it.
  - Evidence (required in notebook): `notebooks/foundation/foundation_01_ingest_audio.py:426`
  - Evidence (enforced required): `notebooks/foundation/foundation_01_ingest_audio.py:458`
  - Evidence (missing in workflow task params): `workflows/smoke_test_job.json:21`
  - Evidence (missing in workflow task params): `workflows/full_job.json:21`

---

## PASS
1. All 16 notebook-source Python files compile successfully.
2. No secret/token signatures found outside `legacy_private/` for required patterns.
3. Stage IDs and workflow names are canonical and consistent across all notebooks.
4. 06?07?08 table chain is consistent in code:
   - `silver_llm_chunk_insights` ? `gold_speech_insights` + `gold_conversation_metrics`.
5. Dimension table names are consistent (`dim_contact_driver`, `dim_issue`, `dim_intent`, `dim_emotion_catalog`).
6. Taxonomy YAMLs are parseable and satisfy loader-required keys/uniqueness checks.
7. Workflows JSON files are syntactically valid.
8. Transcript safety guardrails are present and enforced for final gold insights table.
9. `.env.example` contains placeholders only (no secrets).
10. `.gitignore` excludes `.env*`, key material, data/audio artifacts.

## WARNINGS (non-blocking)
1. `workflows/*.json` are templates and still require environment-specific path replacement (`<user>`).
2. `workflows/*.json` omit `volume_root` for `foundation_01_ingest_audio`; jobs will fail if launched without overrides.
3. `silver_text_chunks` and `silver_embeddings` have richer implemented schemas than currently emphasized in key-column docs.
4. `insights_06_llm_extract_chunk_insights.py` uses `DELETE + INSERT` for `ops_pipeline_runs` instead of MERGE (behaviorally acceptable but inconsistent with the rest).
   - Evidence: `notebooks/insights/insights_06_llm_extract_chunk_insights.py:168`
   - Evidence: `notebooks/insights/insights_06_llm_extract_chunk_insights.py:192`

## BLOCKERS
- None for GitHub publication quality after docs alignment in this audit.

---

## Docs-only Fixes Applied During This Audit

1. `docs/03_data_model.md`
- Updated `ops_file_status.status` enum to `SUCCESS / WARN / FAILED / SKIPPED`.
- Added missing operational columns to `ops_pipeline_runs` key columns: `total_files`, `success_count`, `failed_count`, `updated_at`.
- Added `updated_at` to `gold_turns_redacted` and `gold_turns_translated` key columns.
- Aligned `gold_speech_insights` grain to versioned output (`call_id + metrics_version + consolidation_version`).
- Replaced `prompt_version` with `llm_prompt_version` and documented implemented provenance/risk/RAG/version fields.

Rationale:
- Remove contract drift between docs and implemented notebook DDL/write schemas.

---

## Exact Fix List (remaining recommendations)

1. `workflows/smoke_test_job.json`
- Change: add `volume_root` in `foundation_01_ingest_audio` base parameters.
- Rationale: required widget; current template will fail at runtime.

2. `workflows/full_job.json`
- Change: add `volume_root` in `foundation_01_ingest_audio` base parameters.
- Rationale: same required parameter constraint.

3. `workflows/smoke_test_job.json`, `workflows/full_job.json`
- Change: replace `/Repos/<user>/...` with actual workspace repo path.
- Rationale: template placeholder must be concrete before execution.

4. `notebooks/insights/insights_06_llm_extract_chunk_insights.py`
- Change: refactor `_upsert_run_running` and `_upsert_run_final` to `MERGE` pattern (match other notebooks).
- Rationale: consistency and lower race-risk for concurrent reruns.

5. `docs/03_data_model.md` (optional enhancement)
- Change: expand key columns for `silver_text_chunks` and `silver_embeddings` to include currently implemented metadata columns.
- Rationale: improve discoverability for reviewers and downstream users.

---

## Publishing Checklist

- [x] All notebook `.py` files compile
- [x] Canonical stage/workflow naming verified
- [x] Sensitive token scan clear (required patterns)
- [x] No raw transcript fields in `gold_speech_insights`
- [x] Taxonomy YAML parse/required keys/uniqueness checks pass
- [x] Workflow JSON files parse
- [x] Data-model contract drift reduced (docs updated)
- [ ] Workflow templates parameterized for real execution (`<user>`, `volume_root`)

---

## Final Decision

**READY TO PUBLISH: YES**

Top 5 remaining actions (recommended, not publication blockers):
1. Set concrete repo path in `workflows/*.json` (replace `<user>`).
2. Add `volume_root` to Foundation 01 workflow task params.
3. Normalize Insights 06 `ops_pipeline_runs` writes to MERGE.
4. Optionally document full `silver_text_chunks`/`silver_embeddings` metadata columns.
5. Keep `.env.example` placeholders only and continue enforcing `.gitignore` secret exclusions.
