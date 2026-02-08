# How To Run (First Databricks Execution)

This guide is for the first workspace execution with a dry-run mindset.

Scope:
- Run order and parameters for a safe smoke test
- No external-model dependencies required
- Focus on validating pipeline wiring, contracts, and ops tracking

## Prerequisites

### 1) Databricks CLI auth
High-level setup:
1. Install Databricks CLI.
2. Configure auth profile (PAT or OAuth).
3. Verify workspace access (`databricks workspace ls` style check).

Do not hardcode secrets in notebooks or repo files.

### 2) Unity Catalog catalog/schema
Expected objects:
- One writable catalog and schema for pipeline tables.
- Principal needs:
  - `USE CATALOG`, `USE SCHEMA`
  - `CREATE TABLE`, `SELECT`, `MODIFY`, `CREATE VIEW` (optional for latest views)

### 3) Volume / DBFS paths and permissions
`foundation_01_ingest_audio.py` requires a readable `volume_root` with WAV files.

Expected:
- Input path under `<volume_root>/bronze/audio_raw/`
- Caller can list/read files in that location.

### 4) Cluster/runtime expectations
Baseline:
- Databricks runtime with Python + Spark
- Sufficient driver memory for first-run collect-heavy stages

Optional libraries (for extended stages; not required for minimal smoke test):
- `pyyaml` (Insights 02 taxonomy load)
- `requests` (HTTP backend paths)
- `mlflow.deployments` (endpoint inference paths)
- `pyannote` (Foundation 03 advanced diarization)
- `faster-whisper` or `openai-whisper` (Foundation 04 ASR backends)
- `databricks.vector_search` (Insights 05/06 RAG vector-search paths)

## Minimal Smoke Test (No External Services)

Run only these stages:

### Foundation path
1. `foundation_01_ingest_audio`
2. `foundation_02_preprocess_audio` (disabled behavior)
3. `foundation_03_diarize_audio` (disabled or fallback behavior)
4. `foundation_04_transcribe_audio`
5. `foundation_05_align_turns`
6. `foundation_06_redact_pii`
7. `foundation_08_publish_and_finalize`

Do not run Foundation 07 for first smoke test.

### Insights path
1. `insights_01_compute_conversation_metrics`
2. `insights_02_load_taxonomies_to_dim_tables`
3. `insights_03_build_text_chunks`
4. `insights_08_quality_gates_and_finalize`

Do not run Insights 04/05/06/07 for first smoke test.

## Smoke-Test Widget Defaults

Use these baseline values in all executed notebooks:

| Widget | Recommended smoke-test value | Notes |
|---|---|---|
| `catalog` | `<your_catalog>` | Required everywhere |
| `schema` | `<your_schema>` | Required everywhere |
| `run_id` | `smoke_YYYYMMDD_HHMMSS` | Unique per run |
| `run_mode` | `sample` | Safer first run |
| `max_files_per_run` | `5` | Keep initial blast radius low |
| `volume_root` | `<readable_volume_root>` | Required by Foundation 01/02/03 |
| `metrics_version` | `v1` | Insights 01/07/08 |
| `consolidation_version` | `v1` | Insights 07/08 |

Kill-switch values for smoke test:
- Foundation:
  - `enable_preprocess_audio=false`
  - `enable_diarization=false` (or keep fallback on)
  - `enable_translation=false` (when running Foundation 08)
- Insights:
  - `translation_enabled=false` (Insights 01/03)
  - `enable_embeddings=false` (if Insights 04 is tested separately)
  - `enable_rag_index=false` (Insights 05)
  - `enable_llm=false` (Insights 06)
  - `enable_llm_consolidation=false` (Insights 07)
  - `enable_quality_gates=true`, `allow_warn=true` (Insights 08)

## Run ID Strategy

Recommended format:
- `smoke_YYYYMMDD_HHMMSS`
- `dev_<feature>_YYYYMMDD_HHMMSS`
- `prod_YYYYMMDD_HHMMSS`

Why:
- Easy to trace in ops tables.
- Avoids collisions across repeated tests.

Ops tracking:
- `ops_pipeline_runs` keyed by `run_id + workflow_name`.
- `ops_file_status` tracks stage status per `call_id + stage_name`, with latest `run_id`.

## Troubleshooting (Common First-Run Issues)

1. Missing table errors
- Symptom: `Required input table is missing ...`
- Mitigation: verify stage order and run prerequisites first.

2. Sample mode validation failures
- Symptom: `max_files_per_run must be > 0`
- Mitigation: set `run_mode=sample` and `max_files_per_run` explicitly.

3. Volume path issues (Foundation 01)
- Symptom: no discovered files or file-read errors.
- Mitigation: confirm `volume_root` and workspace permissions.

4. Optional backend not available
- Symptom: endpoint/vector/LLM backend initialization errors.
- Mitigation: keep kill-switches off for smoke test; enable advanced stages later.

5. Taxonomy validation errors (Insights 02/08)
- Symptom: invalid/missing taxonomy labels.
- Mitigation: run Insights 02 before quality/finalize; verify taxonomy YAML structure.

## Safety

- No raw transcript text should be persisted in final insights outputs.
- Downstream LLM paths use redacted text and hashed chunk references (`chunk_text_hash`).
- Keep secrets out of code and notebooks; pass via workspace secret mechanisms or runtime env.
