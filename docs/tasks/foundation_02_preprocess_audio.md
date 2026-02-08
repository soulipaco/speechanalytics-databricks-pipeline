# FOUNDATION_02_PREPROCESS_AUDIO (docs/tasks/foundation_02_preprocess_audio.md)

## Task ID
- **ID**: FOUNDATION_02_PREPROCESS_AUDIO

## Title
Preprocess audio to a standardized format (mono / 16kHz)

## Objective
Optionally standardize input audio to a consistent format (mono, 16kHz) to improve ASR and diarization stability. This stage produces a traceable mapping from raw audio to preprocessed audio and enables downstream stages to choose the best input source.

## Scope
### In Scope
- Select eligible call_ids from `bronze_audio_files`.
- Convert audio to target format (mono, 16kHz) when enabled.
- Write standardized files to a Silver volume location.
- Persist a mapping/metadata row per call_id (input_path → output_path).
- Update `ops_file_status` stage `preprocess_audio`.

### Out of Scope
- ASR, diarization, alignment, redaction, translation.
- Quality scoring of audio (beyond basic validation).

## Inputs
### Data Inputs
- Table: `<catalog>.<schema>.bronze_audio_files`
- Files: `Volumes/<catalog>/<schema>/bronze/audio_raw/*.wav`

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `volume_root` (string, required)
- `run_id` (string, required)
- `run_mode` (string, required): `sample | incremental | full`
- `max_files_per_run` (int, optional)
- `enable_preprocess_audio` (bool, default false)
- `target_sample_rate` (int, default 16000)
- `target_channels` (int, default 1)
- `output_dir` (string, default: `Volumes/<catalog>/<schema>/silver/audio_preprocessed/`)
- `preprocess_version` (string, default `v1`)

## Outputs
### Data Outputs
- Table: `<catalog>.<schema>.silver_audio_preprocessed`
  - **Grain**: one row per call_id
  - **Key columns**: call_id, input_path, output_path, output_sample_rate, output_channels, preprocess_version, run_id

### Operational Outputs
- `ops_file_status` stage `preprocess_audio` SUCCESS/FAILED/SKIPPED per call_id

### Artifacts
- Preprocessed `.wav` files written to Silver path.

## Business Rules / Logic
- If `enable_preprocess_audio=false`, stage is SKIPPED for all eligible calls.
- Eligibility in incremental mode:
  - preprocess missing or FAILED calls only
- Output naming:
  - output filename should preserve call_id (recommended) to simplify traceability
- Idempotency:
  - if output already exists for the same call_id and preprocess_version, skip or overwrite based on policy (recommended: skip in incremental, overwrite in full)

## Error Handling & Failure Isolation
- Per-call conversion errors are recorded and do not stop the batch.
- Task fails only if:
  - conversion library is unavailable OR
  - all eligible calls fail preprocessing.

## Data Quality Checks (Task-Level)
CRITICAL:
- output_sample_rate == target_sample_rate for SUCCESS
- output_channels == target_channels for SUCCESS
- output file exists (readable) for SUCCESS

## Acceptance Criteria
- For one eligible call_id, a standardized output file exists in Silver path and a metadata row exists in `silver_audio_preprocessed`.
- Re-run does not duplicate rows.
- `ops_file_status` reflects correct stage outcomes.

## Manual Verification Steps
1. Enable preprocess toggle and run for 1 file (sample mode).
2. Verify output file exists in Silver path.
3. Query `silver_audio_preprocessed` for that call_id.

## Deliverables
- Preprocess task notebook/script
- `silver_audio_preprocessed` table created/updated
