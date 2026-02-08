# FOUNDATION_03_DIARIZE_AUDIO (docs/tasks/foundation_03_diarize_audio.md)

## Task ID
- **ID**: FOUNDATION_03_DIARIZE_AUDIO

## Title
Diarize audio and produce speaker segments (with fallback)

## Objective
Generate speaker segmentation for each call using a primary diarization method (e.g., pyannote) and a fallback segmentation strategy if diarization fails or is disabled. Outputs are stored as `silver_diarization_segments` and used for turn alignment and speech metrics.

## Scope
### In Scope
- Select eligible call_ids.
- Load audio (preprocessed if configured).
- Run diarization (primary) OR fallback segmentation (secondary).
- Persist segments with method metadata.
- Update `ops_file_status` stage `diarize_audio`.

### Out of Scope
- ASR transcription.
- Role assignment logic beyond basic labeling.
- LLM insights.

## Inputs
### Data Inputs
- Table: `<catalog>.<schema>.bronze_audio_files`
- Optional: `<catalog>.<schema>.silver_audio_preprocessed`
- Audio files under configured Bronze/Silver paths

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `run_mode` (string, required)
- `max_files_per_run` (int, optional)
- `enable_diarization` (bool, default true)
- `use_preprocessed_audio` (bool, default false)
- `diarization_model_name` (string, optional)
- `diarization_version` (string, default `v1`)
- `enable_fallback_segmentation` (bool, default true)
- `fallback_method` (string, default `vad_fallback`)  # e.g., VAD-based segmentation
- `min_segment_sec` (float, optional, default 0.5)
- `merge_gap_sec` (float, optional, default 0.2)

## Outputs
### Data Outputs
- Table: `<catalog>.<schema>.silver_diarization_segments`
  - **Grain**: one row per speaker segment per call
  - **Key columns**:
    - call_id, segment_id, speaker_label, start_sec, end_sec
    - method (pyannote / vad_fallback)
    - diarization_model, diarization_version, run_id
    - optional: confidence

### Operational Outputs
- `ops_file_status` stage `diarize_audio` SUCCESS/FAILED/SKIPPED per call_id

## Business Rules / Logic
Eligibility:
- incremental mode processes calls where diarize_audio missing or FAILED
- sample mode limits calls by max_files_per_run
- full mode reprocesses all calls

Primary vs fallback:
- If `enable_diarization=true`, attempt primary diarization.
- If primary diarization fails AND `enable_fallback_segmentation=true`, run fallback segmentation and mark method accordingly.
- If `enable_diarization=false` AND fallback enabled, run fallback segmentation only.
- If both disabled, stage is SKIPPED (but alignment may still run in ASR-only mode with role=Unknown).

Segment rules:
- Ensure start_sec < end_sec
- Enforce minimum segment length if configured
- Merge adjacent segments within merge_gap_sec if configured

Idempotency:
- For a call_id, re-run overwrites/replaces segments based on mode policy (recommended: overwrite in full; skip SUCCESS in incremental)

## Error Handling & Failure Isolation
- Per-call diarization errors are recorded.
- If fallback also fails for a call, mark FAILED.
- Task fails only if all eligible calls fail.

## Data Quality Checks (Task-Level)
CRITICAL:
- no segments where start_sec >= end_sec
- segments within call duration tolerance
WARNING:
- very low number of segments for long calls (possible diarization failure)

## Acceptance Criteria
- For one call_id, segments exist in `silver_diarization_segments` with method populated.
- If primary diarization fails, fallback segments still appear with method `vad_fallback`.
- `ops_file_status` updated for diarize_audio stage.

## Manual Verification Steps
1. Run diarize task for 1 call (sample mode).
2. Query `silver_diarization_segments` and confirm segments exist.
3. Confirm method indicates primary or fallback.
