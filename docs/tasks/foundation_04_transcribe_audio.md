# FOUNDATION_04_TRANSCRIBE_AUDIO (docs/tasks/foundation_04_transcribe_audio.md)

## Task ID
- **ID**: FOUNDATION_04_TRANSCRIBE_AUDIO

## Title
Transcribe audio with Whisper and produce ASR segments

## Objective
Generate time-aligned transcript segments from audio calls using Whisper (or compatible ASR), producing `silver_asr_segments` with timestamps and language detection. This stage is the foundation for turn alignment, redaction, translation, and downstream analytics.

## Scope
### In Scope
- Select eligible `call_id`s based on manifest and stage statuses.
- Load audio files for eligible calls.
- Run ASR and write segment-level outputs into `silver_asr_segments`.
- Record model and compute configuration used.
- Update `ops_file_status` for stage `transcribe_audio`.

### Out of Scope
- Diarization (handled in separate task).
- Turn alignment (handled in separate task).
- PII redaction and translation.

## Inputs
### Data Inputs
- Table: `<catalog>.<schema>.bronze_audio_files`
- Files/Paths: raw audio under `Volumes/<catalog>/<schema>/bronze/audio_raw/`
- (Optional) Table: `<catalog>.<schema>.silver_audio_preprocessed` if preprocessing stage is used

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `run_mode` (string, required): `sample | incremental | full`
- `max_files_per_run` (int, optional)
- `asr_model_name` (string, required): e.g., "whisper-large" (implementation-specific)
- `compute_type` (string, optional): e.g., float16/int8 (implementation-specific)
- `use_preprocessed_audio` (bool, default false)
- `language_mode` (string, default `auto`): `auto | force`
- `forced_language` (string, optional): only used when language_mode=force

## Outputs
### Data Outputs
- Table: `<catalog>.<schema>.silver_asr_segments`
  - **Grain**: one row per ASR segment per call
  - **Key columns**:
    - call_id, asr_segment_id, start_sec, end_sec, text
    - language_detected
    - asr_model_name, compute_type, run_id
    - optional: avg_logprob, no_speech_prob

### Operational Outputs
- `ops_file_status` stage `transcribe_audio` SUCCESS/FAILED per call_id
- `ops_pipeline_runs` updated with counts

## Business Rules / Logic
Eligibility:
- process calls with:
  - `bronze_audio_files.status in (NEW, PROCESSED)` depending on mode, AND
  - `ops_file_status(transcribe_audio)` missing or FAILED (incremental), OR all calls (full), OR limited set (sample)

ASR output rules:
- Only persist segments that represent speech (policy-defined if no-speech exists).
- `start_sec < end_sec` must hold for all segments.
- `language_detected` should be stored even if translation is disabled.

Idempotency:
- For a given `call_id` + `asr_model_name` + `run_id`:
  - outputs must be consistent and not duplicated.
- Upsert/merge policy:
  - v1 recommended: overwrite segments for the call_id when re-running in full mode, otherwise skip successful ones.

## Error Handling & Failure Isolation
- If ASR fails for a call_id:
  - mark call_id FAILED for stage `transcribe_audio`
  - store error_message
  - continue to next call_id
- Task fails only if:
  - zero calls are successfully transcribed AND at least one call was eligible.

## Performance & Resource Notes
- Whisper large may be heavy; in free tier:
  - keep `max_files_per_run` small
  - prefer short calls
  - consider a smaller model for dev runs, and run large only for “final demo” (documented)

## Data Quality Checks (Task-Level)
CRITICAL:
- no rows with `start_sec >= end_sec`
- non-null `text` for speech segments
- call_id must exist in bronze manifest
WARNING:
- extremely low transcript coverage vs duration (possible no-speech or failure)

## Acceptance Criteria (Definition of Done)
- For at least one eligible call_id:
  - `silver_asr_segments` contains multiple segments with valid timestamps and text
  - `language_detected` is populated (auto or forced)
- Re-run in incremental mode does not duplicate rows for already-successful call_ids.
- `ops_file_status` for stage `transcribe_audio` correctly shows SUCCESS/FAILED per call_id.

## Manual Verification Steps
1. Ensure at least one call_id is NEW in `bronze_audio_files`.
2. Run transcription task in sample mode with max_files_per_run=1.
3. Query `silver_asr_segments` by call_id and verify segments exist.
4. Check `ops_file_status` for stage `transcribe_audio`.

## Deliverables
- Task notebook/script for ASR transcription
- `silver_asr_segments` table created/updated
- Status updates for `transcribe_audio`

## Notes / Future Improvements
- Add diarization-aware re-segmentation for improved turn alignment.
- Store additional ASR metadata (word-level timestamps if supported).
