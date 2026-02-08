# Foundation Runbook

Foundation notebooks are implemented and production-shaped, but this repository flow is static-only (no Databricks job execution here).

## Execution guidance
- Recommended order: `01 -> 08`
- Start with `run_mode=sample` before `incremental` or `full`
- Common core widgets: `catalog`, `schema`, `run_id`, `run_mode`, `max_files_per_run`

## Execution order + kill switches
Smoke-test order:
1. `foundation_01_ingest_audio`
2. `foundation_02_preprocess_audio`
3. `foundation_03_diarize_audio`
4. `foundation_04_transcribe_audio`
5. `foundation_05_align_turns`
6. `foundation_06_redact_pii`
7. `foundation_08_publish_and_finalize`

Important smoke-test switches:
- `foundation_02_preprocess_audio`: set `enable_preprocess_audio=false` (optional stage for smoke test)
- `foundation_03_diarize_audio`: set `enable_diarization=false` (or keep fallback segmentation enabled)
- `foundation_07_translate_turns`: default `enable_translation=true`; set `enable_translation=false` for smoke test (or skip stage 07)
- `foundation_08_publish_and_finalize`: set `enable_translation=false` when stage 07 is skipped

Input requirement callout:
- `foundation_01_ingest_audio.py` requires a readable `volume_root` with audio files available under expected source paths.

## Notebook index

### 01) `foundation_01_ingest_audio.py`
- Purpose: Discover source audio and register manifest records.
- Inputs: volume/source audio path (`volume_root` conventions).
- Outputs: `bronze_audio_files`, `ops_file_status`, `ops_pipeline_runs`.
- Stage name: `ingest_audio`.
- Idempotency key: manifest identity (`file_hash`/path-level dedupe + MERGE behavior).
- Key widgets: `catalog`, `schema`, `run_id`, `run_mode`, `volume_root`, `language_hint_strategy`.

### 02) `foundation_02_preprocess_audio.py`
- Purpose: Optional normalization (sample rate/channels) and preprocessed artifact registration.
- Inputs: `bronze_audio_files` (+ raw audio files).
- Outputs: `silver_audio_preprocessed`, `ops_file_status`, `ops_pipeline_runs`.
- Stage name: `preprocess_audio`.
- Idempotency key: successful `call_id` rows are overwrite-by-delete+append for stage output.
- Key widgets: `enable_preprocess_audio`, `target_sample_rate`, `target_channels`, `preprocess_version`.

### 03) `foundation_03_diarize_audio.py`
- Purpose: Build speaker segments with diarization and fallback segmentation options.
- Inputs: `bronze_audio_files`, optional `silver_audio_preprocessed`.
- Outputs: `silver_diarization_segments`, `ops_file_status`, `ops_pipeline_runs`.
- Stage name: `diarize_audio`.
- Idempotency key: successful `call_id` segment rows replaced on rerun.
- Key widgets: `enable_diarization`, `diarization_model_name`, `enable_fallback_segmentation`, `fallback_method`.

### 04) `foundation_04_transcribe_audio.py`
- Purpose: Generate ASR segments with timing/language metadata.
- Inputs: `bronze_audio_files`, optional `silver_audio_preprocessed`.
- Outputs: `silver_asr_segments`, `ops_file_status`, `ops_pipeline_runs`.
- Stage name: `transcribe_audio`.
- Idempotency key: successful `call_id` ASR rows replaced on rerun.
- Key widgets: `asr_model_name`, `compute_type`, `language_mode`, `forced_language`.

### 05) `foundation_05_align_turns.py`
- Purpose: Align ASR + diarization into conversation turns.
- Inputs: `silver_asr_segments`, optional `silver_diarization_segments`, `bronze_audio_files`.
- Outputs: `silver_turns_aligned`, `ops_file_status`, `ops_pipeline_runs`.
- Stage name: `align_turns`.
- Idempotency key: successful `call_id` turn rows replaced on rerun.
- Key widgets: `alignment_version`, `alignment_policy`, `role_assignment_policy`, `language_resolution_policy`.

### 06) `foundation_06_redact_pii.py`
- Purpose: Redact PII to produce compliance-safe text.
- Inputs: `silver_turns_aligned`.
- Outputs: `gold_turns_redacted`, `ops_file_status`, `ops_pipeline_runs`.
- Stage name: `redact_pii`.
- Idempotency key: successful `call_id` redacted rows replaced on rerun.
- Key widgets: `enable_pii_redaction`, `redaction_version`, `presidio_language`, `enable_residual_risk_scan`.

### 07) `foundation_07_translate_turns.py`
- Purpose: Optional translation of redacted turns.
- Inputs: `gold_turns_redacted`.
- Outputs: `gold_turns_translated` (optional), `ops_file_status`, `ops_pipeline_runs`.
- Stage name: `translate_turns`.
- Idempotency key: successful `call_id` translated rows replaced on rerun.
- Key widgets: `enable_translation`, `translation_target_language`, `translation_version`, `translation_endpoint_name`.

### 08) `foundation_08_publish_and_finalize.py`
- Purpose: Finalize Foundation completion state and update run summaries.
- Inputs: `bronze_audio_files`, `ops_file_status`, `gold_turns_redacted`, optional `gold_turns_translated`.
- Outputs: manifest/status updates in `bronze_audio_files`, `ops_file_status`, `ops_pipeline_runs`.
- Stage name: `publish_and_finalize`.
- Idempotency key: deterministic status transitions; rerun-safe completion logic.
- Key widgets: `enable_translation`, `foundation_complete_policy`.
