# FOUNDATION_01_INGEST_AUDIO (docs/tasks/foundation_01_ingest_audio.md)

## Task ID
- **ID**: FOUNDATION_01_INGEST_AUDIO

## Title
Ingest audio files into Bronze manifest

## Objective
Discover `.wav` files from a governed Unity Catalog Volume location and register them into the `bronze_audio_files` Delta manifest. This enables incremental processing, de-duplication, and file-level failure isolation across the pipeline.

## Scope
### In Scope
- Scan the configured Bronze audio directory for `.wav` files.
- Compute or extract key metadata per file (hash, duration, sample_rate, channels).
- Generate a stable `call_id` per file.
- Upsert rows into `bronze_audio_files` with status and metadata.
- Update `ops_file_status` for stage `ingest_audio`.

### Out of Scope
- Audio preprocessing (mono/16kHz conversion).
- ASR, diarization, alignment, redaction, translation.
- Any real PII processing (this stage handles files only).

## Inputs
### Data Inputs
- Files/Paths:
  - `Volumes/<catalog>/<schema>/bronze/audio_raw/` — source directory for raw `.wav`

### Configuration Inputs (Parameters)
- `catalog` (string, required): UC catalog name
- `schema` (string, required): schema name
- `volume_root` (string, required): base volume path (used to derive bronze path)
- `run_id` (string, required): workflow run identifier
- `run_mode` (string, required): `sample | incremental | full`
- `max_files_per_run` (int, optional): limit files ingested in sample mode
- `language_hint_strategy` (string, optional, default: `from_filename_prefix`):
  - `none | from_filename_prefix | from_folder_name | mapping_table`
- `source_type` (string, optional, default: `synthetic`): for manifest tagging

## Outputs
### Data Outputs
- Table: `<catalog>.<schema>.bronze_audio_files`
  - **Grain**: one row per audio file (call)
  - **Key columns**: call_id, file_path, file_hash, duration_sec, sample_rate, channels, language_hint, source_type, status

### Operational Outputs
- Table: `<catalog>.<schema>.ops_file_status`
  - stage_name = `ingest_audio`
  - status = SUCCESS/FAILED per call_id
- Table: `<catalog>.<schema>.ops_pipeline_runs` updated with counts and timestamps (workflow-level)

## Business Rules / Logic
- Only files with extension `.wav` are considered eligible.
- `call_id` must be stable across reruns:
  - recommended: derived from `file_hash` (e.g., `call_<first12ofhash>`)
- De-duplication:
  - if a file with the same `file_hash` already exists, do not insert a new row (or update the existing row’s metadata if policy allows).
- Status initialization:
  - new rows default to `status = NEW`
- Incremental behavior:
  - in `incremental`, only ingest new files not already in `bronze_audio_files`
  - in `full`, rescan and refresh metadata if desired (policy-defined)
  - in `sample`, ingest up to `max_files_per_run`

## Error Handling & Failure Isolation
- Per-file failures (corrupt audio, cannot read metadata) should:
  - mark that file as FAILED in `ops_file_status` for stage `ingest_audio`
  - write error_message
  - continue scanning remaining files
- Task fails only if:
  - the source directory is unreachable
  - or all scanned files failed

## Performance & Resource Notes
- On free tier, avoid scanning massive folders. Keep sample dataset small.
- Prefer metadata extraction that does not fully decode audio if possible.

## Data Quality Checks (Task-Level)
CRITICAL:
- duration_sec > 0 for SUCCESS files
- file_hash not null for SUCCESS files
WARNING:
- language_hint missing (allowed; pipeline can still run)

## Acceptance Criteria (Definition of Done)
- Placing a new `.wav` in Bronze results in a new row in `bronze_audio_files` with:
  - non-null `call_id`, `file_path`, `file_hash`, `duration_sec`, `sample_rate`, `channels`
  - `status = NEW`
- Re-running ingestion does **not** create duplicates for the same file.
- `ops_file_status` contains one row per `call_id` for stage `ingest_audio` with SUCCESS/FAILED.

## Manual Verification Steps
1. Upload one `.wav` into `Volumes/<catalog>/<schema>/bronze/audio_raw/`.
2. Run the ingestion task.
3. Query `bronze_audio_files` for the file_path and confirm metadata populated.
4. Query `ops_file_status` for stage `ingest_audio` and confirm SUCCESS.

## Deliverables
- Task notebook/script for ingestion
- `bronze_audio_files` table created/updated
- `ops_file_status` stage updates for ingest_audio

## Notes / Future Improvements
- Add optional “language_hint mapping table” keyed by filename patterns.
- Add support for other audio formats (mp3, flac) behind a toggle.
