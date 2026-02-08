# FOUNDATION_08_PUBLISH_AND_FINALIZE (docs/tasks/foundation_08_publish_and_finalize.md)

## Task ID
- **ID**: FOUNDATION_08_PUBLISH_AND_FINALIZE

## Title
Finalize Foundation run: update manifest status and publish stable views

## Objective
Finalize the Foundation workflow by marking completed calls as processed, updating run summaries, and optionally creating stable “latest” views for demo/consumption. This stage is responsible for end-of-run state transitions.

## Scope
### In Scope
- Determine which call_ids are “foundation complete”.
- Update `bronze_audio_files.status` accordingly.
- Write a run summary into `ops_pipeline_runs`.
- Optionally publish convenience views for demo.

### Out of Scope
- Any ASR/diarization/translation/redaction processing.
- Insights pipeline artifacts.

## Inputs
### Data Inputs
- `bronze_audio_files`
- `ops_file_status`
- `gold_turns_redacted`
- `gold_turns_translated` (if translation enabled)

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `enable_translation` (bool, default true)
- `foundation_complete_policy` (string, default `redacted_required`)
  - `redacted_required`:
    - align_turns SUCCESS
    - redact_pii SUCCESS
    - translate_turns SUCCESS or SKIPPED (if enabled)
  - `translation_optional`:
    - translation does not block completion

## Outputs
### Data Outputs
- Updates `bronze_audio_files.status`:
  - NEW → PROCESSED for completed call_ids
  - FAILED stays FAILED or is updated based on policy

### Operational Outputs
- `ops_pipeline_runs` updated with:
  - total eligible calls
  - succeeded/failed counts per stage
  - end status SUCCESS/WARN/FAILED

### Optional Outputs
- Views like:
  - `vw_gold_turns_latest`
  - `vw_gold_insights_latest` (if available later)

## Business Rules / Logic
Completion definition:
- A call_id is “foundation complete” when required stages succeeded based on policy.

Manifest updates:
- If foundation complete: `bronze_audio_files.status = PROCESSED`
- If critical stage failed: keep as FAILED (or record stage-specific failure in ops)

Idempotency:
- Re-running finalize should not flip statuses incorrectly.

## Error Handling
- If ops tables unavailable, task fails (critical).
- If no calls are eligible, task succeeds but records 0 counts.

## Acceptance Criteria
- Completed calls are marked PROCESSED in bronze manifest.
- Run summary reflects correct counts.
- No completed call is left as NEW.

## Manual Verification Steps
1. Query `ops_file_status` and identify a call_id with required stage SUCCESS.
2. Run finalize task.
3. Confirm `bronze_audio_files.status` changed to PROCESSED for that call_id.
