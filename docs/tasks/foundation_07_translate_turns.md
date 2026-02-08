# FOUNDATION_07_TRANSLATE_TURNS (docs/tasks/foundation_07_translate_turns.md)

## Task ID
- **ID**: FOUNDATION_07_TRANSLATE_TURNS

## Title
Translate redacted turns to a target language (default English) with skip logic

## Objective
Normalize conversation text into a chosen target language (default `en`) to simplify downstream analytics and LLM extraction. Translation occurs **after** PII redaction and includes a strict skip rule when source language equals the target language.

## Scope
### In Scope
- Select eligible call_ids from `gold_turns_redacted`.
- Determine source language using `language_final`.
- Translate text when required.
- Enforce skip logic and record flags.
- Persist results to `gold_turns_translated`.
- Update `ops_file_status` stage `translate_turns`.

### Out of Scope
- PII detection/redaction (already completed upstream).
- LLM insights.

## Inputs
### Data Inputs
- Table: `<catalog>.<schema>.gold_turns_redacted` (required)
- Optional: language mapping rules if language codes differ (policy-defined)

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `run_mode` (string, required)
- `max_files_per_run` (int, optional)
- `enable_translation` (bool, default true)
- `translation_target_language` (string, default `en`)
- `translation_model_name` (string, required when enabled)
- `translation_version` (string, default `v1`)
- `skip_when_same_language` (bool, default true)

## Outputs
### Data Outputs
- Table: `<catalog>.<schema>.gold_turns_translated`
  - **Grain**: one row per turn per call
  - **Key columns**:
    - call_id, turn_id, role, start_sec, end_sec
    - language_final
    - translation_target_language
    - translation_skipped_flag
    - text_redacted_source
    - text_translated (nullable if skipped by policy)
    - translation_model, translation_version, run_id

### Operational Outputs
- `ops_file_status` stage `translate_turns` SUCCESS/FAILED/SKIPPED per call_id

## Business Rules / Logic
Eligibility:
- incremental mode processes calls where translate_turns missing or FAILED
- requires redacted turns; if missing, mark FAILED

Skip rule (CRITICAL):
- If `enable_translation=false`:
  - mark stage SKIPPED and do not write translated rows (or write passthrough rows per policy)
- Else if `skip_when_same_language=true` AND `language_final == translation_target_language`:
  - set `translation_skipped_flag=true`
  - set `text_translated` either null or equal to source based on policy (must be consistent)
- Else:
  - translate and set `translation_skipped_flag=false`
  - `text_translated` must be non-empty

Idempotency:
- re-run does not duplicate rows
- overwrite/merge per call_id is allowed depending on mode policy

## Error Handling & Failure Isolation
- Per-call translation errors are recorded and do not stop the batch.
- Task fails only if all eligible calls fail translation.

## Data Quality Checks (Task-Level)
CRITICAL:
- If not skipped: translated text non-null and non-empty
- Skip logic is obeyed exactly (language_final == target => skipped)
WARNING:
- Very short translations for long inputs (possible truncation) — log only

## Acceptance Criteria
- English calls are skipped when target is `en`, with skip flag true.
- Non-English calls produce translated text with skip flag false.
- `ops_file_status` updated for translate_turns stage.

## Manual Verification Steps
1. Pick one non-English call_id with redacted turns.
2. Run translation with target `en`.
3. Verify translated rows exist and skip flag is false.
4. Pick one English call_id and verify skip flag is true.
