# FOUNDATION_05_ALIGN_TURNS (docs/tasks/foundation_05_align_turns.md)

## Task ID
- **ID**: FOUNDATION_05_ALIGN_TURNS

## Title
Align diarization segments and ASR segments into conversation turns

## Objective
Combine diarization output (who spoke when) and ASR output (what was said when) into a coherent, ordered, turn-level representation. This produces `silver_turns_aligned`, which is the primary intermediate dataset for redaction, translation, metrics, and LLM insights.

## Scope
### In Scope
- Select eligible call_ids where ASR exists (and diarization if enabled).
- Align ASR segments to speaker segments.
- Construct turn-level rows with speaker labels, roles, timestamps, and text.
- Resolve language fields (hint/detected/final).
- Update `ops_file_status` stage `align_turns`.

### Out of Scope
- PII redaction and translation.
- Perfect Agent/Customer classification (v1 uses baseline heuristic).

## Inputs
### Data Inputs
- Table: `<catalog>.<schema>.silver_asr_segments` (required)
- Table: `<catalog>.<schema>.silver_diarization_segments` (optional but preferred)
- Table: `<catalog>.<schema>.bronze_audio_files` (for language_hint and metadata)

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `run_mode` (string, required)
- `max_files_per_run` (int, optional)
- `alignment_version` (string, default `v1`)
- `enable_diarization` (bool, default true)  # controls whether diarization is expected
- `alignment_policy` (string, default `max_overlap`)  # alternatives: midpoint
- `role_assignment_policy` (string, default `unknown_roles_v1`)
- `language_resolution_policy` (string, default `prefer_detected_else_hint`)

## Outputs
### Data Outputs
- Table: `<catalog>.<schema>.silver_turns_aligned`
  - **Grain**: one row per turn per call
  - **Key columns**:
    - call_id, turn_id, speaker_label, role
    - start_sec, end_sec
    - text_original
    - language_hint, language_detected, language_final
    - alignment_version, run_id

### Operational Outputs
- `ops_file_status` stage `align_turns` SUCCESS/FAILED per call_id

## Business Rules / Logic
Eligibility:
- incremental mode processes calls where align_turns missing or FAILED
- requires ASR segments to exist; if missing, mark FAILED with reason

Alignment logic (v1 baseline):
- For each ASR segment, assign it to a speaker segment using:
  - max time overlap, or
  - midpoint containment if configured
- Merge contiguous ASR segments belonging to the same speaker_label if the gap is small (optional)
- Generate turns in chronological order

Diarization optionality:
- If diarization is unavailable:
  - set speaker_label = `SPEAKER_UNKNOWN`
  - role = `Unknown`
  - create turns purely from ASR segments

Role assignment (v1 baseline):
- Default: `Unknown` for all roles
- Optionally:
  - if diarization yields 2 dominant speakers, map them to Agent/Customer using a simple heuristic (documented), but keep v1 conservative.

Language resolution:
- `language_hint` from bronze manifest (if present)
- `language_detected` from ASR
- `language_final` rule:
  - prefer detected if non-null else hint else `unknown`

Idempotency:
- overwrite or merge per call_id depending on run_mode policy (recommended: overwrite in full; skip SUCCESS in incremental)

## Error Handling & Failure Isolation
- Per-call alignment failures are recorded in ops and do not stop the batch.
- Task fails only if all eligible calls fail.

## Data Quality Checks (Task-Level)
CRITICAL:
- start_sec < end_sec for all turns
- language_final not null (use `unknown` if necessary)
WARNING:
- high share of empty text turns (should be rare)

## Acceptance Criteria
- For one eligible call_id, `silver_turns_aligned` exists with:
  - ordered turns, valid timestamps, non-empty text
  - language fields populated (final at minimum)
- If diarization missing, turns still exist with speaker_unknown.
- `ops_file_status` updated for align_turns stage.

## Manual Verification Steps
1. Ensure ASR segments exist for a call_id.
2. Run align task for that call_id.
3. Query `silver_turns_aligned` and verify timestamps, text, and language_final.
