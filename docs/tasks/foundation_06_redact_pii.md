# FOUNDATION_06_REDACT_PII (docs/tasks/foundation_06_redact_pii.md)

## Task ID
- **ID**: FOUNDATION_06_REDACT_PII

## Title
Detect and redact PII in aligned conversation turns

## Objective
Convert aligned turn text into a compliance-safe dataset by detecting and redacting PII using Microsoft Presidio plus a deterministic regex rule layer. Outputs are stored in `gold_turns_redacted` and become the default input surface for downstream analytics and the LLM insights layer.

## Scope
### In Scope
- Select eligible call_ids with aligned turns.
- Run Presidio detection and apply redaction placeholders.
- Apply regex-based redaction rules (secondary layer).
- Perform residual risk scan and set `pii_residual_risk_flag`.
- Persist redacted turns into `gold_turns_redacted`.
- Update `ops_file_status` stage `redact_pii`.

### Out of Scope
- Translation (separate task).
- LLM insights (separate workflow).
- Perfect multilingual entity recognition (v1 focuses on layered coverage + flags).

## Inputs
### Data Inputs
- Table: `<catalog>.<schema>.silver_turns_aligned`

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `run_mode` (string, required)
- `max_files_per_run` (int, optional)
- `enable_pii_redaction` (bool, default true)
- `presidio_language` (string, optional, default `en`): analyzer language setting (implementation-specific)
- `redaction_version` (string, required, default `v1`)
- `redaction_placeholders` (map, optional): entity_type → placeholder token
- `enable_residual_risk_scan` (bool, default true)

## Outputs
### Data Outputs
- Table: `<catalog>.<schema>.gold_turns_redacted`
  - **Grain**: one row per turn per call
  - **Key columns**:
    - call_id, turn_id, role, start_sec, end_sec
    - text_redacted
    - pii_found_flag, pii_entities, pii_entity_counts
    - pii_residual_risk_flag
    - redaction_method, redaction_version, run_id

### Operational Outputs
- `ops_file_status` stage `redact_pii` SUCCESS/FAILED/SKIPPED per call_id
- `ops_pipeline_runs` updated with counts

## Business Rules / Logic
Eligibility:
- calls with `silver_turns_aligned` available
- in incremental mode:
  - process call_ids that are missing `redact_pii` stage or previously FAILED

Redaction ordering:
1) Presidio analyze + anonymize
2) Regex masking (email/phone/url/iban-like/digit sequences)
3) Residual scan (detect patterns that should not remain)

Skip behavior:
- if `enable_pii_redaction=false`, stage sets SKIPPED and writes passthrough redacted text policy-defined
  - recommended: still create `gold_turns_redacted` with text_redacted = original (but clearly flagged) OR stop pipeline (policy choice)

Placeholders:
- Must be consistent and versioned (e.g., `[EMAIL]`, `[PHONE]`, `[PERSON]`)

Idempotency:
- re-runs overwrite/redetermine redaction for a call_id under the same redaction_version (policy-defined)

## Error Handling & Failure Isolation
- If redaction fails for a call_id:
  - mark FAILED for stage `redact_pii`
  - store error_message
  - continue with other call_ids
- Task fails only if:
  - zero call_ids are redacted successfully AND at least one was eligible

## Performance & Resource Notes
- Presidio analysis is generally cheaper than ASR/diarization.
- Regex masking is lightweight and should always run.

## Data Quality Checks (Task-Level)
CRITICAL:
- text_redacted not null
- if pii_found_flag=true then:
  - placeholders appear in text_redacted OR entity_counts not empty
WARNING:
- residual risk true (report but do not fail unless strict mode)

## Acceptance Criteria (Definition of Done)
- For test calls containing fictional PII:
  - `gold_turns_redacted` replaces PII with placeholders
  - pii_found_flag=true and entity_counts populated
- For calls with no PII:
  - pii_found_flag=false and redacted text equals original (except normalization)
- Residual risk scan sets `pii_residual_risk_flag` when suspicious patterns remain.
- `ops_file_status` shows correct statuses per call_id for stage `redact_pii`.

## Manual Verification Steps
1. Ensure `silver_turns_aligned` exists for at least one call_id.
2. Run redaction task in sample mode for that call_id.
3. Query `gold_turns_redacted` and confirm placeholders are present for known test PII.
4. Query `ops_file_status` stage `redact_pii` and confirm SUCCESS.

## Deliverables
- Task notebook/script for PII detection + redaction
- `gold_turns_redacted` table created/updated
- Stage status updates for `redact_pii`

## Notes / Future Improvements
- Add language-specific Presidio recognizers for non-English.
- Add Luhn validation for credit-card-like patterns.
- Add configurable “strict masking” demo mode for public sharing.
