# INSIGHTS_01_COMPUTE_CONVERSATION_METRICS (docs/tasks/insights_01_compute_conversation_metrics.md)

## Task ID
- **ID**: INSIGHTS_01_COMPUTE_CONVERSATION_METRICS

## Title
Compute conversation metrics (non-LLM) from turns

## Objective
Create analytics-ready, deterministic speech metrics per call without using any LLM. These metrics support dashboards and provide structured features for later modeling/insights.

## Scope
### In Scope
- Select eligible call_ids from the Foundation outputs.
- Compute call-level metrics using turn timestamps and roles.
- Persist results to `gold_conversation_metrics`.
- Update `ops_file_status` stage `insights_01_compute_conversation_metrics`.

### Out of Scope
- Any LLM-based labeling (drivers/issues/intents/sentiment/emotions).
- Vector search / embeddings.
- Turn alignment changes (assumes Foundation outputs are correct).

## Inputs
### Data Inputs
- Preferred: `<catalog>.<schema>.gold_turns_translated` (if translation enabled)
- Else: `<catalog>.<schema>.gold_turns_redacted`
- Optional: `<catalog>.<schema>.silver_turns_aligned` (for extra timing metadata if needed)

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `run_mode` (string, required): `sample | incremental | full`
- `max_files_per_run` (int, optional)
- `translation_enabled` (bool, default true)  # determines preferred source table
- `metrics_version` (string, default `v1`)
- `silence_gap_threshold_sec` (float, default 0.8)  # defines silence between turns
- `overlap_handling_policy` (string, default `ignore_overlap_v1`)  # v1 simplicity

## Outputs
### Data Outputs
- Table: `<catalog>.<schema>.gold_conversation_metrics`
  - **Grain**: one row per call_id per metrics_version
  - **Key columns**:
    - call_id
    - total_duration_sec
    - agent_talk_time_sec
    - customer_talk_time_sec
    - unknown_talk_time_sec
    - silence_time_sec
    - turn_count_total
    - turn_count_agent
    - turn_count_customer
    - avg_turn_length_sec
    - first_turn_ts_sec, last_turn_ts_sec
    - metrics_version, run_id

### Operational Outputs
- `ops_file_status` stage `insights_01_compute_conversation_metrics` SUCCESS/FAILED per call_id
- `ops_pipeline_runs` updated with counts

## Business Rules / Logic
Eligibility:
- incremental mode: calls missing `insights_01_compute_conversation_metrics` or previously FAILED
- full mode: recompute for all eligible calls
- sample mode: limit to `max_files_per_run`

Metric computation (baseline v1):
- total_duration_sec:
  - use (max end_sec - min start_sec) from turns
- talk_time by role:
  - sum of (end_sec - start_sec) per role bucket
- silence_time_sec (approx):
  - sort turns by start_sec
  - silence gap between consecutive turns:
    - gap = next.start_sec - prev.end_sec
    - if gap > silence_gap_threshold_sec, add to silence_time_sec
- turn counts and averages:
  - count turns by role
  - avg_turn_length_sec = avg(end_sec - start_sec)

Notes:
- Overlap between turns may exist; v1 ignores overlap and treats each turn duration independently.
- More advanced overlap handling can be added later (v2).

Idempotency:
- Upsert by call_id + metrics_version (policy-defined)
- Re-run in incremental does not duplicate rows.

## Error Handling & Failure Isolation
- If a call_id has no turns, mark FAILED with reason.
- Per-call failures do not stop the batch.
- Task fails only if all eligible calls fail.

## Data Quality Checks (Task-Level)
CRITICAL:
- total_duration_sec > 0 for SUCCESS rows
- all talk times and silence time are >= 0
WARNING:
- agent_talk_time_sec + customer_talk_time_sec > total_duration_sec * 1.2 (overlap indicator)

## Acceptance Criteria
- For at least one call_id, `gold_conversation_metrics` row exists and metrics are non-negative.
- Re-run does not duplicate rows for already-successful calls.
- `ops_file_status` updated for stage `insights_01_compute_conversation_metrics`.

## Manual Verification Steps
1. Pick a call_id with turns in `gold_turns_redacted` or `gold_turns_translated`.
2. Run compute metrics.
3. Query `gold_conversation_metrics` for that call_id and validate fields.
