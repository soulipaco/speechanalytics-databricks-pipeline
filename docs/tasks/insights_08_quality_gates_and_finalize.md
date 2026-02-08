# INSIGHTS_08_QUALITY_GATES_AND_FINALIZE (docs/tasks/insights_08_quality_gates_and_finalize.md)

## Task ID
- **ID**: INSIGHTS_08_QUALITY_GATES_AND_FINALIZE

## Title
Apply quality gates and finalize Insights run

## Objective
Enforce the correctness contract of Insights outputs (taxonomy validity, enums, numeric ranges), record run summaries, and optionally publish stable “latest” views for demo/consumption.

## Scope
### In Scope
- Validate:
  - taxonomy membership
  - required fields
  - enum validity
  - confidence/score ranges
- Write results to an audit table (recommended) or ops summary.
- Mark calls as SUCCESS/WARN/FAILED for Insights stages.
- Update `ops_pipeline_runs` with final counts.

### Out of Scope
- Any changes to upstream Foundation outputs.

## Inputs
### Data Inputs
- `gold_conversation_metrics`
- `gold_speech_insights` (if enabled)
- taxonomy dim tables
- `ops_file_status`

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `fail_run_if_quality_checks_fail` (bool, default true)
- `quality_version` (string, default `v1`)
- `publish_latest_views` (bool, default false)

## Outputs
### Operational Outputs
- `ops_pipeline_runs` updated with success/warn/fail counts
- Optional: `<catalog>.<schema>.qa_quality_results` populated with check outcomes

### Optional Outputs
- Views:
  - `vw_gold_conversation_metrics_latest`
  - `vw_gold_speech_insights_latest`

## Business Rules / Logic
Critical validation checks:
- Labels must exist in active taxonomy dims for taxonomy_version
- Required enums must match allowed sets
- Confidence must be within [0,1]
- Love scores must be integer 1–10

Outcome policy:
- If critical checks fail and `fail_run_if_quality_checks_fail=true`, mark run FAILED.
- Otherwise, mark run WARN and record failures.

Idempotency:
- Re-running finalize should not duplicate QA results for same run_id unless policy allows.

## Acceptance Criteria
- Quality checks run and are recorded.
- Run summary updated.
- Optional latest views created when enabled.
