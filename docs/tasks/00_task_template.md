# Task Template (docs/tasks/00_task_template.md)

## Task ID
- **ID**: <PIPELINE>_<NN>_<TASK_NAME>  
  Example: `FOUNDATION_01_INGEST_AUDIO`

## Title
<Short imperative title>  
Example: `Ingest audio files into Bronze manifest`

## Objective
One paragraph describing the task goal and why it exists.

## Scope
### In Scope
- Bullet list of what this task must do.

### Out of Scope
- Bullet list of what this task must not do (to prevent scope creep).

## Inputs
### Data Inputs
- Tables:
  - `<CATALOG>.<SCHEMA>.<TABLE>` (purpose)
- Files/Paths:
  - `Volumes/<catalog>/<schema>/...` (purpose)

### Configuration Inputs (Parameters)
List all parameters this task requires, with:
- name
- type
- default (if any)
- description

Example:
- `catalog` (string, required): UC catalog name
- `schema` (string, required): schema name
- `volume_root` (string, required): base volume path
- `run_id` (string, required): workflow run identifier
- `run_mode` (string, required): sample|incremental|full
- `max_files_per_run` (int, optional): limit for sample mode

## Outputs
### Data Outputs
- Tables created/updated with a short description and expected row grain:
  - `<CATALOG>.<SCHEMA>.<TABLE>` — grain, key columns, description

### Operational Outputs
- Ops tables updated:
  - `ops_pipeline_runs` updates expected
  - `ops_file_status` stage entries expected

### Artifacts (Optional)
- Files written to Volumes (if any)

## Business Rules / Logic
Describe the exact rules and edge cases as bullet points.
Include:
- eligibility logic (which call_id rows are processed)
- skip logic
- failure handling behavior
- idempotency expectations (safe re-runs)

## Error Handling & Failure Isolation
Define:
- which errors are per-file and should not fail the batch
- which errors are critical and should fail the task
- how errors are recorded (error_message fields)

## Performance & Resource Notes
- Expected runtime characteristics
- Known free-tier constraints
- Recommended defaults (e.g., max_files_per_run)

## Data Quality Checks (Task-Level)
List explicit checks this task must run and how failures are handled:
- CRITICAL checks (fail task/workflow or mark call_id failed)
- WARNING checks (log + continue)

## Acceptance Criteria (Definition of Done)
Provide concrete, verifiable outcomes:
- “When X is done, Y table has Z rows…”
- “Re-running does not duplicate…”
- “Status tables show stage SUCCESS…”

## Manual Verification Steps
Step-by-step checklist a human can follow in Databricks SQL/Notebook to confirm success.

## Deliverables
List what changes will exist after completion:
- notebooks/foundation/<task_notebook>.py OR notebooks/insights/<task_notebook>.py created
- configuration keys added
- tables/views created
- docs updated (if needed)

## Notes / Future Improvements
Optional section for follow-ups that should *not* block completion.
