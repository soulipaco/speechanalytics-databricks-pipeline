# INSIGHTS_02_LOAD_TAXONOMIES_TO_DIM_TABLES (docs/tasks/insights_02_load_taxonomies_to_dim_tables.md)

## Task ID
- **ID**: INSIGHTS_02_LOAD_TAXONOMIES_TO_DIM_TABLES

## Title
Load taxonomy YAML files into dimension tables

## Objective
Load controlled taxonomies (drivers/issues/intents/emotions) from `taxonomies/*.yml` into Delta dimension tables. These dim tables are the authoritative label sets used to constrain LLM outputs and validate results.

## Scope
### In Scope
- Read YAML files from the repo folder `taxonomies/`.
- Validate YAML structure and required fields.
- Create/overwrite or upsert into dim tables:
  - `dim_contact_driver`
  - `dim_issue`
  - `dim_intent`
  - `dim_emotion_catalog`
- Record taxonomy version and active flags.
- Update `ops_pipeline_runs` with taxonomy load status.

### Out of Scope
- Running LLM tasks.
- Embeddings or vector search.

## Inputs
### Data Inputs
- Files:
  - `taxonomies/contact_drivers.yml`
  - `taxonomies/issues.yml`
  - `taxonomies/intents.yml`
  - `taxonomies/emotions.yml`

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `taxonomy_load_mode` (string, default `upsert_by_version`)
  - `replace_all`: replace dim tables fully
  - `upsert_by_version`: insert new versions, keep old
- `enforce_unique_labels` (bool, default true)
- `require_examples` (bool, default true)

## Outputs
### Data Outputs
- `<catalog>.<schema>.dim_contact_driver`
- `<catalog>.<schema>.dim_issue`
- `<catalog>.<schema>.dim_intent`
- `<catalog>.<schema>.dim_emotion_catalog`

Each table includes at minimum:
- taxonomy_name
- taxonomy_version
- label (or emotion)
- active_flag
- definition
- synonyms (array)
- examples (array)
- created_at, updated_at (optional)
- source_file (optional)

### Operational Outputs
- `ops_pipeline_runs` updated with taxonomy load counts and versions
- Optional: `qa_quality_results` entries for taxonomy validation (recommended)

## Business Rules / Logic
Validation rules (CRITICAL):
- taxonomy version is present
- each item has label/emotion, active flag, definition
- labels are unique within a taxonomy version (if enforced)
- required enums for emotions: sentiment_group must be in {Positive, Neutral, Negative}
- polarity_score must be numeric in [-1, +1] (or policy-defined)

Load strategy:
- `replace_all`: overwrite the whole table
- `upsert_by_version`: preserve history; insert rows for new versions

Active flags:
- do not delete old labels; deactivate instead

## Error Handling & Failure Isolation
- Any YAML parsing failure is CRITICAL and fails the task.
- If one taxonomy file fails validation, fail the whole taxonomy load (safer).

## Data Quality Checks (Task-Level)
CRITICAL:
- duplicate labels within same version not allowed (if enforced)
- required fields exist
- emotion polarity score in range
WARNING:
- too many labels set to inactive (informational)

## Acceptance Criteria
- Dim tables exist and contain expected label counts.
- A query of active labels by version returns the same labels as YAML.
- Validation failures prevent pipeline from continuing to LLM steps.

## Manual Verification Steps
1. Confirm YAML files exist in repo.
2. Run taxonomy load task.
3. Query each dim table and validate `taxonomy_version`, active labels, and definitions.
