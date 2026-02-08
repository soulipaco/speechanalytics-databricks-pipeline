# INSIGHTS_03_BUILD_TEXT_CHUNKS (docs/tasks/insights_03_build_text_chunks.md)

## Task ID
- **ID**: INSIGHTS_03_BUILD_TEXT_CHUNKS

## Title
Build text chunks from conversation turns for long-call handling

## Objective
Create chunked text representations of calls to support long transcripts and improve reliability for embedding and LLM stages. Chunking provides bounded context windows and enables retrieval of relevant portions of the call.

## Scope
### In Scope
- Select eligible call_ids.
- Create chunks from ordered turns.
- Store chunk text + metadata in `silver_text_chunks`.
- Update `ops_file_status` stage `insights_03_build_text_chunks`.

### Out of Scope
- Embeddings generation.
- LLM inference.

## Inputs
### Data Inputs
- Preferred: `gold_turns_translated` (if translation enabled)
- Else: `gold_turns_redacted`

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `run_mode` (string, required)
- `max_files_per_run` (int, optional)
- `translation_enabled` (bool, default true)
- `chunking_strategy` (string, default `turn_count`)
  - `turn_count` | `time_window` | `hybrid`
- `max_turns_per_chunk` (int, default 12)
- `max_seconds_per_chunk` (int, default 90)  # used for time_window/hybrid
- `chunk_version` (string, default `v1`)
- `include_role_prefix` (bool, default true)  # e.g., "AGENT: ..."

## Outputs
### Data Outputs
- Table: `<catalog>.<schema>.silver_text_chunks`
  - **Grain**: one row per call_id + chunk_id
  - **Key columns**:
    - call_id, chunk_id
    - chunk_start_sec, chunk_end_sec
    - chunk_text
    - source_language, target_language (if known)
    - turn_count_in_chunk
    - chunk_version, run_id

### Operational Outputs
- `ops_file_status` stage `insights_03_build_text_chunks` SUCCESS/FAILED per call_id

## Business Rules / Logic
Eligibility:
- incremental mode: missing/FAILED insights_03_build_text_chunks
- requires turns; if missing, mark FAILED

Chunk formation:
- sort turns by start_sec
- build chunks according to strategy:
  - turn_count: N turns per chunk
  - time_window: group turns within a fixed seconds window
  - hybrid: stop when hitting either max turns or max seconds
- chunk_text formatting:
  - if include_role_prefix: prefix each line with role (AGENT/CUSTOMER/UNKNOWN)
  - include minimal timestamps only if helpful (optional)

Idempotency:
- upsert by call_id + chunk_version (policy-defined)
- avoid duplication on re-run

## Error Handling & Failure Isolation
- if a call has no usable turns, mark FAILED
- per-call failure does not stop batch

## Data Quality Checks (Task-Level)
CRITICAL:
- chunk_text not null and not empty for SUCCESS
- chunk_start_sec < chunk_end_sec
WARNING:
- chunks with extremely small text (possible upstream issue)

## Acceptance Criteria
- For one call_id, multiple chunks exist for longer calls (or 1 chunk for short calls).
- Chunks are ordered and cover the call reasonably.
- `ops_file_status` updated for insights_03_build_text_chunks stage.

## Manual Verification Steps
1. Choose a call_id with > 15 turns.
2. Run chunk task.
3. Query `silver_text_chunks` and verify chunk counts and text formatting.
