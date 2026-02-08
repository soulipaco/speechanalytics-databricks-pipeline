# INSIGHTS_04_EMBED_CHUNKS (docs/tasks/insights_04_embed_chunks.md)

## Task ID
- **ID**: INSIGHTS_04_EMBED_CHUNKS

## Title
Generate embeddings for text chunks

## Objective
Create vector embeddings for each chunk so the system can retrieve the most relevant portions of a call (RAG) and optionally retrieve taxonomy examples for better classification consistency.

## Scope
### In Scope
- Select eligible chunks from `silver_text_chunks`.
- Generate embeddings using the configured embedding model.
- Store embeddings and metadata in `silver_embeddings`.
- Update `ops_file_status` stage `insights_04_embed_chunks`.

### Out of Scope
- Creating a Vector Search index (separate task).
- LLM inference.

## Inputs
### Data Inputs
- Table: `silver_text_chunks`

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `run_mode` (string, required)
- `max_files_per_run` (int, optional)
- `enable_embeddings` (bool, default false)
- `embedding_model_name` (string, required when enabled)
- `embedding_version` (string, default `v1`)
- `embedding_dim` (int, optional)  # validate if known
- `embedding_storage_format` (string, default `array_float`)
  - `array_float` | `binary` (policy-defined)

## Outputs
### Data Outputs
- Table: `<catalog>.<schema>.silver_embeddings`
  - **Grain**: one row per call_id + chunk_id
  - **Key columns**:
    - call_id, chunk_id
    - embedding_vector
    - embedding_model_name, embedding_version
    - chunk_version
    - run_id

### Operational Outputs
- `ops_file_status` stage `insights_04_embed_chunks` SUCCESS/FAILED/SKIPPED per call_id

## Business Rules / Logic
- If `enable_embeddings=false`, stage is SKIPPED.
- Eligibility:
  - chunks exist and are not empty
  - incremental mode processes chunks missing embeddings or previously FAILED

Idempotency:
- upsert by (call_id, chunk_id, embedding_model_name, embedding_version)

## Error Handling & Failure Isolation
- If embedding fails for a chunk:
  - record failure (optionally at chunk-level table if you track it)
  - call-level status may be WARNING or FAILED (policy-defined)
- Task fails only if all eligible embeddings fail.

## Data Quality Checks (Task-Level)
CRITICAL:
- embedding_vector not null when SUCCESS
- embedding vector length matches expected dim if provided
WARNING:
- chunk_text unusually long or short (may affect embedding quality)

## Acceptance Criteria
- For at least one call_id, embeddings exist for each chunk.
- SKIPPED behavior occurs when embeddings disabled.
