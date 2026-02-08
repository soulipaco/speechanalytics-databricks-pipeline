# INSIGHTS_05_BUILD_VECTOR_SEARCH_RAG_INDEX (docs/tasks/insights_05_build_vector_search_rag_index.md)

## Task ID
- **ID**: INSIGHTS_05_BUILD_VECTOR_SEARCH_RAG_INDEX

## Title
Build retrieval layer for RAG (Vector Search or Delta-based retrieval)

## Objective
Enable retrieval of the most relevant text chunks (and optionally taxonomy examples) for a given query. This task prepares the system for RAG-enhanced LLM inference.

## Scope
### In Scope
- Define and implement the project’s retrieval approach:
  - Option A: Databricks Vector Search index
  - Option B: Delta-based retrieval (cosine similarity) for free-tier portability
- Store retrieval metadata and versioning.

### Out of Scope
- LLM inference itself.

## Inputs
### Data Inputs
- `silver_text_chunks`
- `silver_embeddings`

### Configuration Inputs (Parameters)
- `catalog` (string, required)
- `schema` (string, required)
- `run_id` (string, required)
- `enable_rag` (bool, default false)
- `rag_backend` (string, default `delta_similarity`)
  - `vector_search` | `delta_similarity`
- `rag_top_k` (int, default 5)
- `rag_version` (string, default `v1`)

## Outputs
### Data Outputs
- If `rag_backend=vector_search`:
  - a vector search index resource (external to Delta tables)
- If `rag_backend=delta_similarity`:
  - no new required tables; retrieval is computed on the fly
- Optional: `<catalog>.<schema>.silver_rag_retrieval_log`
  - stores query_type, retrieved chunk_ids, similarity scores, run_id

### Operational Outputs
- `ops_pipeline_runs` updated with retrieval backend and parameters used

## Business Rules / Logic
- If `enable_rag=false`, stage is SKIPPED.
- Retrieval scope:
  - v1: “in-call retrieval” (chunks within same call_id)
  - optional: taxonomy example retrieval if examples are embedded and stored

Auditability:
- store rag_enabled_flag, top_k, backend, and version in downstream insights outputs.

## Error Handling
- If retrieval backend creation fails (vector search), fail the stage and disable RAG for that run (policy choice).
- Delta-based retrieval should rarely fail; treat missing embeddings as a precondition failure.

## Data Quality Checks
CRITICAL:
- embeddings exist for chunks when enable_rag=true
WARNING:
- insufficient chunks for a call_id (RAG less useful)

## Acceptance Criteria
- When enabled, the system can retrieve top-k chunk_ids for a call query deterministically.
- Downstream LLM stages can record retrieval metadata.
