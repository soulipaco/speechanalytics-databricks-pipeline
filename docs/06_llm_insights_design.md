# LLM Insights Design — Speech Analytics Lakehouse on Databricks (docs/06_llm_insights_design.md)

## Status of Documentation So Far

The following documentation sections are complete drafts and can be committed as-is:
- `docs/00_roadmap.md`
- `docs/01_problem_statement.md`
- `docs/03_data_model.md`
- `docs/04_workflows.md`
- `docs/05_security_and_pii.md`
- `docs/09_testing_strategy.md`

This document (`docs/06_llm_insights_design.md`) introduces the **LLM Insights Layer** design. It assumes the Foundation pipeline already produces compliance-safe text (redacted and optionally translated).

---

## 1. Purpose

This document defines the design and constraints of the **LLM Insights Layer**, which generates structured speech analytics outputs from call transcripts. The design prioritizes:

- **Reliability**: consistent labels and schemas across calls
- **Auditability**: versioning for prompts, taxonomies, and models
- **Safety**: LLM consumes redacted (and optionally translated) text by default
- **Scalability**: chunking and retrieval (RAG) to handle long calls effectively

The LLM Insights Layer outputs a single curated record per call in `gold_speech_insights` and can optionally emit supporting artifacts (per-chunk classifications, rationales, retrieval references).

---

## 2. Inputs and Data Boundary

### 2.1 Allowed input text sources (default)
The LLM must consume **compliance-safe** text only:
- Primary: `gold_turns_translated.text_translated` (if translation enabled)
- Else: `gold_turns_redacted.text_redacted`

Raw/unredacted transcript text is out of scope for LLM consumption in v1.

### 2.2 Input scope per call
The call is represented as:
- ordered turns with timestamps and roles (Agent/Customer/Unknown)
- or chunks constructed from turns (see chunking strategy)

**Rationale**: turn-based representation enables role-aware insights and robust chunking.

---

## 3. Outputs (What the LLM Must Produce)

### 3.1 Required output fields (call-level)
The LLM must produce all required fields below; missing values are treated as schema failure.

**Compliance check (secondary)**
- `pii_possible_remaining_flag`: boolean
- `pii_notes`: short optional note (max 1–2 sentences)

**Summary**
- `summary_text`: concise structured summary (2–5 bullets OR 4–8 sentences; format policy-defined)

**Taxonomy classification**
- `contact_driver_label`
- `issue_label`
- `intent_label`

**Outcome classification**
- `resolution`: one of {`Resolved`, `Not resolved`}
- `effort`: one of {`High`, `Low`}
- `sentiment`: one of {`Positive`, `Neutral`, `Negative`}

**Emotion timeline**
- `customer_emotion_start`
- `customer_emotion_end`
- `agent_emotion_start`
- `agent_emotion_end`

**Scores**
- `agent_love_score_1_10`: integer 1–10
- `brand_love_score_1_10`: integer 1–10

**Confidence**
- `contact_driver_confidence` in [0,1]
- `issue_confidence` in [0,1]
- `intent_confidence` in [0,1]
- `resolution_confidence` in [0,1]
- `effort_confidence` in [0,1]
- `sentiment_confidence` in [0,1]

**Provenance**
- `llm_model_name`
- `prompt_version`
- `taxonomy_version`
- `rag_enabled_flag`
- `rag_top_k`
- `run_id`

### 3.2 Controlled vocabularies
The following must be constrained:

**Taxonomies**
- `contact_driver_label` must be from `dim_contact_driver` where active_flag=true and taxonomy_version matches
- `issue_label` must be from `dim_issue` (active)
- `intent_label` must be from `dim_intent` (active)

**Enums**
- resolution: Resolved / Not resolved
- effort: High / Low
- sentiment: Positive / Neutral / Negative

**Emotion catalog**
- emotions must be from `dim_emotion_catalog` where active_flag=true and catalog_version matches

**Rationale**: Constrained outputs reduce hallucinations and ensure analytics consistency.

---

## 4. Emotion Model (Catalog-Driven)

### 4.1 Emotion catalog design
The Emotion Catalog provides a controlled set of emotions with sentiment grouping:
- ~30 emotions for Positive
- ~30 emotions for Neutral
- ~30 emotions for Negative

Each emotion includes:
- `emotion_name`
- `sentiment_group`
- `polarity_score` (e.g., -1..+1)
- definition and examples

### 4.2 Emotion timeline outputs
The LLM outputs start and end emotions for each role:
- Customer Emotion Start/End
- Agent Emotion Start/End

Optional (derived, not necessarily LLM):
- emotion shift score = end_score - start_score (computed using polarity_score mapping)

---

## 5. Handling Long Calls: Chunking + Aggregation

### 5.1 Why chunking is required
Long calls exceed typical LLM context windows and reduce accuracy. The system must chunk input text and aggregate results.

### 5.2 Chunking strategy (v1)
Create `silver_text_chunks` from turns using one of:
- **Time window chunking** (e.g., 60 seconds per chunk)
- **Turn-count chunking** (e.g., 10–15 turns per chunk)
- **Hybrid**: max turns or max time, whichever first

Each chunk includes:
- chunk_id, call_id
- start/end timestamps
- chunk_text
- role distribution metadata (optional)

### 5.3 Aggregation strategy (v1)
Two-step approach:

1) **Chunk-level extraction**
   - Summarize each chunk
   - Identify candidate taxonomies per chunk with confidence
   - Identify resolution signals and sentiment signals per chunk

2) **Call-level consolidation**
   - Combine chunk signals into final call-level decisions using rules:
     - taxonomy label = most confident label across chunks (or weighted by recency)
     - resolution = evaluate final segments more heavily (last 20–30% of call)
     - sentiment/emotions = start from early chunks, end from late chunks
   - generate final summary from chunk summaries (structured reduction)

**Note**: v1 can implement consolidation as either:
- a deterministic rule engine, or
- a second “consolidation prompt” that consumes chunk summaries + retrieved context (recommended for clarity).

---

## 6. RAG (Vector Retrieval) Integration

### 6.1 RAG objectives
RAG improves:
- taxonomy classification consistency
- focus on relevant evidence for resolution/effort/sentiment
- handling long calls without feeding entire transcript

### 6.2 Retrieval sources
Two retrieval channels are recommended:

1) **In-call retrieval**
- Retrieve the top-k most relevant chunks from the same call for a given query (e.g., “main issue”, “resolution evidence”).
- This ensures the LLM focuses on the most informative call sections.

2) **Taxonomy example retrieval**
- For each taxonomy label, store examples and embed them.
- Retrieve top-k examples most similar to the call content.
- Provide these examples in the prompt as “reference categories” to anchor classification.

### 6.3 Retrieval metadata (must be stored)
For auditability, store:
- retrieved chunk_ids
- retrieved taxonomy labels/examples identifiers
- similarity scores (optional)

This metadata may be stored in a supporting table, e.g.:
- `silver_rag_retrieval_log` (optional)

---

## 7. Prompt Design (Contracts and Guardrails)

### 7.1 Prompt components (conceptual)
A robust LLM prompt for this system must include:

1) Role and objective:
- “You are an analytics extraction system…”

2) Data boundary rules:
- “Input is already redacted; do not attempt to infer exact PII…”

3) Allowed label constraints:
- “Choose ONLY from these labels…”

4) Output schema contract:
- “Return JSON with the following keys…”

5) Confidence scoring rules:
- “Confidence is 0–1 and reflects certainty…”

6) Evidence policy:
- Provide short, non-sensitive rationales where helpful (optional)
- Avoid quoting large transcript chunks

### 7.2 Output must be schema-valid
The LLM output is considered invalid if:
- missing required fields
- contains labels not in taxonomy lists
- includes disallowed enums
- violates numeric ranges (confidence, scores)

Invalid outputs should:
- be re-tried with stricter instructions (optional)
- or marked FAILED for that call/stage (logged in ops tables)

### 7.3 Prompt versioning
Every time prompt instructions change meaningfully:
- increment `prompt_version`
- store it on every output row in `gold_speech_insights`

---

## 8. Model Versioning and Reproducibility

### 8.1 Required provenance
Store per run:
- `llm_model_name`
- `prompt_version`
- `taxonomy_version`
- `rag_enabled_flag`, `rag_top_k`

### 8.2 Controlled drift policy
LLM outputs may drift with:
- model upgrades
- taxonomy changes
- prompt changes

Drift is acceptable only when:
- versions are updated
- changes are documented
- regression snapshots are updated (if used)

---

## 9. Safety and Compliance Rules for LLM Insights

### 9.1 PII handling
- The LLM is not the primary PII detector.
- LLM may flag “possible remaining PII” but should not attempt to reconstruct PII.

### 9.2 Data minimization
- Prefer chunked context over full call text.
- Provide only the minimal necessary text to answer each extraction.

### 9.3 Public demo safety
When publishing example insights:
- avoid including direct transcript text in the repo
- include only aggregated outputs and taxonomy labels
- ensure synthetic calls do not include real names/addresses

---

## 10. Supporting Artifacts (Optional Tables)

If additional transparency is desired, add:

### 10.1 `silver_llm_chunk_insights` (optional)
**Grain**: one row per call_id + chunk_id  
Stores chunk-level intermediate outputs:
- chunk summary
- candidate driver/issue/intent labels with confidences
- sentiment/emotion signals

### 10.2 `silver_rag_retrieval_log` (optional)
**Grain**: one row per call_id + query_type  
Stores:
- retrieved chunks
- retrieved taxonomy examples
- retrieval parameters and scores

These tables help debugging and portfolio demonstration.

---

## 11. Quality Checks Specific to LLM Outputs

Critical checks:
- labels must exist in active taxonomies
- enums must be valid
- confidence in [0,1]
- love scores in [1,10]
- required fields non-null

Warnings:
- very low confidence outputs (policy-defined threshold)
- contradictory outcomes (e.g., “Resolved” but summary indicates unresolved)

---

## 12. Definition of Done (LLM Insights Layer)

The LLM Insights Layer is complete when:
- `gold_speech_insights` is produced for each eligible call_id
- outputs comply with schema and taxonomy constraints
- provenance fields are present and correct
- RAG integration is documented and retrieval metadata is stored (if enabled)
- testing strategy includes at least:
  - one multilingual example
  - one long-call example (chunking demonstrated)
  - one PII test case (fictional)

---
