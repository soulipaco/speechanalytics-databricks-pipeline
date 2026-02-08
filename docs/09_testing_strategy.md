# Testing Strategy — Speech Analytics Lakehouse on Databricks

## 1. Purpose

This document defines a testing approach for the Speech Analytics Lakehouse that is:

- **Professional** (clear quality gates, measurable checks, auditable results)
- **Lightweight** (appropriate for a personal Databricks free-tier environment)
- **Data-first** (validates tables and outputs rather than relying only on unit tests)
- **Incremental** (supports partial pipeline progress and re-runs)

The system is tested at three levels:
1. **Smoke tests** (pipeline runs end-to-end on small sample set)
2. **Data quality checks** (table-level validation rules)
3. **Regression tests** (optional snapshots for stability)

---

## 2. Testing Philosophy

### 2.1 “Tables are the contract”
The primary outputs are Delta tables. Therefore, the most valuable tests confirm:
- required tables exist
- schema expectations are met (at least for required columns)
- rows are present when expected
- cross-table relationships are consistent

### 2.2 Fail fast where it matters; isolate where it doesn’t
- **File-level failures** (corrupt audio, decode errors) should not crash the entire run.
- **Pipeline-level failures** should occur when:
  - all files fail in a stage
  - critical quality gates fail (configurable)

### 2.3 Test small, then scale
Always validate with:
- 1 file → 3 files → 10 files
before expanding.

This is critical for free-tier constraints and ML model performance variability.

---

## 3. Test Environments & Datasets

### 3.1 Environments
- **Dev/Sample mode**: small runs, frequent iteration
- **Incremental mode**: realistic repeated runs with manifest-based eligibility
- **Full mode**: rare, used for major version upgrades

### 3.2 Sample dataset requirements
The sample dataset should include:
- at least 1 English call
- at least 1 non-English call (e.g., Turkish/Greek/German)
- at least 1 multi-speaker call (diarization relevant)
- at least 1 call containing deliberate test PII tokens (fictional):
  - phone number, email, person name, address-like phrase

All sample calls must be **synthetic or public licensed** to remain safe for a public repo.

---

## 4. Test Levels

## 4.1 Level 1 — Smoke Tests (Must Have)

Smoke tests validate that workflows run and produce outputs.

### Smoke Test A — Foundation Workflow End-to-End (1 file)
**Goal**: Confirm the pipeline runs from ingestion to redacted turns (and translation stage behavior).

**Steps**
1. Place one `.wav` file into Bronze audio volume.
2. Run Foundation Workflow in `run_mode=sample`, `max_files_per_run=1`.
3. Verify downstream tables contain that `call_id`.

**Expected results**
- `bronze_audio_files` contains the file with status NEW → PROCESSED (after finalize)
- `silver_asr_segments` has rows for call_id
- `silver_turns_aligned` has rows for call_id
- `gold_turns_redacted` has rows for call_id
- `gold_turns_translated` exists if translation enabled (and respects skip logic)

**Pass criteria**
- workflow status SUCCESS
- at least one row exists in each expected table for that call_id

---

### Smoke Test B — Insights Workflow End-to-End (1 file)
**Goal**: Confirm metrics and LLM insights can run after foundation outputs exist.

**Steps**
1. Ensure Foundation outputs exist for one call_id.
2. Run Insights Workflow in `run_mode=sample`, `max_files_per_run=1`.
3. Verify `gold_conversation_metrics` and `gold_speech_insights`.

**Pass criteria**
- metrics row exists for call_id
- insights row exists for call_id (if LLM enabled)
- taxonomy labels are valid (see quality checks)

---

### Smoke Test C — Incremental Re-run (No duplicates)
**Goal**: Confirm idempotency and incremental behavior.

**Steps**
1. Run Foundation Workflow again with same dataset.
2. Verify no duplicate segments/turns are created.

**Pass criteria**
- row counts do not double for already processed call_id
- status remains consistent
- run logs show “0 eligible files” or “skipped already processed” behavior

---

## 4.2 Level 2 — Data Quality Checks (Core Quality Gates)

Data quality checks validate correctness and consistency of table content. These checks should be performed after key stages and recorded per run.

### 4.2.1 Quality Check Reporting
Recommended options:
- Store a `qa_quality_results` table with:
  - run_id, check_name, table_name, severity, passed_flag, failed_count, details
- Or store summarized results in `ops_pipeline_runs`

Severity levels:
- **CRITICAL**: fail the workflow if check fails (configurable)
- **WARNING**: allow completion but record failure and flag output

---

## 5. Table-Level Quality Checks (Minimum Set)

## 5.1 Bronze: `bronze_audio_files`
**Checks**
- `duration_sec > 0`
- `file_hash` is not null
- `call_id` is unique (or unique by policy)
- `file_path` not null and appears under configured volume root

**Severity**
- CRITICAL for null/invalid duration and missing file_hash

---

## 5.2 Silver: `silver_asr_segments`
**Checks**
- `start_sec < end_sec` for all rows
- `text` not null (empty allowed only if explicitly tagged)
- `call_id` exists in bronze
- basic coverage sanity:
  - total ASR segment time should not exceed call duration + tolerance

**Severity**
- CRITICAL for timing violations
- WARNING for unusually low transcript coverage (possible no-speech calls)

---

## 5.3 Silver: `silver_diarization_segments`
**Checks**
- `start_sec < end_sec`
- `method` in allowed set (pyannote / vad_fallback)
- segments within call duration tolerance
- no extreme overlaps unless policy allows

**Severity**
- CRITICAL for timing violations
- WARNING for “too few segments” (diarization failure indicator)

---

## 5.4 Silver: `silver_turns_aligned`
**Checks**
- `start_sec < end_sec`
- `speaker_label` not null
- `role` in allowed enum
- `language_final` not null
- turn text non-null (empty only if policy allows)

**Severity**
- CRITICAL for missing language_final or invalid timings
- WARNING for high proportion of Unknown roles (heuristic needs improvement)

---

## 5.5 Gold: `gold_turns_redacted`
**Checks**
- `text_redacted` not null
- `pii_found_flag` not null
- If `pii_found_flag=true` then:
  - `pii_entity_counts` not empty OR placeholders present in text
- If `pii_residual_risk_flag=true`:
  - call is flagged for review (not necessarily failure)

**Severity**
- CRITICAL if redaction stage produces null text
- WARNING for residual risk (unless strict mode enabled)

---

## 5.6 Gold: `gold_turns_translated`
**Checks**
- If translation enabled:
  - `translation_target_language` not null
  - `translation_skipped_flag` not null
- Skip logic:
  - If `language_final == translation_target_language` then `translation_skipped_flag=true`
- If skipped_flag=false then `text_translated` must be non-null and non-empty

**Severity**
- CRITICAL for skip rule violation (logic regression)
- WARNING for translation quality issues (optional)

---

## 5.7 Gold: `gold_conversation_metrics`
**Checks**
- all metric fields non-negative
- `agent_talk_time_sec + customer_talk_time_sec <= total_duration_sec + tolerance`
- `silence_time_sec <= total_duration_sec`
- turn counts match turns table aggregated counts (within tolerance if filtering applies)

**Severity**
- CRITICAL for negative values
- WARNING for large inconsistencies (documented assumptions may explain)

---

## 5.8 Gold: `gold_speech_insights`
**Checks**
- required fields not null (summary, labels, outcomes)
- taxonomy labels must exist in active dimension tables for `taxonomy_version`
- controlled enums:
  - resolution in {Resolved, Not resolved}
  - effort in {High, Low}
  - sentiment in {Positive, Neutral, Negative}
- numeric ranges:
  - love scores in [1,10]
  - confidence scores in [0,1] (if numeric)

**Severity**
- CRITICAL for invalid enums or missing required fields
- WARNING for low confidence results (policy-defined)

---

## 6. Workflow Quality Gates

Quality gates determine whether a workflow run should be marked FAILED.

### 6.1 Foundation Workflow Gate
Recommended default:
- FAIL if CRITICAL checks fail
- SUCCEED with warnings if only WARNING checks fail
- Always update run summary with counts and failure reasons

### 6.2 Insights Workflow Gate
Recommended default:
- FAIL if taxonomy validation fails (CRITICAL)
- FAIL if LLM output schema contract fails (CRITICAL)
- SUCCEED with warnings for low confidence (WARNING)

Configurable parameter:
- `fail_run_if_quality_checks_fail = true/false`

---

## 7. Regression Testing (Optional but Portfolio-Strong)

Regression tests detect output drift when models or prompts change.

### 7.1 Snapshot strategy
For 2–3 synthetic calls, store a “golden snapshot” of:
- selected fields from `gold_turns_redacted`
- selected fields from `gold_speech_insights`
- taxonomy version and prompt version

Regression check compares:
- required fields exist
- labels remain within expected set
- summary length constraints remain within bounds
- skip logic still holds

### 7.2 Handling expected drift
If a model upgrade is intentional:
- bump `prompt_version` / `model_version`
- record that regression differences are expected
- preserve old snapshots for traceability

---

## 8. Manual Review Checklist (Minimal)

Some outputs benefit from manual sanity checks:
- diarization plausibility on multi-speaker calls
- redaction correctness for inserted test PII
- translation correctness for non-English calls
- taxonomy label plausibility for a small sample

Manual reviews should be documented as:
- “known limitations” and “expected behavior” in README or docs.

---

## 9. Known Limitations and How Tests Reflect Them

- Diarization can be imperfect; tests focus on **valid segmentation** rather than perfect speaker identity.
- Multilingual PII detection can be imperfect; tests focus on **layered masking and residual risk flags**.
- LLM outputs can vary; tests enforce **schema + allowed labels** rather than exact wording.

---

## 10. Definition of Done (Testing)

Testing is considered complete for v1 when:
- Smoke tests pass for Foundation and Insights workflows on a small multilingual sample set.
- All CRITICAL data quality checks pass.
- WARNING checks are recorded and documented.
- A basic quality report per run is stored and queryable.

---
