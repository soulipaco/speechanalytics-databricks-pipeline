# Taxonomies (docs/07_taxonomies.md)

## Status
This section defines the controlled label systems used by the Insights pipeline.  
The taxonomy files are stored under the repository folder:

- `taxonomies/contact_drivers.yml`
- `taxonomies/issues.yml`
- `taxonomies/intents.yml`
- `taxonomies/emotions.yml`

These YAML files are the **source of truth** for category definitions used by the LLM layer and reporting.

---

## 1. Purpose

Taxonomies solve a key problem in speech analytics:

- LLMs can be inconsistent (“Billing problem” vs “Payment issue” vs “Charges”).
- Reporting requires **stable labels** for trend analysis.
- Category sets must be easy to update without rewriting code.

This project uses **controlled taxonomies** so that:
- The LLM must select from a defined set of labels (guardrail).
- Labels have definitions and examples (improves accuracy).
- The taxonomy can evolve via versioning.

---

## 2. Taxonomy Types in This Repo

### 2.1 Contact Drivers (High-level “Why they contacted”)
**File:** `taxonomies/contact_drivers.yml`  
**Use:** Top-level categorization (executive reporting, dashboards)

Example labels:
- Billing and Payments
- Account Access
- Technical Support

### 2.2 Issues (Concrete “What went wrong”)
**File:** `taxonomies/issues.yml`  
**Use:** More specific classification (root cause trends)

Example labels:
- Payment Failed
- Login Failure
- App Crash or Freeze

### 2.3 Intents (Requested outcome “What the customer wants”)
**File:** `taxonomies/intents.yml`  
**Use:** Customer goal identification (workflow optimization)

Example labels:
- Request Refund
- Restore Account Access
- Track or Update an Order

### 2.4 Emotions (Controlled emotion catalog for start/end emotions)
**File:** `taxonomies/emotions.yml`  
**Use:** Start/End emotion labeling for Customer + Agent

Notes:
- Emotion values are grouped as Positive / Neutral / Negative.
- Each emotion includes a polarity score (e.g., -1..+1) to enable numeric rollups.

---

## 3. Standard YAML Structure (Contract)

All taxonomy YAML files follow the same design principles:

### 3.1 Versioned metadata
Each file includes:
- `name`
- `version`
- `description`
- selection rules

**Why:** supports reproducibility (you can trace which version produced which outputs).

### 3.2 Active flag
Each taxonomy item has:
- `active: true/false`

**Why:** allows soft-deprecation without breaking pipelines or historical runs.

### 3.3 Definitions + examples
Each label includes:
- clear definition
- synonyms
- example utterances

**Why:** improves LLM label selection and human interpretability.

---

## 4. How Taxonomies Are Used in the Pipeline

### 4.1 Loading strategy
At runtime, the Insights pipeline loads YAML taxonomies into dimension tables such as:
- `dim_contact_driver`
- `dim_issue`
- `dim_intent`
- `dim_emotion_catalog`

The dimension tables store:
- label name
- definition/synonyms/examples
- active flag
- taxonomy version

### 4.2 LLM guardrails
The LLM is instructed:
- “Choose labels ONLY from the active taxonomy list”
- “If uncertain, choose the closest and lower confidence”
- “Do not invent new labels”

If the LLM returns a label not in the taxonomy:
- the output is considered invalid (CRITICAL quality check)
- the call can be retried with stricter prompting or marked failed (policy-defined)

---

## 5. Updating Taxonomies Safely

### 5.1 Add a new label
When adding a label:
1. Add label to the appropriate YAML file
2. Include definition + synonyms + 2–5 examples
3. Keep `active: true`
4. Bump taxonomy `version` (recommended when behavior meaningfully changes)

### 5.2 Deprecate a label
Instead of deleting:
- set `active: false`
- keep the definition for historical traceability

### 5.3 Replace or restructure labels
If you do major label changes:
- bump `version` (e.g., v1.0 → v2.0)
- keep old versions in Git history
- document the change in `CHANGELOG.md` (optional but recommended)

---

## 6. Quality Rules (Enforced)

### 6.1 Mandatory rules
- Contact Driver / Issue / Intent: must output exactly one label each
- Labels must be in the active taxonomy set (for the selected version)
- Emotion start/end must be from the emotion catalog
- Confidence fields must be in range [0, 1]

### 6.2 Recommended rules (warnings)
- Too many “Other” classifications should be flagged (taxonomy too small or prompt needs tuning)
- Extremely low confidence trend should be flagged (RAG or examples may need improvement)

---

## 7. Portfolio Demo Guidance

To keep the project safe and shareable:
- Use synthetic calls and fictional scenarios
- Avoid domain-specific sensitive labels from real employers/clients
- Prefer generic customer support labels (billing, account, orders, tech issues)

---

## 8. Next Related Docs

- `docs/06_llm_insights_design.md` — explains how taxonomies constrain LLM output
- `docs/08_vector_search_rag.md` — shows how taxonomy examples can be used in retrieval for improved accuracy
- `docs/09_testing_strategy.md` — includes taxonomy validation as critical checks
