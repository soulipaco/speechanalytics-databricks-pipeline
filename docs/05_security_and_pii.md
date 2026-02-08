# Security & PII — Speech Analytics Lakehouse on Databricks

## 1. Purpose

This document defines the security model and PII handling strategy for the Speech Analytics Lakehouse. The project is designed as a portfolio-grade reference architecture, but it follows enterprise-style compliance patterns:

- Raw audio and unredacted transcripts are treated as **sensitive**.
- Analytics and LLM consumption use **redacted** text by default.
- Secrets are managed outside the repository.
- PII detection is layered to reduce false negatives, especially in multilingual settings.

This project does not claim compliance certification; it demonstrates technical controls and best practices.

---

## 2. Data Sensitivity Classification

### 2.1 Data Categories
1. **Raw Audio** (`.wav`)
   - May contain voices, names, addresses, phone numbers, account numbers.
   - Considered highly sensitive.

2. **Unredacted Transcript Text**
   - Likely contains PII and personal context.
   - Considered highly sensitive.

3. **Redacted Transcript Text**
   - Primary analytics product.
   - Safer for BI and LLM usage.
   - Still may contain contextual sensitive information (non-PII).

4. **Derived Insights**
   - Summaries, labels, sentiments, emotions.
   - May still reveal sensitive content about a person or issue.
   - Must be treated carefully in public demos.

### 2.2 Public Repository Policy (GitHub)
The public repository must not contain:
- real customer audio
- real customer transcripts
- any secrets/tokens
- private URLs or internal workspace identifiers
- any content that could identify a real person

Allowed in the repository:
- architecture docs and data model docs
- taxonomy YAML files (drivers, issues, intents, emotions)
- synthetic audio generated from fictional scripts (if licensing permits)
- small sample outputs with fictional data and no PII

---

## 3. Storage Security (Unity Catalog + Volumes)

### 3.1 Recommended storage layout
- Bronze raw audio:
  - `Volumes/<catalog>/<schema>/bronze/audio_raw/`
- Optional Silver preprocessed audio:
  - `Volumes/<catalog>/<schema>/silver/audio_preprocessed/`
- Intermediate artifacts (optional):
  - `Volumes/<catalog>/<schema>/silver/artifacts/`

### 3.2 Access boundaries (Conceptual)
- Restrict raw audio paths and unredacted transcript tables to “high trust” users only.
- Publish redacted/translated Gold tables as the default analytic surface.

Even in a personal free tier environment, the design should document intended access boundaries to demonstrate governance awareness.

---

## 4. Secrets Management

### 4.1 Secrets that may be required
- Hugging Face token (for diarization models, translation models, or downloads)
- API keys for LLM endpoints (if used)
- any third-party service keys

### 4.2 Rules
- Never hardcode secrets in notebooks or scripts.
- Never commit secrets to GitHub.
- Use one of the following:
  - Databricks secret scope
  - environment variables (via `.env` locally, but `.env` must be gitignored)

### 4.3 Repo hygiene
- Provide `.env.example` with placeholders only.
- Add `.env`, `*.token`, `secrets.json` to `.gitignore`.
- Scan notebooks for accidental tokens before committing.

---

## 5. PII Handling Strategy (Layered Defense)

### 5.1 Why layered defense is necessary
- PII patterns vary by language, formatting, and transcription quality.
- Presidio is strong but not universal across all languages.
- ASR errors can alter PII patterns (e.g., misrecognized digits).
- LLMs are non-deterministic and should not be the only compliance gate.

Therefore the project uses multiple layers:

1. **Primary detection**: Microsoft Presidio
2. **Secondary detection**: regex rules for universal patterns
3. **Residual risk scan**: post-redaction verification
4. **LLM-assisted check** (optional): as a secondary signal only

---

## 6. Primary PII Detection and Redaction (Microsoft Presidio)

### 6.1 Detection
Presidio Analyzer identifies entity types such as:
- PERSON
- PHONE_NUMBER
- EMAIL_ADDRESS
- CREDIT_CARD
- LOCATION
- ID / NATIONAL_ID / PASSPORT (if configured)
- DATE_TIME (policy dependent)

### 6.2 Redaction / Anonymization
Use consistent placeholders to support downstream analytics:
- `[PERSON]`, `[PHONE]`, `[EMAIL]`, `[ADDRESS]`, `[CARD]`, `[ID]`

This consistency enables:
- counting PII occurrences
- filtering calls with PII
- verifying redaction policy

### 6.3 What to store
In `gold_turns_redacted` store:
- `pii_found_flag`
- `pii_entities` (type + offsets if available)
- `pii_entity_counts` (map entity_type → count)
- `redaction_version` (important for reproducibility)

---

## 7. Secondary PII Protection (Regex Rules)

Presidio may miss multilingual or unusual formats. Add deterministic rules for patterns that are mostly language-independent:

### 7.1 Universal patterns (examples)
- Email patterns (user@domain)
- Phone number patterns (various separators)
- URLs
- IBAN-like sequences
- Long digit sequences that resemble account numbers
- Credit-card-like sequences (with optional Luhn check in later versions)

### 7.2 Rule philosophy
- Prefer “safe over perfect” in analytics tables:
  - masking too much is preferable to leaking PII
- Keep rule sets versioned and documented:
  - `redaction_version` increments when rules change

---

## 8. Residual Risk Scanning (Post-Redaction)

After redaction, run a second pass that checks for patterns that should not remain.

### 8.1 Purpose
Detect potential false negatives and transcription artifacts such as:
- phone numbers not redacted because ASR inserted spaces
- emails partially redacted
- IDs that resemble random digit sequences

### 8.2 Output fields
- `pii_residual_risk_flag` boolean
- optional `pii_residual_notes` (short)

### 8.3 Policy
If `pii_residual_risk_flag=true`:
- call is marked for review
- insights workflow may either:
  - skip LLM stage for that call, or
  - proceed but flag results (configurable)

---

## 9. LLM Usage and PII Safety

### 9.1 Default rule
LLM tasks should use:
- redacted text (`gold_turns_redacted`) or
- redacted+translated text (`gold_turns_translated`)

LLM should not consume raw/unredacted text by default.

### 9.2 LLM-assisted PII detection (secondary only)
The LLM may be asked:
- “Is there any remaining PII visible in the provided redacted transcript?”

This output is treated as:
- an additional risk signal (`pii_possible_remaining_flag`)
- not as the primary compliance control

Reason:
- LLM output is probabilistic and can hallucinate or miss details.

### 9.3 Data minimization for prompts
To reduce accidental leakage:
- provide only necessary text (chunked)
- avoid including raw identifiers
- use shortest sufficient context for classification

---

## 10. Translation and PII

Translation occurs **after redaction** to avoid generating PII in the target language.
Policy:
- redact first
- translate redacted text
- never translate unredacted text

This reduces risk and simplifies compliance boundaries.

---

## 11. Public Demo / Portfolio Safety Guidelines

### 11.1 Recommended sample data
- synthetic calls generated from fictional scripts
- fictional names/addresses (or none)
- multiple languages to demonstrate pipeline generality

### 11.2 Avoid in public demos
- real customer audio
- real internal call drivers or confidential categories
- real account identifiers or company-sensitive references

### 11.3 Optional “demo mode”
A documented configuration mode can enforce:
- aggressive masking of digit sequences
- skipping calls flagged by residual risk
- disabling raw transcript storage entirely

---

## 12. Security Checklist (Pre-Commit)

Before pushing to GitHub:
- verify `.gitignore` contains `.env` and secret files
- search notebooks for tokens (HF, API keys)
- confirm no audio files are committed unless they are synthetic and licensed
- confirm sample outputs do not include PII or real-person identifiers
- confirm docs do not reference private internal URLs or identifiers

---
