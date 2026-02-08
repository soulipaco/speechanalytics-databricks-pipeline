# Legacy Reference: `Final.ipynb`

## A) Purpose of this doc

This document provides a public, sanitized reference for the legacy notebook at `legacy_private/Final.ipynb`.  
The legacy notebook is retained only as a local historical artifact to capture useful implementation patterns while the new project follows the structured design in `docs/tasks/*.md`.

## B) What `Final.ipynb` does

- Installs and imports ASR/diarization/translation dependencies inside notebook cells.
- Loads a Whisper ASR model with fallback behavior across model sizes and hardware modes.
- Initializes pyannote diarization when available and falls back when unavailable.
- Loads audio safely, normalizes channel layout, and resamples to 16 kHz.
- Runs diarization, then transcribes speech segments with fixed language assumptions.
- Aligns ASR segments to speaker segments and assigns speaker labels/roles.
- Translates transcribed text to English.
- Writes transcript outputs to local TXT, JSON, and Parquet files.

## C) Patterns worth reusing

- Whisper model load fallback ladder (large to small, GPU-first then CPU fallback).
- pyannote output normalization to handle `Annotation` and wrapper output variants.
- Safe audio loading with explicit mono conversion and 16 kHz resampling before diarization.
- Diarization fallback path (Silero VAD segmentation) when pyannote is unavailable/fails.
- Speaker overlay alignment using segment midpoint matching with nearest-speaker fallback.

## D) What NOT to copy into the new project

- Hardcoded source language assumptions (for example fixed Greek code).
- Translation always running without robust skip logic when source equals target.
- First-speaker-equals-agent role heuristic.
- Local filesystem writes (TXT/JSON/Parquet) to machine-specific Windows paths.
- `pip install` inside notebooks as an execution prerequisite.
- Hardcoded secrets/tokens in notebook-local configuration patterns.
- Notebook-coupled runtime behavior that bypasses workflow/task contracts.

## E) How Codex should use this legacy notebook

- Source of truth: `docs/tasks/*`
- Legacy reference: `legacy_private/Final.ipynb` (read-only)
- If conflict exists: follow `docs/tasks/*`

