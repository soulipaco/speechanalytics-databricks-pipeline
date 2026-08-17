# Deterministic synthetic audio fixture

This directory provides a public, non-confidential path for exercising the
pipeline's file-ingestion contract. Generate a small PCM WAV fixture with:

```bash
python samples/generate_synthetic_wav.py --output samples/generated/synthetic_support_call.wav
```

Upload the generated file to the configured Unity Catalog Volume under
`<volume_root>/bronze/audio_raw/` before running `workflows/smoke_test_job.json`.
The generated waveform contains alternating synthetic tones and silence. It is
useful for deterministic ingestion, path, hashing, and audio-container checks;
it is intentionally not claimed as speech or as evidence of ASR accuracy.

Generated audio is ignored by git. Replace it with approved public or synthetic
spoken audio when validating transcription and downstream semantic stages.
