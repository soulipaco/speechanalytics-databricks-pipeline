# Validation Record

Date: 2026-08-17

## Current maturity

**Working prototype.** The repository has deterministic local verification and
GitHub Actions quality gates. It does not have a recorded successful Databricks
pipeline execution, so it is not presented as released or production-ready.

## Verified locally

The following commands pass on Python 3.11:

```bash
python -m compileall -q notebooks samples tools tests
python tools/validate_repo.py
python -m unittest discover -s tests -v
```

Verified contracts:

- all 16 notebook-source Python files compile;
- both 16-stage workflow JSON documents parse and retain the canonical linear DAG;
- every notebook task uses a relative Git-source path backed by a tracked file;
- `catalog`, `schema`, and `volume_root` are job parameters;
- Foundation 01/02/03 receive the required `volume_root` parameter;
- all four taxonomy files parse, include required fields, and have unique labels;
- the final gold insights DDL excludes raw transcript-like columns;
- the quality gate retains explicit raw-text column rejection;
- the public synthetic WAV generator is deterministic and produces mono,
  16-bit, 16 kHz audio.

The unit suite contains five deterministic contract tests. GitHub Actions also
runs the same compile, validator, and test commands plus a full-history Gitleaks
scan.

## Live execution status

No Databricks job was run in this validation pass. The authenticated development
workspace did not expose an existing compute resource or the template's default
schema/Volume. Creating new billable compute or unapproved Unity Catalog
resources solely to manufacture evidence was outside the safe smoke-test
boundary.

Residual live-validation steps:

1. select approved workspace compute;
2. provision or identify an approved catalog, schema, and Volume;
3. upload non-sensitive spoken audio for ASR/semantic validation (the generated
   tone fixture covers ingestion only);
4. run the smoke workflow and capture a sanitized run ID and quality results;
5. keep the prototype label until those steps succeed.
