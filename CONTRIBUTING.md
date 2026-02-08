# Contributing

## Scope
Contributions should improve documentation, configuration scaffolding, quality checks, and implementation details that align with the project roadmap.

## Branch Naming
Suggested format:
- `docs/<short-topic>`
- `chore/<short-topic>`
- `feat/<short-topic>`
- `fix/<short-topic>`

## Docs-Only Checks
For docs-focused changes, run lightweight checks before opening a PR:
- YAML validity (for taxonomy files and config files)
- Markdown lint (optional but recommended)

## Pull Request Checklist
- No secrets or credentials are committed.
- No real customer audio files are committed.
- No transcripts or other PII are committed.
- Documentation links and filenames are accurate.
- Taxonomy files remain under `taxonomies/*.yml`.