"""Validate repository contracts without requiring a Databricks workspace."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_TASKS = [
    *(f"foundation_{index:02d}_{name}" for index, name in enumerate(
        [
            "ingest_audio",
            "preprocess_audio",
            "diarize_audio",
            "transcribe_audio",
            "align_turns",
            "redact_pii",
            "translate_turns",
            "publish_and_finalize",
        ],
        start=1,
    )),
    *(f"insights_{index:02d}_{name}" for index, name in enumerate(
        [
            "compute_conversation_metrics",
            "load_taxonomies_to_dim_tables",
            "build_text_chunks",
            "embed_chunks",
            "build_vector_search_rag_index",
            "llm_extract_chunk_insights",
            "llm_consolidate_call_insights",
            "quality_gates_and_finalize",
        ],
        start=1,
    )),
]
WORKFLOW_FILES = [ROOT / "workflows/full_job.json", ROOT / "workflows/smoke_test_job.json"]
TAXONOMY_FIELDS = {
    "contact_drivers.yml": ("label", "active", "definition"),
    "issues.yml": ("label", "active", "definition"),
    "intents.yml": ("label", "active", "definition"),
    "emotions.yml": ("emotion", "active", "sentiment_group", "polarity_score", "definition"),
}
FORBIDDEN_GOLD_COLUMNS = {
    "chunk_text",
    "chunk_text_redacted",
    "chunk_text_translated",
    "turn_text",
    "turn_text_redacted",
    "turn_text_translated",
    "transcript_text",
    "transcript",
    "raw_transcript",
    "text_redacted",
    "text_translated",
    "text_raw",
}


def _job_parameters(job: dict[str, Any]) -> dict[str, str]:
    return {entry["name"]: entry["default"] for entry in job.get("parameters", [])}


def validate_workflows() -> list[str]:
    errors: list[str] = []
    for workflow_path in WORKFLOW_FILES:
        label = workflow_path.relative_to(ROOT).as_posix()
        try:
            job = json.loads(workflow_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(f"{label}: invalid JSON: {exc}")
            continue

        git_source = job.get("git_source", {})
        if git_source != {
            "git_url": "https://github.com/soulipaco/speechanalytics-databricks-pipeline.git",
            "git_provider": "gitHub",
            "git_branch": "main",
        }:
            errors.append(f"{label}: git_source must pin the public GitHub main branch")

        parameters = _job_parameters(job)
        if set(parameters) != {"catalog", "schema", "volume_root"}:
            errors.append(f"{label}: expected catalog/schema/volume_root job parameters")
        volume_root = parameters.get("volume_root", "")
        volume_parts = [part for part in volume_root.split("/") if part]
        if len(volume_parts) != 4 or volume_parts[0] != "Volumes":
            errors.append(f"{label}: volume_root must be /Volumes/<catalog>/<schema>/<volume>")
        if any(marker in json.dumps(job) for marker in ("<user>", "<volume_root>", "/Repos/")):
            errors.append(f"{label}: unresolved personal or placeholder path")

        tasks = job.get("tasks", [])
        task_keys = [task.get("task_key") for task in tasks]
        if task_keys != EXPECTED_TASKS:
            errors.append(f"{label}: task order/names do not match the 16-stage contract")
        for index, task in enumerate(tasks):
            task_key = task.get("task_key", "<missing>")
            notebook_task = task.get("notebook_task", {})
            expected_group = "foundation" if task_key.startswith("foundation_") else "insights"
            expected_path = f"notebooks/{expected_group}/{task_key}.py"
            actual_path = notebook_task.get("notebook_path")
            if notebook_task.get("source") != "GIT" or actual_path != expected_path:
                errors.append(f"{label}:{task_key}: expected relative GIT path {expected_path}")
            elif not (ROOT / actual_path).is_file():
                errors.append(f"{label}:{task_key}: notebook source is missing")

            base = notebook_task.get("base_parameters", {})
            if base.get("catalog") != "{{job.parameters.catalog}}":
                errors.append(f"{label}:{task_key}: catalog must reference the job parameter")
            if base.get("schema") != "{{job.parameters.schema}}":
                errors.append(f"{label}:{task_key}: schema must reference the job parameter")
            if base.get("run_id") != "{{job.run_id}}":
                errors.append(f"{label}:{task_key}: run_id must be traceable to the job run")
            if index < 3 and base.get("volume_root") != "{{job.parameters.volume_root}}":
                errors.append(f"{label}:{task_key}: required volume_root contract is missing")

            expected_dependencies = [] if index == 0 else [{"task_key": EXPECTED_TASKS[index - 1]}]
            if task.get("depends_on", []) != expected_dependencies:
                errors.append(f"{label}:{task_key}: dependency chain is not linear/canonical")
    return errors


def validate_taxonomies() -> list[str]:
    errors: list[str] = []
    for filename, required_fields in TAXONOMY_FIELDS.items():
        path = ROOT / "taxonomies" / filename
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list) or not items:
            errors.append(f"taxonomies/{filename}: items must be a non-empty list")
            continue
        key_field = required_fields[0]
        labels: list[str] = []
        for index, item in enumerate(items):
            missing = [field for field in required_fields if field not in item]
            if missing:
                errors.append(f"taxonomies/{filename}: item {index} missing {missing}")
            label = str(item.get(key_field, "")).strip()
            if not label:
                errors.append(f"taxonomies/{filename}: item {index} has an empty {key_field}")
            labels.append(label.casefold())
            if filename == "emotions.yml":
                score = item.get("polarity_score")
                if not isinstance(score, (int, float)) or not -1 <= score <= 1:
                    errors.append(f"taxonomies/{filename}: item {index} polarity_score outside [-1, 1]")
        if len(labels) != len(set(labels)):
            errors.append(f"taxonomies/{filename}: labels must be case-insensitively unique")
    return errors


def validate_schema_privacy() -> list[str]:
    errors: list[str] = []
    consolidation = (ROOT / "notebooks/insights/insights_07_llm_consolidate_call_insights.py").read_text(
        encoding="utf-8"
    )
    match = re.search(
        r"CREATE TABLE IF NOT EXISTS \{gold_table\} \((.*?)\) USING DELTA",
        consolidation,
        re.DOTALL,
    )
    if not match:
        errors.append("gold_speech_insights DDL contract was not found")
    else:
        ddl_columns = {
            line.strip().split()[0].strip("`,").casefold()
            for line in match.group(1).splitlines()
            if line.strip()
        }
        leaked = sorted(ddl_columns & FORBIDDEN_GOLD_COLUMNS)
        if leaked:
            errors.append(f"gold_speech_insights contains forbidden raw-text columns: {leaked}")

    quality_gate = (ROOT / "notebooks/insights/insights_08_quality_gates_and_finalize.py").read_text(
        encoding="utf-8"
    )
    missing_guards = sorted(name for name in FORBIDDEN_GOLD_COLUMNS if f'"{name}"' not in quality_gate)
    if missing_guards:
        errors.append(f"quality gate is missing raw-text guards: {missing_guards}")
    if "Raw transcript-like columns detected in gold_speech_insights" not in quality_gate:
        errors.append("quality gate no longer fails on raw transcript-like columns")
    return errors


def validate_sample() -> list[str]:
    errors: list[str] = []
    generator = ROOT / "samples/generate_synthetic_wav.py"
    readme = ROOT / "samples/README.md"
    if not generator.is_file() or not readme.is_file():
        errors.append("deterministic synthetic sample path is incomplete")
    elif "intentionally not claimed as speech" not in readme.read_text(encoding="utf-8"):
        errors.append("synthetic fixture limitations must remain explicit")
    return errors


def validate_all() -> list[str]:
    return [
        *validate_workflows(),
        *validate_taxonomies(),
        *validate_schema_privacy(),
        *validate_sample(),
    ]


def main() -> int:
    errors = validate_all()
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("Repository contracts: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
