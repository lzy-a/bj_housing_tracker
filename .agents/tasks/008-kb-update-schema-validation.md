# Task 008: KB-Update Schema Validation

status: ACCEPTED
owner: Claude Code
created: 2026-06-10
priority: P2

## Goal

Add JSON schema validation for `kb-update` blocks so invalid LLM output does not corrupt the knowledge base.

## Context

The weekly analyst extracts a `kb-update` JSON block from the LLM response and writes it to Obsidian memory files. Currently there is no validation — malformed JSON, missing fields, or wrong types can silently produce bad memory entries.

The design doc (`.agents/design/analyst-memory-system.md`) lists this as Phase 2 work.

## Scope

- Add a `validate_kb_update(data: dict) -> tuple[bool, list[str]]` function that checks:
  - Required top-level keys exist (`district_observations`, etc.)
  - `district_observations` has all 6 districts
  - Each observation is either a string or a dict with required fields (`claim`, `evidence`, `source`, `confidence`)
  - `confidence` values are `low / medium / high`
  - `overall_favorability` values are `buyers_market / neutral / sellers_market`
  - `recommended_action` values are `观望 / 开始看 / 可以出手`
  - `status` values in hypothesis_updates are valid
- Integrate validation into `_update_kb_from_response()` in `run_analyst.py`
- If validation fails, log warnings but still write the report (skip KB update)
- Keep backward compatibility with old string-format observations

## Non-Goals

- Do not add a full JSON Schema library dependency
- Do not change the prompt templates
- Do not change the analyst output format

## Acceptance Criteria

- Valid `kb-update` passes validation and updates KB as before
- Invalid `kb-update` (missing districts, wrong confidence values, etc.) logs specific warnings and skips KB update
- Old string-format observations in `district_observations` still pass validation
- Validation function is testable in isolation

## Required Checks

```bash
python -m py_compile analyst/run_analyst.py analyst/knowledge_base.py
python run_analyst.py --dry-run
```

## Required Report

Write `.agents/reports/008-kb-update-schema-validation-result.md` with:
- changed files;
- behavior summary;
- validation rules;
- commands run;
- risks or skipped checks.
