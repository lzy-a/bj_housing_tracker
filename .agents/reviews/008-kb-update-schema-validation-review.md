# Review 008: KB-Update Schema Validation

task: `.agents/tasks/008-kb-update-schema-validation.md`
report: `.agents/reports/008-kb-update-schema-validation-result.md`
reviewer: Codex
reviewed: 2026-06-10
decision: ACCEPTED

## Findings

No blocking findings.

## Accepted Behavior

- `kb-update` is validated before writing to long-term memory.
- Missing `district_observations` now fails validation.
- `district_observations` must include all 6 districts.
- Empty/null district observations are allowed as "no significant observation".
- Old string district observations remain backward-compatible.
- Structured observations must include non-empty `claim`, `evidence`, `source`, and valid `confidence`.
- Invalid hypothesis item types are rejected instead of silently falling through to string writes.
- Invalid `overall_favorability`, `recommended_action`, and hypothesis status values are rejected when present.
- Invalid `kb-update` output logs warnings and skips KB update while preserving the human report.

## Verification

Ran:

```bash
venv/bin/python -m py_compile analyst/run_analyst.py analyst/knowledge_base.py
venv/bin/python run_analyst.py --dry-run
```

Result: PASS.

## Residual Risk

Validation is still hand-rolled instead of using a formal JSON Schema library, by design for this task. Future tightening can require `buyer_decision_update` and stricter hypothesis fields if the weekly prompt proves stable across models.
