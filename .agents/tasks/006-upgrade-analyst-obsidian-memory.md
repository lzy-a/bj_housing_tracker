# Task 006: Upgrade Analyst Obsidian Memory Protocol

status: ACCEPTED
owner: Claude Code
created: 2026-06-09
priority: P2

## Goal

Make the analyst knowledge base more useful in Obsidian by preserving evidence, reading historical observations deeply enough, and turning hypotheses into a trackable ledger instead of an append-only text stream.

## Context

The current weekly analyst writes reports and appends `kb-update` JSON into Markdown files. This is directionally good, but the memory layer is too shallow:

- weekly prompts only inject district observation counts, not recent district observations;
- hypothesis updates are appended as new text instead of updating existing hypothesis state;
- observations do not carry evidence/source/date-window metadata;
- `(无数据)` can mean true empty data or a broken SQL/data pipeline;
- Obsidian navigation would benefit from clearer research-log and indicator-dictionary pages.

Relevant files:

- `analyst/run_analyst.py`
- `analyst/knowledge_base.py`
- `analyst/prompt_templates.py`
- `analyst/extractor.py`
- possibly `analyst/report_writer.py`

## Scope

- Improve weekly knowledge-context injection so each district includes recent observations, not just observation counts.
- Extend the `kb-update` schema in prompts to allow evidence-bearing observations, preferably objects with `claim`, `evidence`, `source`, and `confidence`.
- Update KB writing logic to render those structured observations cleanly in district and biz-circle Markdown files.
- Introduce or prepare a hypothesis ledger format where each hypothesis can have stable identity, status, confidence, and evidence log.
- Make critical data extraction errors distinguishable from legitimate empty data in analyst output or prompt context.
- Add Obsidian-friendly structure for indicator definitions and/or research logs if it can be done cleanly within scope.

## Non-Goals

- Do not rewrite the whole analyst system.
- Do not change crawler behavior.
- Do not call the LLM API as part of this task.
- Do not migrate existing reports destructively.
- Do not make broad unrelated SQL changes; use Task 002/003 for SQL correctness and missing view work.

## Acceptance Criteria

- Weekly prompt context includes recent district memory content sufficient for verification, with truncation to avoid runaway prompt size.
- New weekly `kb-update` instructions require evidence-backed observations instead of unsupported one-line claims.
- Existing string-style `kb-update` values remain backward-compatible, or the code handles both old and new shapes.
- Hypothesis updates no longer create only ambiguous append-only text; they are represented with status/confidence/evidence in a clear ledger-like format.
- If a critical query fails, the analyst path can surface that as a data issue rather than silently formatting it as `(无数据)`.
- Generated Markdown remains useful in Obsidian with links and readable headings.

## Suggested Approach

Keep the first implementation conservative:

- Add helper functions in `knowledge_base.py` for extracting recent sections from district profiles.
- Make `append_district_observation()` and `append_biz_circle_observation()` accept either a string or a dict.
- Add a new hypothesis helper that writes ledger entries with stable IDs when possible, while preserving existing `活跃假设.md`.
- Consider adding `reports/07-指标字典/` with initial placeholder pages for key metrics if this does not distract from the memory protocol.
- For data extraction status, a small wrapper object or metadata dict is fine; avoid a sweeping refactor.

## Required Checks

```bash
python -m py_compile analyst/run_analyst.py analyst/knowledge_base.py analyst/prompt_templates.py analyst/extractor.py
python run_analyst.py --dry-run
```

If PostgreSQL is unavailable, run compile checks and state that dry-run could not be fully verified.

## Required Report

Write `.agents/reports/006-upgrade-analyst-obsidian-memory-result.md` with:

- changed files;
- behavior summary;
- examples of the new Markdown/JSON shapes;
- commands run;
- risks or skipped checks;
- questions, if any.
