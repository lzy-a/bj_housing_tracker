# Task 002: Fix WoW Date Ranking SQL

status: ACCEPTED
owner: Claude Code
created: 2026-06-09
priority: P1

## Goal

Fix week-over-week SQL windows so they rank distinct dates, not individual region rows.

## Context

`DISTRICT_WOW_CHANGE` and `RENTAL_WOW_CHANGE` currently compute `ROW_NUMBER() OVER (ORDER BY record_date DESC)` after joining snapshots back to all regions. Because each date has multiple region rows, `rn BETWEEN 1 AND 3` selects only a few latest region rows instead of all regions over the latest three dates.

Relevant file:

- `analyst/sql_queries.py`

## Scope

- Fix `DISTRICT_WOW_CHANGE`.
- Fix `RENTAL_WOW_CHANGE`.
- Keep output column names stable.
- Prefer clear CTEs that rank dates once, then join snapshots to date ranks.

## Non-Goals

- Do not rewrite unrelated SQL.
- Do not change prompt templates.
- Do not change snapshot table schemas.

## Acceptance Criteria

- Current window means latest 3 distinct snapshot dates for every region.
- Week-ago window means dates ranked 8 through 10 for every region.
- Query output columns remain compatible with existing prompt formatting.
- Query remains valid PostgreSQL.

## Required Checks

```bash
python -m py_compile analyst/sql_queries.py analyst/run_analyst.py
python run_analyst.py --dry-run
```

If PostgreSQL is unavailable, run compile checks and state that SQL execution was not verified.

## Required Report

Write `.agents/reports/002-fix-wow-date-ranking-sql-result.md` with:

- changed files;
- behavior summary;
- commands run;
- risks or skipped checks;
- questions, if any.
