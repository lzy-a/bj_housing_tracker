# Task 003: Make Price-Change View Explicit

status: READY_FOR_CLAUDE
owner: Claude Code
created: 2026-06-09
priority: P2

## Goal

Ensure `v_house_price_changes` exists or replace its usage so analyst queries do not silently lose adjustment data.

## Context

`analyst/sql_queries.py` references `v_house_price_changes`, but this repository does not create that view. `DatabaseManager.execute_query()` returns an empty DataFrame on SQL failure, which can make missing infrastructure look like real empty market data.

Relevant files:

- `analyst/sql_queries.py`
- `etl/db_manager.py`
- possibly `analyst/extractor.py`

## Scope

- Add idempotent creation of `v_house_price_changes` in database initialization, or rewrite analyst SQL to compute the view inline.
- Make missing critical analyst dependencies visible enough that reports are not misleading.
- Keep compatibility with existing SQL column expectations: `record_date`, `region`, `diff`, and any fields currently queried.

## Non-Goals

- Do not overhaul all analyst error handling unless necessary for this task.
- Do not change report format unless required to surface critical failures.
- Do not drop or rebuild existing history tables.

## Acceptance Criteria

- Fresh database initialization provides the data source needed by price adjustment queries.
- `PRICE_ADJUSTMENTS_TODAY`, `PRICE_ADJUSTMENTS_7DAY`, and `DAILY_PULSE` no longer depend on an undeclared object.
- A missing critical object should not be indistinguishable from a legitimate empty result.
- Schema/view creation is repeatable.

## Required Checks

```bash
python -m py_compile etl/db_manager.py analyst/sql_queries.py analyst/extractor.py analyst/run_analyst.py
python run_analyst.py --dry-run
```

If PostgreSQL is unavailable, run compile checks and describe the unverified DB path.

## Required Report

Write `.agents/reports/003-price-change-view-explicit-result.md` with:

- changed files;
- behavior summary;
- commands run;
- risks or skipped checks;
- questions, if any.
