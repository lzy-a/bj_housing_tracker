# Review 003: Make Price-Change View Explicit

task: `.agents/tasks/003-price-change-view-explicit.md`
report: `.agents/reports/003-price-change-view-explicit-result.md`
reviewer: Codex
reviewed: 2026-06-10
decision: ACCEPTED

## Findings

No blocking findings.

## Accepted Behavior

- `DatabaseManager._init_db()` now creates or replaces `v_house_price_changes`.
- The view definition matches the current local PostgreSQL view recovered from `pg_get_viewdef`.
- Analyst price-adjustment queries now depend on an object that is created by repository code during database initialization.
- Existing output columns expected by `PRICE_ADJUSTMENTS_TODAY`, `PRICE_ADJUSTMENTS_7DAY`, and `DAILY_PULSE` are preserved.

## Verification

Codex ran:

```bash
venv/bin/python -m py_compile etl/db_manager.py analyst/sql_queries.py analyst/extractor.py analyst/run_analyst.py
venv/bin/python run_analyst.py --dry-run
```

Results:

- compile checks: PASS
- dry-run: PASS after allowing localhost PostgreSQL access
- `price_adjustments`: 6 rows

## Notes

This addresses the reproducibility issue for the existing view. Broader analyst error handling for distinguishing query failures from legitimate empty data remains a separate concern.
