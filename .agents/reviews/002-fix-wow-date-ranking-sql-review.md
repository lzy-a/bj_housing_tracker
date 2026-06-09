# Review 002: Fix WoW Date Ranking SQL

task: `.agents/tasks/002-fix-wow-date-ranking-sql.md`
report: `.agents/reports/002-fix-wow-date-ranking-sql-result.md`
reviewer: Codex
reviewed: 2026-06-09
decision: ACCEPTED

## Findings

No blocking findings.

## Accepted Behavior

- `DISTRICT_WOW_CHANGE` now ranks distinct snapshot dates with `DENSE_RANK()` before joining back to all regions.
- `RENTAL_WOW_CHANGE` uses the same date-rank pattern.
- Current MA3 windows now mean date ranks 1-3 for every region.
- Week-ago MA3 windows now mean date ranks 8-10 for every region.
- Existing output column names are preserved.

## Verification

Codex ran:

```bash
python -m py_compile analyst/sql_queries.py analyst/run_analyst.py
venv/bin/python -m py_compile analyst/sql_queries.py analyst/run_analyst.py
venv/bin/python run_analyst.py --dry-run
```

Results:

- compile checks: PASS
- dry-run: PASS after allowing localhost PostgreSQL access
- `district_wow`: 6 rows
- `rental_wow`: 6 rows

## Notes

The non-venv system Python lacks `python-dotenv`, so project verification should use `venv/bin/python` or an activated venv.
