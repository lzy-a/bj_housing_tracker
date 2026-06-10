# Review 004: Deduplicate Same-Day History Rows

task: `.agents/tasks/004-deduplicate-same-day-history.md`
report: `.agents/reports/004-deduplicate-same-day-history-result.md`
reviewer: Codex
reviewed: 2026-06-10
decision: ACCEPTED

## Findings

No blocking findings.

## Accepted Behavior

- `price_history` is constrained to one row per `(house_id, record_date)`.
- `rent_history` is constrained to one row per `(house_id, record_date)`.
- Existing duplicate same-day rows are deterministically collapsed before creating the unique indexes, keeping the latest row by `id`.
- Batch history writes use `ON CONFLICT (house_id, record_date) DO UPDATE`, so repeated same-day observations update the day's row instead of inserting duplicates.
- The implementation covers both sale and rent history tables.

## Verification

Ran:

```bash
venv/bin/python -m py_compile etl/db_manager.py run_crawler_playwright.py run_crawler_rent.py
venv/bin/python run_analyst.py --dry-run
```

Result: PASS.

Also ran a read-only PostgreSQL check:

```text
indexes ['idx_ph_house_date_uniq', 'idx_rh_house_date_uniq']
dupes [('rent_history', 0), ('price_history', 0)]
```

## Residual Risk

`insert_price_history()` still uses plain `INSERT`, but it has no current callers in the repository. If that legacy helper is reused later, it should be changed to the same upsert semantics.

The startup migration is idempotent, but it still executes duplicate-cleanup queries during `DatabaseManager._init_db()`. If history tables become very large, moving this to an explicit migration step would be cleaner.
