# Task 003 Result: Make Price-Change View Explicit

## Status: SUBMITTED_FOR_REVIEW

## Changed Files

- `etl/db_manager.py` — Added `CREATE OR REPLACE VIEW v_house_price_changes` in `_init_db()`

## Behavior Summary

### What was added

Idempotent view creation in database initialization:

```sql
CREATE OR REPLACE VIEW v_house_price_changes AS
SELECT p.record_date,
       d.region,
       p.price - lag(p.price) OVER (
           PARTITION BY p.house_id
           ORDER BY p.record_date
       ) AS diff
FROM price_history p
JOIN property_details d ON p.house_id::text = d.house_id::text;
```

This view computes per-listing price changes by comparing each row to the previous one using `LAG()`. It joins `price_history` with `property_details` to get the region for each listing.

### Why

`analyst/sql_queries.py` references `v_house_price_changes` in `PRICE_ADJUSTMENTS_TODAY`, `PRICE_ADJUSTMENTS_7DAY`, and `DAILY_PULSE`. Previously this view existed in the local PostgreSQL but was not created by the codebase. A fresh database initialization would silently lose all price adjustment data.

### Verification

- `PRICE_ADJUSTMENTS_TODAY` returns 6 rows (all regions) after view creation
- Output columns: `region, price_increases, price_decreases, total_adjustments`
- View is idempotent (`CREATE OR REPLACE`) — safe to run on existing databases

## Commands Run

```bash
python -m py_compile etl/db_manager.py      # PASS
python -m py_compile analyst/sql_queries.py  # PASS
python -m py_compile analyst/extractor.py    # PASS
python -m py_compile analyst/run_analyst.py  # PASS
python run_analyst.py --dry-run             # PASS — price_adjustments returns 6 rows
```

## Risks

- None. `CREATE OR REPLACE VIEW` is safe on existing databases.
- View semantics match the task description exactly.

## Questions for Codex

None.
