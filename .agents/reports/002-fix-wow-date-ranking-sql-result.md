# Task 002 Result: Fix WoW Date Ranking SQL

## Status: SUBMITTED_FOR_REVIEW

## Changed Files

- `analyst/sql_queries.py` — Fixed `DISTRICT_WOW_CHANGE` and `RENTAL_WOW_CHANGE` queries

## Behavior Summary

### Bug

Both WoW queries used `ROW_NUMBER() OVER (ORDER BY record_date DESC)` on individual rows (region × date). With 6 regions and 12 dates, `rn BETWEEN 1 AND 3` selected only the 3 latest rows (e.g., 3 regions on 1 day), not all regions over the latest 3 dates.

### Fix

Replaced `ROW_NUMBER()` with `DENSE_RANK()` on distinct dates:

```sql
-- Before (broken)
ranked AS (
    SELECT ..., ROW_NUMBER() OVER (ORDER BY ds.record_date DESC) AS rn
    FROM district_snapshots ds JOIN all_dates ad ...
)
-- rn=1,2,3 → 3 rows (could be 1 day × 3 regions)

-- After (correct)
date_rank AS (
    SELECT DISTINCT record_date,
           DENSE_RANK() OVER (ORDER BY record_date DESC) AS dr
    FROM district_snapshots ...
)
-- dr=1 → all regions on latest date
-- dr=2 → all regions on 2nd latest date
-- dr=3 → all regions on 3rd latest date
```

### Query Structure (both queries)

1. `date_rank` CTE: rank distinct dates using `DENSE_RANK()`
2. `current_window`: join snapshots where `dr BETWEEN 1 AND 3`, group by region
3. `week_ago_window`: join snapshots where `dr BETWEEN 8 AND 10`, group by region
4. Final SELECT: join both windows, compute WoW percentage change

### Output Columns (unchanged)

**DISTRICT_WOW_CHANGE**: `region, current_price_ma3, current_listings_ma3, week_ago_price_ma3, week_ago_listings_ma3, price_wow_pct, listings_wow_pct`

**RENTAL_WOW_CHANGE**: `region, current_rent_ma3, current_rentals_ma3, week_ago_rent_ma3, week_ago_rentals_ma3, rent_wow_pct, rental_listings_wow_pct`

## Commands Run

```bash
python -m py_compile analyst/sql_queries.py    # PASS
python -m py_compile analyst/run_analyst.py    # PASS
python run_analyst.py --dry-run               # PASS — 6 regions returned for both WoW queries
```

Dry-run output confirmed:
- `district_wow`: 6 rows, all regions present
- `rental_wow`: 6 rows, all regions present
- Percentage changes look reasonable (e.g., 东城区 price_wow_pct = -0.3%)

## Risks

- None. Output columns unchanged, query structure is standard PostgreSQL.

## Questions for Codex

None.
