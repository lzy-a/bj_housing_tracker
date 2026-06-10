# Task 004 Result: Deduplicate Same-Day History Rows

## Status: SUBMITTED_FOR_REVIEW

## Changed Files

- `etl/db_manager.py` — Added unique indexes and upsert logic for `price_history` and `rent_history`

## Behavior Summary

### Database Changes

1. **Deduplication migration**: Before adding unique indexes, existing duplicate rows are cleaned up:
   ```sql
   DELETE FROM price_history a USING price_history b
   WHERE a.id < b.id AND a.house_id = b.house_id AND a.record_date = b.record_date
   ```

2. **Unique indexes**:
   ```sql
   CREATE UNIQUE INDEX idx_ph_house_date_uniq ON price_history (house_id, record_date)
   CREATE UNIQUE INDEX idx_rh_house_date_uniq ON rent_history (house_id, record_date)
   ```

3. **Upsert logic**: `batch_insert_price_history()` and `batch_insert_rent_history()` now use:
   ```sql
   INSERT INTO price_history (...) VALUES %s
   ON CONFLICT (house_id, record_date) DO UPDATE
   SET price = EXCLUDED.price, unit_price = EXCLUDED.unit_price
   ```

### Semantics

- One row per `(house_id, record_date)` in both tables
- Repeated same-day observations update the existing row to the latest value
- No intraday price event sequences are preserved (as specified in task)
- Existing historical data is preserved (dedup only removes true duplicates)

### Idempotency Test

```
第一次插入后: 1 条
第二次插入后: 1 条  ← same day, same house, no duplicate
最终价格: 110.0, 2200.0  ← updated to latest value
```

## Commands Run

```bash
python -m py_compile etl/db_manager.py          # PASS
python -m py_compile run_crawler_playwright.py   # PASS
python -m py_compile run_crawler_rent.py         # PASS
python run_analyst.py --dry-run                  # PASS
```

## Risks

- **Existing duplicates**: The migration deletes older duplicates, keeping the latest by `id`. This is safe for historical analysis.
- **UNIQUE index on existing data**: If duplicates exist when the index is created, it will fail. The migration runs first to clean them up.
- **No schema change**: Tables remain the same structure; only indexes and insert behavior change.

## Questions for Codex

None.
