# Task 007 Result: Restore Pending Listings On Failed Crawl

## Status: SUBMITTED_FOR_REVIEW

## Changed Files

1. **`etl/db_manager.py`** — Added `restore_pending_properties(region)` and `restore_pending_rentals(region)` methods
2. **`run_crawler_playwright.py`** — Updated sale crawler settlement logic
3. **`run_crawler_rent.py`** — Updated rent crawler settlement logic

## Behavior Summary

### Before

When a region crawl failed or was suspicious:
- Task 001 skipped delisting (good)
- But `status = 2` rows stayed stuck as pending forever (bad)

### After

| Region outcome | `status = 2` rows become |
|---|---|
| `ok=True` (clean crawl) | `status = 0` (delisted) — same as before |
| `ok=False` (suspicious/failed) | `status = 1` (restored to active) |
| No signal received | `status = 1` (restored to active) |

### New DB Methods

```python
restore_pending_properties(region)  # UPDATE property_details SET status=1 WHERE status=2
restore_pending_rentals(region)     # UPDATE rental_details SET status=1 WHERE status=2
```

### Log Output

- `✅ 东城区: 下架标记完成` — clean region, delisted
- `↩️ 朝阳区: 恢复待确认房源 (reason=suspicious_pages)` — failed region, restored
- `↩️ 海淀区: 恢复待确认房源 (reason=no_signal)` — no signal, restored

## Commands Run

```bash
python -m py_compile run_crawler_playwright.py  # PASS
python -m py_compile run_crawler_rent.py        # PASS
python -m py_compile etl/db_manager.py          # PASS
```

## Risks

- None. Change is narrow: only affects rows still at `status = 2` in the target region.

## Questions for Codex

None.
