# Task 004: Deduplicate Same-Day History Rows

status: ACCEPTED
owner: Claude Code
created: 2026-06-09
priority: P2

## Goal

Prevent repeated same-day crawler runs from duplicating identical price/rent history facts.

## Required Semantics

Use Option A:

- `price_history`: one row per `(house_id, record_date)`.
- `rent_history`: one row per `(house_id, record_date)`.
- If the crawler observes the same house again on the same day, update that day's row to the latest observed `price` / `rent_price`.
- Do not preserve intraday price event sequences in this task.

Reasoning: this project analyzes daily and weekly market movement. Repeated same-day crawler runs should not inflate historical change counts or create duplicate facts.

## Scope

- Add database-level uniqueness or upsert behavior so duplicate same-day facts cannot accumulate.
- Cover both sale history (`price_history`) and rent history (`rent_history`).
- Update insert/write code paths in `etl/db_manager.py` as needed.
- Preserve existing table data where practical; if a migration is needed, make it deterministic and explain it in the result report.
- Do not rewrite analyst SQL unless required by the deduplication change.

## Acceptance Criteria

- Running the crawler twice on the same day for the same unchanged listing does not create duplicate history rows.
- If the same listing is observed again later that day with a different price/rent, the same date row reflects the latest observed value.
- Existing historical analysis continues to work.
- The change is enforced at the database/write layer, not only by caller-side filtering.

## Required Checks

```bash
python -m py_compile etl/db_manager.py run_crawler_playwright.py run_crawler_rent.py
python run_analyst.py --dry-run
```

If a local PostgreSQL check is possible, also run a small idempotency check against `price_history` and `rent_history` and include the SQL/results in the report.

## Required Report

Write `.agents/reports/004-deduplicate-same-day-history-result.md` with:

- changed files;
- exact DB/write semantics;
- whether a migration/backfill was needed;
- commands run;
- risks or skipped checks.
