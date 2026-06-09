# Task 004: Deduplicate Same-Day History Rows

status: DRAFT
owner: Claude Code
created: 2026-06-09
priority: P2

## Goal

Prevent repeated same-day crawler runs from duplicating identical price/rent history facts.

## Draft Notes

Codex needs to decide the desired same-day semantics before this task becomes READY:

- Option A: unique `(house_id, record_date)` and update to latest observed price/rent for that day.
- Option B: unique `(house_id, record_date, price)` / `(house_id, record_date, rent_price)` and ignore exact duplicates.
- Option C: keep all intraday events but add timestamp-aware analysis SQL.

Current likely preference: Option A for simple daily research reports.

Do not start this task until Codex marks it `READY_FOR_CLAUDE`.
