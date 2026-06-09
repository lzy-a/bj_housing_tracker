# Multi-Agent Plan

## Queue

| ID | Task | Status | Owner | Notes |
|---|---|---|---|---|
| 001 | Crawler delist guard | READY_FOR_CLAUDE | Claude Code | Highest priority data-safety fix |
| 002 | Fix WoW date ranking SQL | READY_FOR_CLAUDE | Claude Code | Analyst correctness |
| 003 | Make price-change view explicit | READY_FOR_CLAUDE | Claude Code | Avoid silent missing data |
| 004 | Deduplicate same-day history rows | DRAFT | Codex | Depends on desired same-day semantics |
| 005 | Normalize DB connection pool handling | DRAFT | Codex | Lower urgency unless pool errors recur |
| 006 | Upgrade analyst Obsidian memory protocol | READY_FOR_CLAUDE | Claude Code | Start after 001 is accepted |

## Review Policy

Claude Code should take the lowest-numbered `READY_FOR_CLAUDE` task, finish it,
write the result report, and stop. Codex reviews before the next task proceeds.

## Current Review Findings Source

These tasks come from Codex static review on 2026-06-09. The main risks found:

- suspicious crawl results can trigger mass false delisting;
- week-over-week SQL ranks rows instead of dates;
- `v_house_price_changes` is referenced but not created in this repo;
- history tables can duplicate same-day facts;
- some DB methods close pooled connections directly.
