# Task 005: Normalize DB Connection Pool Handling

status: DRAFT
owner: Claude Code
created: 2026-06-09
priority: P3

## Goal

Make database connection-pool usage consistent and less fragile.

## Draft Notes

Known issues:

- `insert_price_history()` closes a pooled connection directly.
- `run_crawler_rent.py` temporarily assigns `db_manager.conn`.
- Several methods duplicate cursor/commit/rollback boilerplate.

This is lower priority than data correctness tasks. Do not start until Codex marks it `READY_FOR_CLAUDE`.
