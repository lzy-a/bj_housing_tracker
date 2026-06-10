# Task 007: Restore Pending Listings On Failed Crawl

status: ACCEPTED
owner: Claude Code
created: 2026-06-10
priority: P1

## Goal

When a region crawl is not trusted enough to delist, restore that region's remaining `status = 2` listings back to `status = 1` instead of leaving them stuck as pending.

## Context

Current status lifecycle:

- before a region crawl, existing active listings are set from `status = 1` to `status = 2`;
- listings found during the crawl are written back as `status = 1`;
- if the region completes cleanly, remaining `status = 2` rows are marked `status = 0` as delisted;
- after Task 001, suspicious/failed regions skip delisting.

The missing piece: when delisting is skipped, remaining `status = 2` rows should not stay pending forever. Since the crawl was not trustworthy, the safest default is to restore them to `status = 1`.

## Scope

- Sale crawler status settlement in `run_crawler_playwright.py` and/or `etl/db_manager.py`.
- Rent crawler status settlement in `run_crawler_rent.py` and/or `etl/db_manager.py`.
- Add small DB helper methods if useful.

## Non-Goals

- Do not change page parsing.
- Do not change Task 001's accepted hard/soft challenge semantics.
- Do not change price/rent history behavior.
- Do not rewrite the crawler architecture.

## Acceptance Criteria

- For a region with `region_done ok=True`, remaining `status = 2` rows become `status = 0` as before.
- For a region with `region_done ok=False`, remaining `status = 2` rows are restored to `status = 1`.
- For a region with no `region_done` signal, remaining `status = 2` rows are restored to `status = 1`.
- Sale and rent crawlers use equivalent semantics.
- Logs clearly distinguish:
  - delisting applied;
  - pending restored because crawl failed/suspicious;
  - pending restored because no region signal was received.

## Suggested Approach

Add explicit restore helpers, for example:

```python
restore_pending_properties(region)
restore_pending_rentals(region)
```

Then update the final settlement loop:

```python
if outcome and outcome["ok"]:
    mark disappeared
else:
    restore pending
```

Keep this operation narrow: only rows still at `status = 2` for that region should be restored.

## Required Checks

```bash
python -m py_compile run_crawler_playwright.py run_crawler_rent.py etl/db_manager.py
```

If PostgreSQL is available, run a small targeted check or explain why it was not run.

## Required Report

Write `.agents/reports/007-restore-pending-on-failed-crawl-result.md` with:

- changed files;
- behavior summary;
- commands run;
- risks or skipped checks;
- questions, if any.
