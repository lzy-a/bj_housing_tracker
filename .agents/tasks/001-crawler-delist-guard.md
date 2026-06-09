# Task 001: Crawler Delist Guard

status: ACCEPTED
owner: Claude Code
created: 2026-06-09
priority: P1

## Goal

Prevent crawler failures, anti-crawl pages, login prompts, and unknown empty pages from marking active listings as delisted.

## Context

The current consumers set all selected-region listings to `status = 2` before crawling. At shutdown they mark remaining `status = 2` rows as `status = 0`.
This is only safe when the region crawl completed successfully. Today several suspicious page outcomes are treated as `success=True, count=0`, so a bad crawl can create false delisting.

Relevant locations:

- `run_crawler_playwright.py`
- `run_crawler_rent.py`

## Scope

- Add an explicit region crawl outcome for sale and rent crawlers.
- Make delisting conditional on a successful region crawl.
- Log skipped delisting clearly when a region is suspicious or failed.
- Keep the change local to crawler orchestration/consumer coordination unless a tiny helper is clearly needed.

## Non-Goals

- Do not rewrite the scraper parser.
- Do not change database schema.
- Do not change pricing/rent history semantics.
- Do not add a new framework or persistence layer for crawl runs.

## Acceptance Criteria

- Anti-crawl, login prompt, page navigation failure, parser failure, and unknown empty page outcomes cannot cause `mark_disappeared_properties` or the rent equivalent for that region.
- Known no-data pagination still stops a region normally and allows delisting for that region.
- Sale and rent crawlers use the same high-level outcome semantics.
- Logs distinguish at least: region completed, region failed/suspicious, delisting applied, delisting skipped.
- If one region fails, other completed regions may still settle normally.

## Suggested Approach

Consider passing region completion state from the async crawler to the DB consumer through a small control message on the multiprocessing queue, for example:

```python
{"__control__": "region_done", "region": "西城区", "ok": True}
{"__control__": "region_done", "region": "西城区", "ok": False, "reason": "anti_crawl"}
```

The consumer can maintain `region_outcomes` and only mark disappeared listings for regions with `ok=True`.

Be careful not to count control messages as listings.

## Required Checks

```bash
python -m py_compile run_crawler_playwright.py run_crawler_rent.py
```

If Chrome/PostgreSQL are available, run a tiny single-region crawl. If not, say so in the report.

## Required Report

Write `.agents/reports/001-crawler-delist-guard-result.md` with:

- changed files;
- behavior summary;
- commands run;
- risks or skipped checks;
- questions, if any.
