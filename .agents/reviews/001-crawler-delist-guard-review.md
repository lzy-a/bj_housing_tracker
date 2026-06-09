# Review 001: Crawler Delist Guard

task: `.agents/tasks/001-crawler-delist-guard.md`
report: `.agents/reports/001-crawler-delist-guard-result.md`
reviewer: Codex
reviewed: 2026-06-09
decision: ACCEPTED

## Findings

No blocking findings.

## Accepted Behavior

- Sale and rent crawlers now return page outcomes instead of a plain boolean.
- Recovered click verification counts as success and does not block delisting.
- Unrecovered click verification, unknown empty pages, page exceptions, parser exceptions, and worker exceptions mark the region suspicious.
- A region is eligible for delisting only when it reaches normal pagination end and has no suspicious pages.
- The DB consumers apply delisting only for regions with an explicit successful `region_done` control message.

## Verification

Codex ran:

```bash
python -m py_compile run_crawler_playwright.py run_crawler_rent.py
```

Result: PASS.

## Residual Risk

No live Chrome/PostgreSQL crawl was run during review, so the end-to-end control-message path should still be watched on the next small single-region run.
