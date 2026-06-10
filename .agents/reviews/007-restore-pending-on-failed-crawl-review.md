# Review 007: Restore Pending Listings On Failed Crawl

task: `.agents/tasks/007-restore-pending-on-failed-crawl.md`
report: `.agents/reports/007-restore-pending-on-failed-crawl-result.md`
reviewer: Codex
reviewed: 2026-06-10
decision: ACCEPTED

## Findings

No blocking findings.

## Accepted Behavior

- Clean region outcomes (`ok=True`) still convert remaining `status = 2` rows to `status = 0`.
- Failed, suspicious, or missing region outcomes restore remaining `status = 2` rows to `status = 1`.
- Sale and rent crawlers now have equivalent settlement semantics.
- Restore operations are narrow: only rows in the target region still at `status = 2` are changed.

## Verification

Codex ran:

```bash
venv/bin/python -m py_compile run_crawler_playwright.py run_crawler_rent.py etl/db_manager.py
```

Result: PASS.

## Notes

No live crawl or mutation test was run during review. The next single-region crawler run should confirm the settlement log path in practice.
