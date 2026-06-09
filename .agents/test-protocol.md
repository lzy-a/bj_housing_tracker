# Test Protocol

Use the smallest command set that proves the task. If a command cannot be run,
state why in the result report.

## Baseline Checks

```bash
python -m py_compile run_all.py run_crawler_playwright.py run_crawler_rent.py
python -m py_compile etl/db_manager.py analyst/run_analyst.py analyst/sql_queries.py
```

## Crawler Tasks

Required checks:

```bash
python -m py_compile run_crawler_playwright.py run_crawler_rent.py scrapers/i5i5j_scraper_playwright.py scrapers/i5i5j_rent_scraper_playwright.py
```

If database and Chrome are available, prefer a tiny region run:

```bash
python run_all.py -r 5 --sale-only --no-analyst
python run_all.py -r 5 --rent-only --no-analyst
```

Report whether delisting was skipped or applied and why.

## Analyst Tasks

Required checks:

```bash
python -m py_compile analyst/run_analyst.py analyst/extractor.py analyst/sql_queries.py analyst/analyst_agent.py
python run_analyst.py --dry-run
```

If PostgreSQL is unavailable, run compile checks and describe the untested SQL path.

## Database Tasks

Required checks:

```bash
python -m py_compile etl/db_manager.py
```

If PostgreSQL is available, initialize the database and run a targeted query that verifies the new schema/view/index exists.

## Finder Tasks

Required checks:

```bash
python -m py_compile finder/app.py finder/run_finder.py finder/scorer.py finder/detail_scraper.py
```

If the web UI changed, start the local server and verify the relevant page in the browser.

## Report Requirements

Every Claude Code result report must include:

- changed files;
- behavior summary;
- commands run and exact pass/fail result;
- risks or skipped checks;
- questions for Codex, if any.
