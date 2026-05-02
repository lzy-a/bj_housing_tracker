# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

北京二手房数据监控系统 — daily scraping of 我爱我家 (5i5j) listings for Beijing's six core districts, stored in PostgreSQL and visualized via Metabase.

## Common commands

```bash
# Start infrastructure
docker-compose up -d

# Start Chrome with remote debugging
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --remote-debugging-port=9223 \
  --user-data-dir="/tmp/chrome_9223" \
  --blink-settings=imagesEnabled=false

# Run crawler
source venv/bin/activate
python run_crawler_playwright.py              # all 6 districts
python run_crawler_playwright.py -r 0 1 2     # specific districts

# Install deps
pip install -r requirements.txt
playwright install chromium
```

Metabase is available at http://localhost:3000 after Docker starts.

## Architecture

```
run_crawler_playwright.py              → Orchestration: CLI parsing, async multi-tab crawling, queue-based DB writing
scrapers/i5i5j_scraper_playwright.py   → 5i5j scraper: CDP connection, login, HTML parsing
etl/db_manager.py                      → DatabaseManager: schema init, CRUD, batch insert, connection pooling
config/settings.py                     → All configuration, loaded from .env with defaults
```

**Data flow**: Chrome CDP → Playwright connects → multi-tab async page fetch → BeautifulSoup parsing via `I5I5JScraperPlaywright.extract_information()` → `multiprocessing.Queue` → `global_db_consumer` writes to PostgreSQL in batches.

**Status lifecycle**: Before crawling a region, all properties → status 2 (pending). During crawl, found properties → 1 (active). After crawl, remaining status 2 → 0 (delisted).

## Database

PostgreSQL (`house_data`) with 4 tables auto-created by `DatabaseManager._init_db()`:

| Table | Purpose |
|---|---|
| `property_details` | Master table: house_id, price, area, status, time tracking |
| `price_history` | Price change log — only inserted on price change |
| `district_snapshots` | Daily per-district aggregates: listing count, avg/median/weighted unit price |
| `community_info` | Community geo-info: coordinates, town_id, town_name |

## Configuration

All config in `.env`, loaded by `config/settings.py`. Key variables:

- `DB_HOST/PORT/NAME/USER/PASSWORD` — PostgreSQL connection
- `CHROME_DEBUG_PORT` — Chrome remote debugging port (default 9223)
- `I5I5J_PHONE/I5I5J_PASSWORD` — 5i5j login credentials

Settings are accessed as `from config.settings import DB_CONFIG, CHROME_DEBUG_PORT, SCRAPER_CONFIG, ...`.

## District mapping

0=东城, 1=西城, 2=海淀, 3=朝阳, 4=丰台, 5=石景山

URL pattern: `https://bj.5i5j.com/ershoufang/{pinyin}/n{page_num}/`
