# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Multi-agent workflow

This repository uses `.agents/` as the coordination layer between Codex and Claude Code.

Codex is the planner/reviewer and should use these files as the source of truth:

- `.agents/README.md` — collaboration protocol and task state machine
- `.agents/brief.md` — current project mission and priority
- `.agents/invariants.md` — rules that protect data correctness
- `.agents/test-protocol.md` — expected verification commands by task type
- `.agents/plan.md` — current queue
- `.agents/tasks/` — tasks for Claude Code
- `.agents/reports/` — Claude Code implementation reports
- `.agents/reviews/` — Codex review results

When asked to coordinate with Claude Code, prefer writing or updating task files
instead of relying on chat memory. Review Claude Code results by reading the task,
the result report, and the code diff, then write a review file.

## Project overview

北京二手房数据监控系统 — daily scraping of 我爱我家 (5i5j) listings for Beijing's six core districts, stored in PostgreSQL and visualized via Metabase.

## Common commands

```bash
# Start infrastructure
docker-compose up -d

# Start Chrome with remote debugging（手动）
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --remote-debugging-port=9223 \
  --user-data-dir="/tmp/chrome_9223" \
  --blink-settings=imagesEnabled=false

# Run all crawlers（自动拉起 Chrome，先二手房后租房）
source venv/bin/activate
python run_all.py                             # 全量：二手房 → 租房
python run_all.py -r 0 1 2                    # 指定区域
python run_all.py --sale-only                 # 只要二手房
python run_all.py --rent-only                 # 只要租房

# 单独跑
python run_crawler_playwright.py              # 二手房 only
python run_crawler_rent.py                    # 租房 only

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

## AI Analyst（项目分析师）

Architecture: `analyst/` module — runs after crawler, queries PostgreSQL → calls LLM API → writes Markdown reports + persistent knowledge base.

```
run_all.py（爬虫完成）
    └─→ run_analyst.py                       # CLI entry, auto daily/weekly
          ├── extractor.py + sql_queries.py   # SQL → DataFrame（所有计算在 SQL 层）
          ├── prompt_templates.py             # 日报/周报的 system+user prompt
          ├── analyst_agent.py                # Anthropic SDK 调用（MiMo 兼容端点）
          ├── report_writer.py                # 写 reports/ 下 .md 文件
          └── knowledge_base.py               # .md + frontmatter 知识库读写
```

**输出目录** `reports/`（未来可直接作为 Obsidian vault）：
```
reports/
├── 00-总览/市场状态仪表盘.md
├── 01-每日简报/2026-05-27.md
├── 02-每周深度/2026-W22.md
├── 03-区域档案/东城区.md ... 石景山区.md   ← 复利层：按日期追加观察
├── 04-假设追踪/活跃假设.md                  ← frontmatter: status/confidence
└── 05-重点关注/值得关注小区.md
```

**两种模式**：
- **日报**（每日触发）：7 条 SQL，~3500 input tokens，轻量 prompt，不更新知识库
- **周刊**（周六自动触发）：13 条 SQL（含梯队指数/板块排名/供需趋势），~5000 input tokens，附带已有知识库上下文，Codex 回复末尾输出 `kb-update` JSON 块 → programmatic 更新区域档案和假设

**复利机制**：
1. 每次周报读取 `03-区域档案/*.md` + `04-假设追踪/*.md` 作为上下文注入 prompt
2. Codex 基于新数据验证/推翻/修正之前的假设，追加新观察
3. 回复中的 `kb-update` JSON 被解析后自动写入对应 .md 文件
4. 区域档案按日期追加，Obsidian 安装后可直接 `[[双向链接]]`

**API 配置**：
- 端点：`https://token-plan-cn.xiaomimimo.com/anthropic`（MiMo token plan，兼容 Anthropic 协议）
- 模型：`mimo-v2.5-pro`，thinking/enabled, temperature=1.0
- 认证：`.env` 中的 `MIMO_KEY`
- SDK：`anthropic` Python SDK，传 `base_url` 参数指向 MiMo 端点

**SQL 查询设计原则**：
- 所有聚合/窗口函数/百分位计算在 PostgreSQL 完成，LLM 只读 markdown 表格
- 参考 Metabase 看板已验证的 SQL 模式（export/ 下有 40 张卡片的 SQL）
- 关键查询：周环比 MA3、四梯队价格指数（tiered_index，参考 cards 60-63）、板块抗跌排名、售租比

```bash
# 独立运行分析师
python run_analyst.py --mode daily      # 每日简报
python run_analyst.py --mode weekly     # 每周深度
python run_analyst.py --dry-run         # 仅提取数据，不调 API

# run_all.py 集成（默认开启）
python run_all.py                       # 爬虫 + 分析师
python run_all.py --no-analyst          # 只要爬虫
```

## Database

PostgreSQL (`house_data`) with 7 tables auto-created by `DatabaseManager._init_db()`:

| Table | Purpose |
|---|---|
| `property_details` | 二手房主表: house_id, price, area, status, community_id |
| `price_history` | 价格变动日志 — only inserted on price change |
| `district_snapshots` | 每日区域二手房大盘: listing count, avg/median/weighted unit price |
| `community_info` | 小区信息: coordinates, town_id, town_name |
| `rental_details` | 租房主表: house_id, rent_price, rent_type(整租/合租), area, community_id |
| `rent_history` | 租金变动日志 |
| `district_rent_snapshots` | 每日区域租赁大盘 |

## Configuration

All config in `.env`, loaded by `config/settings.py`. Key variables:

- `DB_HOST/PORT/NAME/USER/PASSWORD` — PostgreSQL connection
- `CHROME_DEBUG_PORT` — Chrome remote debugging port (default 9223)
- `I5I5J_PHONE/I5I5J_PASSWORD` — 5i5j login credentials

Settings are accessed as `from config.settings import DB_CONFIG, CHROME_DEBUG_PORT, SCRAPER_CONFIG, ...`.

## Metabase workflow

**Before any Metabase task**, first read the relevant memory files:
- `memory/feedback_metabase_workflow.md` — 完整工作流
- `memory/reference_dashboard_2_structure.md` — 看板快照（tabs/cards/filters/patterns）
- `memory/feedback_test_after_dashboard_change.md` — 完成后必须全参数测试

**Key rules:**
1. export/ 下的 JSON **只读不写** — 用于理解结构，不是编辑源
2. 参考已有卡片写法（SQL、template-tags、parameters），照抄再改
3. 通过 API 创建/修改卡片和看板
4. 完成后全参数测试，确认无误再交付
5. 拉取最新 JSON 到 export/，更新 memory 快照

**API docs:** `metabase-data/api.json`（本地文件）
**Metabase:** http://localhost:3000 (liuziyang101@gmail.com / mysj113598)

## District mapping

0=东城, 1=西城, 2=海淀, 3=朝阳, 4=丰台, 5=石景山

URL pattern: `https://bj.5i5j.com/ershoufang/{pinyin}/n{page_num}/`
