# 北京二手房数据监控

每日自动采集我爱我家（5i5j）北京城六区的二手房挂牌和租房数据，PostgreSQL 存储 + Metabase 可视化。

## 快速启动

```bash
# 1. 启动 Docker（PostgreSQL + Metabase）
docker-compose up -d

# 2. 一键全量爬取
source venv/bin/activate
python run_all.py                    # 全部 6 区（自动拉起 Chrome → 二手房 → 租房 → 关 Chrome）
python run_all.py -r 0 1 2           # 指定区域
python run_all.py --sale-only        # 只爬二手房
python run_all.py --rent-only        # 只爬租房

# 3. 访问 Metabase
# http://localhost:3000
```

`run_all.py` 会自动检测 Chrome CDP 是否在跑：没有就拉起（`--remote-debugging-port=9223 --blink-settings=imagesEnabled=false`），全部爬完后杀掉 Chrome 释放资源，下次启动是全新浏览器。

## 架构

```
run_all.py                              → 一键调度：ensure_chrome → 二手房 → 租房 → shutdown_chrome
run_crawler_playwright.py               → 二手房爬虫（CDP 多标签异步 + Queue → DB consumer）
run_crawler_rent.py                     → 租房爬虫（同上）
scrapers/i5i5j_scraper_playwright.py    → 二手房页面解析（BeautifulSoup）
scrapers/i5i5j_rent_scraper_playwright.py → 租房页面解析
etl/db_manager.py                       → 全部 DB 操作（建表 / 批量写入 / 快照）
config/settings.py                      → 所有配置，读取 .env
```

**数据流**：Chrome CDP → Playwright `connect_over_cdp()` → 多 tab 异步 `page.goto()` → BeautifulSoup `extract_information()` → `multiprocessing.Queue` → `global_db_consumer` 批量写入 PostgreSQL。

**Scraper 生命周期**：
1. 启动时创建临时 scraper 检查/执行登录 → `close()` 释放
2. 每个区域创建新 scraper 实例（新 Playwright 驱动 + CDP 连接）
3. 区域内每 400 页重建 scraper（主要为朝阳 ~700 页设计，防止中途卡住）
4. 全部区域跑完后 `run_all.py` 杀 Chrome 释放浏览器资源

**状态生命周期**：爬取前整区置为 status=2（待确认）→ 爬取中抓到的置为 1 → 爬取后仍为 2 的置为 0（下架）。

## 配置

`.env` 文件：

| 变量 | 说明 | 默认 |
|------|------|------|
| `DB_HOST/PORT/NAME/USER/PASSWORD` | PostgreSQL | localhost:5432/house_data |
| `CHROME_DEBUG_PORT` | Chrome 远程调试端口 | 9223 |
| `I5I5J_PHONE/PASSWORD` | 我爱我家登录凭据 | — |

`config/settings.py` 中的 `SCRAPER_CONFIG`：

| 参数 | 默认 | 说明 |
|------|------|------|
| `window_size` | 5 | 并行 tab 数 |
| `restart_interval` | 400 | 区域内每 N 页重建 scraper |
| `batch_size` | 500 | DB 批量写入条数 |
| `max_page` | 2000 | 单区域最大扫描页 |
| `delay` | 0.0 | 页间延迟（秒） |
| `timeout` | 30 | page.goto 超时（秒） |

## 区域编号

| 编号 | 区域 | 拼音 |
|------|------|------|
| 0 | 东城区 | dongchengqu |
| 1 | 西城区 | xichengqu |
| 2 | 海淀区 | haidianqu |
| 3 | 朝阳区 | chaoyangqu |
| 4 | 丰台区 | fengtaiqu |
| 5 | 石景山区 | shijingshanqu |

URL 模式：`https://bj.5i5j.com/ershoufang/{pinyin}/n{page}/`（租房为 `/zufang/`）

## 数据库

PostgreSQL `house_data`，7 张表由 `DatabaseManager._init_db()` 自动创建。

### 二手房

| 表 | 说明 |
|------|------|
| `property_details` | 房源主表，`house_id` UNIQUE，含 community_id（租售关联桥梁），status 管理上下架 |
| `price_history` | 价格变动日志，**仅价格变化时写入**（consumer 内内存比对，同价跳过） |
| `district_snapshots` | 每日区域大盘：在售数、均价、中位数价、资产平米价，`UNIQUE(record_date, region)` |

### 租房

| 表 | 说明 |
|------|------|
| `rental_details` | 租房主表，差异字段：`rent_price`（月租金）、`rent_type`（整租/合租） |
| `rent_history` | 租金变动日志，逻辑同 price_history |
| `district_rent_snapshots` | 每日区域租赁大盘：在租数、均租、中位租、单位面积租金 |

### 关联

| 表 | 说明 |
|------|------|
| `community_info` | 小区信息：经纬度、乡镇。通过 `community_id` 关联售/租房源 |

### 关键细节

- **价格比对**：consumer 启动时先从 `property_details WHERE status=1` 加载全量价格到内存，之后全部内存比对（无 DB 查询）。注意必须在 status 更新为 2 **之前**加载。
- **批量写入**：`batch_insert_property_details` 使用 `ON CONFLICT (house_id) DO UPDATE`，`district_snapshots` 使用 `ON CONFLICT (record_date, region) DO UPDATE`。
- **回报率/售租比**：已不在 DB 中预计算，改为 Metabase 卡片内 `community_id` JOIN 实时计算。

## Metabase

- 地址：http://localhost:3000
- 看板配置位于 `metabase-data/export/`（只读参考，通过 API 操作）
- 三 tab：行政区 / 板块 / 小区，支持 crossfilter 联动筛选
- 详细工作流见 `memory/feedback_metabase_workflow.md`
