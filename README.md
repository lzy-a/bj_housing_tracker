# 北京二手房数据监控

每日自动采集我爱我家（5i5j）北京城六区的二手房挂牌和租房数据，PostgreSQL 存储 + Metabase 可视化。

## 快速启动

```bash
# 1. 启动 Docker（PostgreSQL + Metabase）
docker-compose up -d

# 2. 一键全量爬取（自动拉起 Chrome，先二手房后租房）
source venv/bin/activate
python run_all.py                    # 全部 6 区
python run_all.py -r 0 1 2           # 指定区域
python run_all.py --sale-only        # 只爬二手房
python run_all.py --rent-only        # 只爬租房

# 3. 访问 Metabase
# http://localhost:3000
```

## 架构

```
run_all.py                              → 一键调度：自动检测/拉起 Chrome → 二手房 → 租房
run_crawler_playwright.py               → 二手房爬虫（CDP 多标签异步）
run_crawler_rent.py                     → 租房爬虫（同上架构）
scrapers/i5i5j_scraper_playwright.py    → 二手房页面解析
scrapers/i5i5j_rent_scraper_playwright.py → 租房页面解析
etl/db_manager.py                       → 全部 DB 操作（建表 / 批量写入 / 快照 / 社区指标）
config/settings.py                      → 配置（读取 .env）
```

**数据流**：Chrome CDP → Playwright 多 tab 并发抓取 → BeautifulSoup 解析 → Queue → DB consumer 批量写入

**状态生命周期**：爬取前整区置为 status=2（待确认）→ 爬取中抓到的置为 1 → 爬取后仍为 2 的置为 0（下架/下租）

## 配置

`.env` 文件：

| 变量 | 说明 | 默认 |
|------|------|------|
| `DB_HOST/PORT/NAME/USER/PASSWORD` | PostgreSQL | localhost:5432/house_data |
| `CHROME_DEBUG_PORT` | Chrome 远程调试端口 | 9223 |
| `I5I5J_PHONE/PASSWORD` | 我爱我家登录 | — |

## 区域编号

| 编号 | 区域 | 拼音 |
|------|------|------|
| 0 | 东城区 | dongchengqu |
| 1 | 西城区 | xichengqu |
| 2 | 海淀区 | haidianqu |
| 3 | 朝阳区 | chaoyangqu |
| 4 | 丰台区 | fengtaiqu |
| 5 | 石景山区 | shijingshanqu |

## 数据库表

以下由 `etl/db_manager.py` 的 `_init_db()` 自动创建。

### 二手房

#### property_details — 房源主表

| 字段 | 类型 | 说明 |
|------|------|------|
| house_id | VARCHAR(20) UNIQUE | 房源 ID |
| community / community_id | VARCHAR | 小区名 / 5i5j 小区物理 ID（租售关联桥梁） |
| region / biz_circle | VARCHAR(50) | 行政区 / 商圈 |
| layout / area | VARCHAR(20) / FLOAT | 户型 / 面积 |
| price / unit_price | FLOAT | 总价(万) / 单价(元/㎡) |
| orientation / decoration / floor_info | VARCHAR | 朝向 / 装修 / 楼层 |
| build_year | INTEGER | 建筑年代 |
| first_seen_date / last_seen_date | DATE | 首次入库 / 最后出现日期 |
| status | INTEGER | 1=在售 0=下架 |

#### price_history — 价格变动（仅在价格变化时写入）

### 租房

#### rental_details — 租房主表

镜像 `property_details`，差异字段：

| 字段 | 说明 |
|------|------|
| rent_price | 月租金（元） |
| rent_type | 整租 / 合租 |

#### rent_history — 租金变动

### 大盘 & 联动

#### district_snapshots / district_rent_snapshots — 每日区域均价/租金快照

#### community_metrics — 小区级租售联动（每日预计算）

| 指标 | 说明 |
|------|------|
| price_rent_ratio | 售租比 = 总价 / (月租金 × 12)，即回本年限 |
| rental_yield | 租金回报率 = 年租金 / 总价 × 100% |

通过 `community_id` 关联同一小区在售和在租房源，避免看板每次 JOIN 两张主表。

## Metabase 看板

`metabase-data/export/` 保存了完整看板配置，包含三 tab：

| Tab | 内容 |
|-----|------|
| 行政区 | 大盘指标、梯队排名、供需变化、涨跌排名（联动行政区筛选器） |
| 板块 | 商圈级分析（详情/走势/均价/户型/挂牌量，联动板块筛选器） |
| 小区 | 小区级分析（同上结构，联动小区筛选器） |

筛选器支持 crossfilter 联动点击：板块名 → 板块筛选器、小区名 → 小区筛选器、户型 → 户型筛选器。
