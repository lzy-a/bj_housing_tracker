# 🏠 Beijing Housing Tracker (2026-2028)

这是一个针对北京二手房市场的长期数据监控工具，旨在通过自动化数据采集与分析，为 2028 年的购房决策提供量化支持。

## 🛠 技术栈
- **Scraper:** Playwright (模拟浏览器抓取住建委动态)
- **Database:** SQLite (轻量级本地存储)
- **Frontend:** Streamlit (实时交互式看板)
- **Language:** Python 3.9+

## 📅 开发计划
- [x] 核心框架搭建 (Config/ETL/Scrapers)
- [x] 住建委官方网签数据抓取
- [ ] 链家/我爱我家挂牌量趋势分析
- [ ] 2028 房价预测模型 (基于宏观利率与库存比)

## 🚀 快速启动
1. `source venv/bin/activate`
2. `pip install -r requirements.txt`
3. `python run_crawler.py` (采集数据)
4. `python run_dashboard.py` (启动看板)



## 数据表

### SQL 建表语句

```sql
-- 1. 宏观水位表：记录每月楼市的“心跳”
CREATE TABLE market_snapshots (
    date TEXT PRIMARY KEY,          -- 改为月份 (如: '2026-03-01')
    bj_total INTEGER,                -- 全北京总量
    xc_total INTEGER,                -- 西城总量
    ft_total INTEGER,                -- 丰台总量
    monthly_inflow INTEGER,          -- 本月新进入系统的 ID 数量
    monthly_outflow INTEGER          -- 本月消失的 ID 数量（估算成交/下架）
);

-- 2. 房源详情表：房源的静态基因 + 生命周期
CREATE TABLE property_details (
    house_id TEXT PRIMARY KEY,
    region TEXT,                     -- 西城/丰台
    biz_circle TEXT,                 -- 车公庄/草桥等
    community TEXT,
    layout TEXT,
    area REAL,
    build_year INTEGER,
    floor_total INTEGER,
    building_type TEXT,
    first_seen_date TEXT,            -- 首次发现日期 (YYYY-MM-DD)
    last_seen_date TEXT,             -- 最后存活日期 (用于判断流动性)
    status INTEGER DEFAULT 1         -- 1:在售, 0:下架
);

-- 3. 价格变动表：不变
CREATE TABLE price_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    house_id TEXT,
    price REAL,                      -- 总价 (万)
    unit_price REAL,                 -- 单价
    record_date TEXT,                -- 记录日期
    FOREIGN KEY (house_id) REFERENCES property_details(house_id)
);
```

------

**如何判断“消失”的房源？**

- **问题：** 链家有 12 万套房，你这个月只爬了西城和丰台的 2.2 万套。
- **补丁：** 每次爬取前，先把数据库里该区域的房源 `status` 设为一个中间态（比如 `2: 待确认`）。爬完之后，被抓到的房源更新为 `1: 在售`，那些依然是 `2` 的房源，就意味着它们消失了，设为 `0: 下架`。