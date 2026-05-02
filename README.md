# 北京二手房数据监控

针对北京城六区二手房市场的长期数据监控，每日自动采集我爱我家（5i5j）房源数据，通过 PostgreSQL + Metabase 进行存储和可视化。

## 快速启动

```bash
# 1. 启动 Docker（PostgreSQL + Metabase）
docker-compose up -d

# 2. 启动 Chrome 远程调试
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
  --remote-debugging-port=9223 \
  --user-data-dir="/tmp/chrome_9223" \
  --blink-settings=imagesEnabled=false

# 3. 激活虚拟环境并运行爬虫
source venv/bin/activate
python run_crawler_playwright.py          # 全量爬取城六区
python run_crawler_playwright.py -r 1 4   # 只爬西城和丰台

# 4. 访问 Metabase 看板
# http://localhost:3000
```

## 配置

数据库连接和爬虫参数通过 `.env` 文件统一管理，`config/settings.py` 负责读取并提供默认值。

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `DB_HOST` / `DB_PORT` / `DB_NAME` | PostgreSQL 连接 | localhost / 5432 / house_data |
| `DB_USER` / `DB_PASSWORD` | 数据库凭据 | mb_admin / — |
| `CHROME_DEBUG_PORT` | Chrome 远程调试端口 | 9223 |
| `I5I5J_PHONE` / `I5I5J_PASSWORD` | 我爱我家登录凭据 | — |

## 区域编号

| 编号 | 区域 | 拼音代码 | 5i5j URL |
|------|------|----------|----------|
| 0 | 东城区 | dongchengqu | https://bj.5i5j.com/ershoufang/dongchengqu/ |
| 1 | 西城区 | xichengqu | https://bj.5i5j.com/ershoufang/xichengqu/ |
| 2 | 海淀区 | haidianqu | https://bj.5i5j.com/ershoufang/haidianqu/ |
| 3 | 朝阳区 | chaoyangqu | https://bj.5i5j.com/ershoufang/chaoyangqu/ |
| 4 | 丰台区 | fengtaiqu | https://bj.5i5j.com/ershoufang/fengtaiqu/ |
| 5 | 石景山区 | shijingshanqu | https://bj.5i5j.com/ershoufang/shijingshanqu/ |

## 数据库表设计

以下表结构由 `etl/db_manager.py` 中的 `_init_db()` 自动创建。

### 1. 房源详情主表 (`property_details`)

| 字段 | 类型 | 说明 |
|------|------|------|
| house_id | VARCHAR(20) UNIQUE | 房源唯一ID（主键） |
| title | TEXT | 房源标题 |
| region | VARCHAR(50) | 行政区 |
| biz_circle | VARCHAR(50) | 商圈 |
| community | VARCHAR(100) | 小区名 |
| layout | VARCHAR(20) | 户型（如 2室1厅） |
| area | FLOAT | 面积（㎡） |
| price | FLOAT | 总价（万） |
| unit_price | FLOAT | 单价（元/㎡） |
| orientation | VARCHAR(20) | 朝向 |
| decoration | VARCHAR(20) | 装修程度 |
| floor_info | VARCHAR(50) | 楼层信息 |
| building_type | VARCHAR(50) | 建筑类型（板楼/塔楼） |
| build_year | INTEGER | 建筑年代 |
| address_raw | TEXT | 原始地址字符串 |
| first_seen_date | DATE | 首次入库日期 |
| last_seen_date | DATE | 最后一次被抓取到的日期 |
| last_update_date | DATE | 网页显示的最后更新日期 |
| status | INTEGER | 1=在售, 0=下架, 2=待确认 |

**状态生命周期**：爬取前整区置为 2（待确认）→ 爬取中抓到的置为 1（在售）→ 爬取后仍为 2 的置为 0（下架）。

### 2. 价格历史轨迹表 (`price_history`)

仅在价格变动时插入新记录。

| 字段 | 类型 | 说明 |
|------|------|------|
| house_id | VARCHAR(20) | 房源ID |
| price | FLOAT | 变动后的总价（万） |
| unit_price | FLOAT | 变动后的单价（元/㎡） |
| record_date | DATE | 抓取到变动的日期 |

### 3. 每日区域大盘表 (`district_snapshots`)

每天爬取后计算一次，唯一键 `(record_date, region)`。

| 字段 | 类型 | 说明 |
|------|------|------|
| record_date | DATE | 爬取日期 |
| region | VARCHAR(50) | 区域 |
| total_listings | INTEGER | 在售房源总数 |
| avg_unit_price | FLOAT | 简单均价（算术平均） |
| median_unit_price | FLOAT | 中位数单价 |
| weighted_avg_price | FLOAT | 资产平米价（总价÷总面积） |

### 4. 社区信息表 (`community_info`)

| 字段 | 类型 | 说明 |
|------|------|------|
| community | VARCHAR(100) UNIQUE | 小区名 |
| region | VARCHAR(50) | 区域 |
| town_id | VARCHAR(20) | 乡镇ID |
| town_name | VARCHAR(50) | 乡镇名称 |
| longitude | FLOAT | 经度 |
| latitude | FLOAT | 纬度 |
