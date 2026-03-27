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