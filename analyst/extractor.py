"""
数据提取器：执行 SQL 查询，返回可用于 prompt 的格式化数据。
"""

import logging
from etl.db_manager import DatabaseManager
from analyst.sql_queries import (
    DISTRICT_LATEST_SNAPSHOT, DISTRICT_WOW_CHANGE,
    PRICE_ADJUSTMENTS_TODAY, PRICE_ADJUSTMENTS_7DAY,
    SUPPLY_DEMAND, RENT_YIELD_BY_DISTRICT,
    TIERED_INDEX, BIZ_CIRCLE_RESILIENCE,
    RENTAL_LATEST_SNAPSHOT, RENTAL_WOW_CHANGE,
    TODAY_PRICE_DROPS, TODAY_NEW_LISTINGS,
)



logger = logging.getLogger(__name__)


def _safe_query(db, query, fallback_name="query"):
    """执行查询，失败时返回空 DataFrame 并记录警告。"""
    try:
        return db.execute_query(query)
    except Exception as e:
        logger.warning(f"查询 {fallback_name} 失败: {e}")
        return None


class DataExtractor:
    """从 PostgreSQL 提取分析所需的结构化数据。"""

    def __init__(self, db: DatabaseManager):
        self.db = db

    # ---- 每日简报（5 条核心查询）----

    def extract_daily_brief(self) -> dict:
        return {
            "district_snapshot":      _safe_query(self.db, DISTRICT_LATEST_SNAPSHOT, "district_snapshot"),
            "district_wow":           _safe_query(self.db, DISTRICT_WOW_CHANGE, "district_wow"),
            "price_adjustments":      _safe_query(self.db, PRICE_ADJUSTMENTS_TODAY, "price_adjustments"),
            "rental_snapshot":        _safe_query(self.db, RENTAL_LATEST_SNAPSHOT, "rental_snapshot"),
            "rental_wow":             _safe_query(self.db, RENTAL_WOW_CHANGE, "rental_wow"),
            "price_drops":            _safe_query(self.db, TODAY_PRICE_DROPS, "price_drops"),
            "new_listings":           _safe_query(self.db, TODAY_NEW_LISTINGS, "new_listings"),
        }

    # ---- 每周深度（日报基础 + 深度查询）----

    def extract_weekly_deep(self) -> dict:
        data = self.extract_daily_brief()
        extra = {
            "price_adjustments_7day": _safe_query(self.db, PRICE_ADJUSTMENTS_7DAY, "price_adjustments_7day"),
            "rent_yield":             _safe_query(self.db, RENT_YIELD_BY_DISTRICT, "rent_yield"),
            "tiered_index":           _safe_query(self.db, TIERED_INDEX, "tiered_index"),
            "biz_resilience":         _safe_query(self.db, BIZ_CIRCLE_RESILIENCE, "biz_resilience"),
            "supply_demand":          _safe_query(self.db, SUPPLY_DEMAND, "supply_demand"),
        }
        data.update(extra)
        return data
