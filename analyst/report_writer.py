"""
报告输出：将分析结果写入 reports/ 目录下的 Markdown 文件。
"""

import os
import logging
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
REPORTS_DIR = PROJECT_ROOT / "reports"

DIR_DAILY  = "01-每日简报"
DIR_WEEKLY = "02-每周深度"


def _ensure_dirs():
    for d in [DIR_DAILY, DIR_WEEKLY]:
        os.makedirs(REPORTS_DIR / d, exist_ok=True)


def write_daily_report(report_text: str, report_date: str = None):
    """写入每日简报。"""
    _ensure_dirs()
    if report_date is None:
        report_date = date.today().isoformat()
    path = REPORTS_DIR / DIR_DAILY / f"{report_date}.md"
    path.write_text(report_text, encoding="utf-8")
    logger.info(f"每日简报已保存: {path}")


def write_weekly_report(report_text: str, year_week: str = None):
    """写入每周深度报告。"""
    _ensure_dirs()
    if year_week is None:
        today = date.today()
        year_week = f"{today.year}-W{today.isocalendar()[1]:02d}"
    path = REPORTS_DIR / DIR_WEEKLY / f"{year_week}.md"
    path.write_text(report_text, encoding="utf-8")
    logger.info(f"周度报告已保存: {path}")
