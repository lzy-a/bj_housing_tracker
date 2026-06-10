#!/usr/bin/env python3
"""
项目分析引擎 — AI 驱动的北京房产市场分析系统。

Usage:
    python run_analyst.py                        # auto-detect mode
    python run_analyst.py --mode daily            # 每日简报
    python run_analyst.py --mode weekly           # 每周深度
    python run_analyst.py --dry-run               # 仅提取数据，不调 API
"""

import argparse
import logging
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import DB_CONFIG, CLAUDE_API_KEY, CLAUDE_BASE_URL, CLAUDE_MODEL, ANALYST_CONFIG
from etl.db_manager import DatabaseManager
from analyst.extractor import DataExtractor, QueryResult
from analyst.analyst_agent import AnalystAgent
from analyst.report_writer import write_daily_report, write_weekly_report
from analyst.knowledge_base import (
    read_all_district_profiles, read_hypotheses,
    append_district_observation, update_district_stats,
    append_hypothesis, append_new_hypothesis, append_hypothesis_update,
    write_watchlist, write_dashboard, read_recent_district_observations,
    read_last_weekly_report, read_buying_assessment,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _is_weekly_day() -> bool:
    """周六触发周度分析。"""
    return date.today().weekday() == 5


def _format_df(df, max_rows: int = 200) -> str:
    """DataFrame → compact markdown table。"""
    if isinstance(df, QueryResult):
        if df.status == "error":
            return f"(数据提取失败: {df.error})"
        df = df.data
    if df is None or df.empty:
        return "(无数据)"
    if len(df) > max_rows:
        df = df.head(max_rows)
    return df.to_markdown(index=False, tablefmt="pipe", floatfmt=".1f")


def _build_knowledge_context() -> str:
    """构建知识库上下文文本，注入每周深度的 user prompt。"""
    profiles = read_all_district_profiles()
    hyps = read_hypotheses()

    parts = ["## 已有知识库\n"]

    parts.append("### 区域档案\n")
    for region in ["东城区", "西城区", "海淀区", "朝阳区", "丰台区", "石景山区"]:
        p = profiles.get(region, {})
        fm = p.get("frontmatter", {})
        recent = read_recent_district_observations(region, limit=3)
        if fm or recent:
            parts.append(f"#### {region}")
            parts.append(f"观察数: {fm.get('observation_count', 0)}")
            if recent:
                parts.append(recent)
            else:
                parts.append("暂无近期观察。")

    parts.append(f"\n### 活跃假设\n")
    if hyps.get("body"):
        parts.append(hyps["body"][:3000])
    else:
        parts.append("暂无历史假设。")

    parts.append(f"\n### 假设统计\n"
                 f"总计 {hyps.get('frontmatter', {}).get('total_hypotheses', 0)} 条假设")

    # 上周报告摘要
    last_report = read_last_weekly_report()
    if last_report:
        parts.append(f"\n### 上周报告摘要\n")
        parts.append(last_report[:2000])

    buying = read_buying_assessment()
    if buying:
        parts.append("\n### 当前买房行动建议\n")
        parts.append(buying)

    return "\n".join(parts)


def _update_kb_from_response(kb_update: dict):
    """将 Claude 回复中的 kb-update JSON 写入知识库文件。"""
    if not kb_update:
        logger.info("kb-update 为空，跳过知识库更新")
        return

    # 区域观察
    obs = kb_update.get("district_observations", {})
    for region, observation in obs.items():
        if observation:
            append_district_observation(region, observation)
            logger.info(f"已更新区域档案: {region}")

    # 板块观察
    biz_obs = kb_update.get("biz_circle_observations", {})
    for biz_circle, observation in biz_obs.items():
        if observation:
            from analyst.knowledge_base import append_biz_circle_observation
            append_biz_circle_observation(biz_circle, observation)
            logger.info(f"已更新板块档案: {biz_circle}")

    # 假设更新
    for h in kb_update.get("hypothesis_updates", []):
        if isinstance(h, dict):
            append_hypothesis_update(h)
        else:
            append_hypothesis(str(h))

    # 新假设
    for h in kb_update.get("new_hypotheses", []):
        if isinstance(h, dict):
            append_new_hypothesis(h)
        else:
            append_hypothesis(str(h))

    # 重点关注
    wl = kb_update.get("watchlist_update", "")
    if wl:
        write_watchlist(wl)

    # 仪表盘
    signals = kb_update.get("market_signals", [])
    if signals:
        sig_text = "\n".join(f"- {s}" for s in signals)
        write_dashboard(f"## 最新市场信号\n{sig_text}")

    # 购买力评估
    ba = kb_update.get("buyer_decision_update") or kb_update.get("buying_assessment", {})
    if ba:
        from analyst.knowledge_base import write_buying_assessment
        write_buying_assessment(ba)
        logger.info("已更新购买力评估")

    logger.info("知识库更新完成")


def main():
    parser = argparse.ArgumentParser(description="北京房产市场 AI 分析师")
    parser.add_argument("--mode", choices=["daily", "weekly", "auto"], default="auto",
                        help="分析模式 (default: auto)")
    parser.add_argument("--dry-run", action="store_true",
                        help="仅提取数据，不调用 API")
    args = parser.parse_args()

    mode = args.mode
    if mode == "auto":
        mode = "weekly" if _is_weekly_day() else "daily"

    today_str = date.today().isoformat()
    week_num = date.today().isocalendar()[1]

    print(f"\n{'=' * 60}")
    print(f"📊 项目分析引擎 — {'周度深度分析' if mode == 'weekly' else '每日简报'}")
    print(f"📅 {today_str}" + (f" (第{week_num}周)" if mode == 'weekly' else ''))
    print(f"{'=' * 60}")

    # 初始化
    db = DatabaseManager(DB_CONFIG)
    extractor = DataExtractor(db)

    # 提取数据
    if mode == "weekly":
        data = extractor.extract_weekly_deep()
    else:
        data = extractor.extract_daily_brief()

    if args.dry_run:
        print("🟡 Dry-run 模式：数据已提取，跳过 API 调用\n")
        for name, result in data.items():
            df = result.data if hasattr(result, "data") else result
            status = getattr(result, "status", "ok")
            if status == "error":
                print(f"--- {name} (ERROR) ---")
                print(_format_df(result, max_rows=5))
                print()
            elif df is not None and not df.empty:
                print(f"--- {name} ({len(df)} rows) ---")
                print(_format_df(result, max_rows=5))
                print()
        print("✅ Dry-run 完成")
        return

    # 构建 prompt
    if mode == "daily":
        from analyst.prompt_templates import DAILY_BRIEF_SYSTEM, DAILY_BRIEF_USER
        system_prompt = DAILY_BRIEF_SYSTEM
        user_prompt = DAILY_BRIEF_USER.format(
            date=today_str,
            price_drops=_format_df(data.get("price_drops"), max_rows=15),
            new_listings=_format_df(data.get("new_listings"), max_rows=20),
            price_adjustments=_format_df(data.get("price_adjustments")),
            district_wow=_format_df(data.get("district_wow")),
        )
        max_tokens = ANALYST_CONFIG.get('daily_max_tokens', 2048)
    else:
        from analyst.prompt_templates import WEEKLY_DEEP_SYSTEM, WEEKLY_DEEP_USER
        knowledge_context = _build_knowledge_context()
        system_prompt = WEEKLY_DEEP_SYSTEM
        user_prompt = WEEKLY_DEEP_USER.format(
            week_number=week_num,
            date=today_str,
            knowledge_context=knowledge_context,
            district_snapshot=_format_df(data.get("district_snapshot")),
            district_wow=_format_df(data.get("district_wow")),
            price_adjustments_7day=_format_df(data.get("price_adjustments_7day")),
            tiered_index=_format_df(data.get("tiered_index"), max_rows=300),
            biz_resilience=_format_df(data.get("biz_resilience")),
            rent_yield=_format_df(data.get("rent_yield")),
            rental_snapshot=_format_df(data.get("rental_snapshot")),
            rental_wow=_format_df(data.get("rental_wow")),
            supply_demand=_format_df(data.get("supply_demand"), max_rows=60),
        )
        max_tokens = ANALYST_CONFIG.get('weekly_max_tokens', 8192)

    # 调用 Claude
    print("🤖 调用 Claude API...")
    t0 = time.perf_counter()

    try:
        agent = AnalystAgent(api_key=CLAUDE_API_KEY, model=CLAUDE_MODEL, base_url=CLAUDE_BASE_URL)
        analysis = agent.analyze(
            system_prompt, user_prompt,
            max_tokens=max_tokens,
            thinking=ANALYST_CONFIG.get('thinking_enabled', True),
            temperature=ANALYST_CONFIG.get('temperature', 1.0),
        )
    except Exception as e:
        logger.error(f"Claude API 调用失败: {e}")
        sys.exit(1)

    elapsed = time.perf_counter() - t0
    print(f"✅ API 调用完成 ({elapsed:.1f}s)")

    # 更新知识库（仅周报）— 先提取再剥离，报告里不保留 raw JSON
    if mode == "weekly":
        kb_update = agent.extract_kb_update(analysis)
        _update_kb_from_response(kb_update)
        analysis = re.sub(r'```kb-update\s*\n.*?\n```', '', analysis, flags=re.DOTALL).strip()

    # 输出报告
    if mode == "daily":
        write_daily_report(analysis, today_str)
        report_file = f"reports/01-每日简报/{today_str}.md"
    else:
        year_week_str = f"{date.today().year}-W{week_num:02d}"
        write_weekly_report(analysis, year_week_str)
        report_file = f"reports/02-每周深度/{year_week_str}.md"

    print(f"📄 报告已保存: {report_file}")
    print(f"{'=' * 60}")

    # 输出 structured stats（供 run_all.py 整合）
    print(f'__STATS__{{"report": "{report_file}", "mode": "{mode}", "elapsed_s": {elapsed:.1f}}}', flush=True)


if __name__ == "__main__":
    main()
