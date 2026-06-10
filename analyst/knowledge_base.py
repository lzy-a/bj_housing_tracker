"""
知识库读写：管理 reports/ 目录下的持久化 .md 文件。
支持 frontmatter 解析和追加式/替换式更新。
"""

import os
import re
import logging
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

# 默认 reports 目录（可通过 settings 覆盖）
PROJECT_ROOT = Path(__file__).parent.parent
REPORTS_DIR = PROJECT_ROOT / "reports"

# 子目录
DIR_OVERVIEW    = "00-总览"
DIR_DAILY       = "01-每日简报"
DIR_WEEKLY      = "02-每周深度"
DIR_DISTRICTS   = "03-区域档案"
DIR_HYPOTHESES  = "04-假设追踪"
DIR_WATCHLIST   = "05-重点关注"
DIR_BIZ_CIRCLE  = "06-板块档案"
DIR_BUYER        = "05-买房决策"

DISTRICTS = ["东城区", "西城区", "海淀区", "朝阳区", "丰台区", "石景山区"]


def _ensure_dirs():
    for d in [DIR_OVERVIEW, DIR_DAILY, DIR_WEEKLY, DIR_DISTRICTS,
              DIR_HYPOTHESES, DIR_WATCHLIST, DIR_BIZ_CIRCLE, DIR_BUYER]:
        os.makedirs(REPORTS_DIR / d, exist_ok=True)


def _parse_frontmatter(text: str) -> tuple:
    """解析 .md 文件的 YAML frontmatter，返回 (frontmatter_dict, body_text)。"""
    if not text.startswith("---"):
        return {}, text
    parts = text.split("---", 2)
    if len(parts) < 3:
        return {}, text
    frontmatter = {}
    for line in parts[1].strip().split("\n"):
        line = line.strip()
        if ":" in line:
            key, _, val = line.partition(":")
            frontmatter[key.strip()] = val.strip()
    return frontmatter, parts[2].strip()


def _serialize_frontmatter(fm: dict) -> str:
    """将 frontmatter dict 序列化为 YAML 字符串。"""
    lines = ["---"]
    for k, v in fm.items():
        if isinstance(v, (list, tuple)):
            lines.append(f"{k}:")
            for item in v:
                lines.append(f"  - {item}")
        elif v is not None:
            lines.append(f"{k}: {v}")
    lines.append("---")
    return "\n".join(lines)


def _format_observation(observation) -> str:
    """将旧字符串或新结构化观察渲染成 Obsidian 友好的 Markdown。"""
    if isinstance(observation, str):
        return observation
    if not isinstance(observation, dict):
        return str(observation)

    claim = observation.get("claim") or observation.get("description") or observation.get("finding") or "未命名观察"
    evidence = observation.get("evidence", "")
    source = observation.get("source", "")
    confidence = observation.get("confidence", "")
    decision_impact = observation.get("decision_impact") or observation.get("impact", "")

    lines = [f"Claim: {claim}"]
    if evidence:
        lines.append(f"Evidence: {evidence}")
    if source:
        lines.append(f"Source: {source}")
    if confidence:
        lines.append(f"Confidence: {confidence}")
    if decision_impact:
        lines.append(f"Decision Impact: {decision_impact}")
    return "\n".join(lines)


def _recent_sections(body: str, limit: int = 3, max_chars: int = 1600) -> str:
    """提取最近 N 个三级标题段落，避免把整个档案塞进 prompt。"""
    if not body:
        return ""
    matches = list(re.finditer(r"(?m)^### .*$", body))
    if not matches:
        return body[-max_chars:]

    sections = []
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(body)
        sections.append(body[start:end].strip())
    recent = "\n\n".join(sections[-limit:])
    if len(recent) > max_chars:
        return recent[-max_chars:]
    return recent


# ================================================================
# 区域档案（复利层核心）
# ================================================================

def read_district_profile(region: str) -> dict:
    """读取某个区的档案，返回 {frontmatter, body}。"""
    path = REPORTS_DIR / DIR_DISTRICTS / f"{region}.md"
    if not path.exists():
        return {"frontmatter": {}, "body": ""}
    fm, body = _parse_frontmatter(path.read_text(encoding="utf-8"))
    return {"frontmatter": fm, "body": body}


def read_all_district_profiles() -> dict:
    """读取全部 6 个区的档案。"""
    return {r: read_district_profile(r) for r in DISTRICTS}


def write_district_profile(region: str, frontmatter: dict, body: str):
    """覆写某个区的档案。"""
    _ensure_dirs()
    path = REPORTS_DIR / DIR_DISTRICTS / f"{region}.md"
    fm_str = _serialize_frontmatter(frontmatter)
    path.write_text(f"{fm_str}\n\n{body}", encoding="utf-8")


def update_district_stats(region: str, stats: dict):
    """更新区域档案的 frontmatter 统计字段，保留正文不变。"""
    profile = read_district_profile(region)
    fm = profile["frontmatter"]
    fm.update(stats)
    fm["last_updated"] = date.today().isoformat()
    write_district_profile(region, fm, profile["body"])


def append_district_observation(region: str, observation):
    """向区域档案正文追加一条按日期标记的观察。"""
    profile = read_district_profile(region)
    body = profile["body"]
    today = date.today().isoformat()
    new_entry = f"\n\n### {today}\n{_format_observation(observation)}"
    body = (body + new_entry).strip()
    fm = profile["frontmatter"]
    fm["last_updated"] = today
    if "observation_count" in fm:
        fm["observation_count"] = int(fm["observation_count"]) + 1
    else:
        fm["observation_count"] = 1
    write_district_profile(region, fm, body)


def read_recent_district_observations(region: str, limit: int = 3) -> str:
    """读取某区最近观察，用于周报上下文。"""
    profile = read_district_profile(region)
    return _recent_sections(profile.get("body", ""), limit=limit)


# ================================================================
# 假设追踪
# ================================================================

def read_hypotheses() -> dict:
    """读取活跃假设文件。"""
    path = REPORTS_DIR / DIR_HYPOTHESES / "活跃假设.md"
    if not path.exists():
        return {"frontmatter": {}, "body": ""}
    fm, body = _parse_frontmatter(path.read_text(encoding="utf-8"))
    return {"frontmatter": fm, "body": body}


def write_hypotheses(frontmatter: dict, body: str):
    """覆写活跃假设文件。"""
    _ensure_dirs()
    path = REPORTS_DIR / DIR_HYPOTHESES / "活跃假设.md"
    fm_str = _serialize_frontmatter(frontmatter)
    path.write_text(f"{fm_str}\n\n{body}", encoding="utf-8")


def append_hypothesis(hypothesis: str):
    """追加一条新假设。"""
    h = read_hypotheses()
    today = date.today().isoformat()
    h["body"] = (h["body"] + f"\n\n### [{today}] 新假设\n{hypothesis}").strip()
    fm = h["frontmatter"]
    fm["last_updated"] = today
    fm["total_hypotheses"] = int(fm.get("total_hypotheses", 0)) + 1
    write_hypotheses(fm, h["body"])


def append_new_hypothesis(hypothesis: dict):
    """追加一条 ledger 风格的新假设，兼容简单 dict。"""
    h = read_hypotheses()
    today = date.today().isoformat()
    fm = h["frontmatter"]
    next_id = int(fm.get("total_hypotheses", 0)) + 1
    hid = hypothesis.get("id") or f"H-{today.replace('-', '')}-{next_id:03d}"
    title = hypothesis.get("title", "未命名假设")
    confidence = hypothesis.get("confidence", "low")
    claim = hypothesis.get("claim") or hypothesis.get("description", "")
    evidence = hypothesis.get("evidence", "")
    impact = hypothesis.get("decision_impact", "")

    entry = [
        f"## {hid} {title}",
        "",
        "status: active",
        f"confidence: {confidence}",
        f"created: {today}",
        f"last_updated: {today}",
        "",
        "### Claim",
        "",
        claim or "待补充。",
    ]
    if evidence:
        entry.extend(["", "### Evidence Log", "", f"- {today}: {evidence}"])
    if impact:
        entry.extend(["", "### Decision Impact", "", impact])

    h["body"] = (h["body"] + "\n\n" + "\n".join(entry)).strip()
    fm["last_updated"] = today
    fm["total_hypotheses"] = next_id
    write_hypotheses(fm, h["body"])


def append_hypothesis_update(update: dict):
    """追加一条假设验证记录，保留旧内容但用 ledger 证据日志格式。"""
    h = read_hypotheses()
    today = date.today().isoformat()
    hid = update.get("id", "")
    title = update.get("title", "未命名假设")
    status = update.get("status", "updated")
    confidence = update.get("confidence", "")
    evidence = update.get("evidence") or update.get("finding", "")
    impact = update.get("decision_impact", "")

    heading = f"### [{today}] {hid + ' ' if hid else ''}{title}"
    lines = [
        heading,
        f"Status: {status}",
    ]
    if confidence:
        lines.append(f"Confidence: {confidence}")
    if evidence:
        lines.append(f"Evidence: {evidence}")
    if impact:
        lines.append(f"Decision Impact: {impact}")

    h["body"] = (h["body"] + "\n\n" + "\n".join(lines)).strip()
    fm = h["frontmatter"]
    fm["last_updated"] = today
    write_hypotheses(fm, h["body"])


# ================================================================
# 重点关注
# ================================================================

def write_watchlist(markdown_table: str, frontmatter: dict = None):
    """覆写值得关注小区清单。"""
    _ensure_dirs()
    path = REPORTS_DIR / DIR_WATCHLIST / "值得关注小区.md"
    fm = frontmatter or {}
    fm["last_updated"] = date.today().isoformat()
    fm_str = _serialize_frontmatter(fm)
    path.write_text(f"{fm_str}\n\n{markdown_table}", encoding="utf-8")


# ================================================================
# 市场状态仪表盘（总览）
# ================================================================

def write_dashboard(daily_summary: str):
    """生成 / 更新首页仪表盘。"""
    _ensure_dirs()
    path = REPORTS_DIR / DIR_OVERVIEW / "市场状态仪表盘.md"
    today = date.today().isoformat()
    # 读取已有内容，在顶部插入最新状态
    existing = ""
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        # 跳过 frontmatter
        _, existing = _parse_frontmatter(existing)

    fm = {
        "last_updated": today,
        "total_daily_reports": 0,  # 后续更新
    }
    fm_str = _serialize_frontmatter(fm)

    content = f"{fm_str}\n\n# 北京房产市场状态\n\n**最后更新**: {today}\n\n{daily_summary}\n\n---\n\n{existing}"
    path.write_text(content.strip() + "\n", encoding="utf-8")


# ================================================================
# 板块档案（复利层扩展）
# ================================================================

def append_biz_circle_observation(biz_circle: str, observation):
    """向板块档案正文追加一条按日期标记的观察。"""
    _ensure_dirs()
    path = REPORTS_DIR / DIR_BIZ_CIRCLE / f"{biz_circle}.md"
    today = date.today().isoformat()

    if path.exists():
        fm, body = _parse_frontmatter(path.read_text(encoding="utf-8"))
    else:
        fm, body = {}, ""

    new_entry = f"\n\n### {today}\n{_format_observation(observation)}"
    body = (body + new_entry).strip()
    fm["last_updated"] = today
    if "observation_count" in fm:
        fm["observation_count"] = int(fm["observation_count"]) + 1
    else:
        fm["observation_count"] = 1

    fm_str = _serialize_frontmatter(fm)
    path.write_text(f"{fm_str}\n\n{body}", encoding="utf-8")


# ================================================================
# 购买力评估
# ================================================================

def write_buying_assessment(assessment: dict):
    """覆写购买力评估文件。"""
    _ensure_dirs()
    path = REPORTS_DIR / DIR_BUYER / "当前行动建议.md"
    today = date.today().isoformat()

    fm = {
        "last_updated": today,
        "favorability": assessment.get("overall_favorability", "neutral"),
        "recommended_action": assessment.get("recommended_action", "观望"),
    }
    fm_str = _serialize_frontmatter(fm)
    watch_next = assessment.get("watch_next", [])
    if isinstance(watch_next, str):
        watch_next = [watch_next]
    watch_text = "\n".join(f"- {item}" for item in watch_next) if watch_next else "- 暂无。"

    body = f"""# 购买力评估

**最后更新**: {today}

**市场判断**: {assessment.get('overall_favorability', '—')}

**建议行动**: {assessment.get('recommended_action', '—')}

**判断依据**: {assessment.get('reasoning', '—')}

## 下周观察

{watch_text}
"""
    path.write_text(f"{fm_str}\n\n{body}", encoding="utf-8")


def read_buying_assessment() -> str:
    """读取当前买房行动建议，用于周报上下文。"""
    path = REPORTS_DIR / DIR_BUYER / "当前行动建议.md"
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")[:2000]


# ================================================================
# 上周报告读取
# ================================================================

def read_last_weekly_report() -> str:
    """读取最近一期周刊报告，用于注入下周 prompt。"""
    weekly_dir = REPORTS_DIR / DIR_WEEKLY
    if not weekly_dir.exists():
        return ""
    reports = sorted(weekly_dir.glob("*.md"), reverse=True)
    if not reports:
        return ""
    return reports[0].read_text(encoding="utf-8")
