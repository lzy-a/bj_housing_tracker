# Task 006 Result: Upgrade Analyst Obsidian Memory Protocol

## Status: ACCEPTED

Implemented directly by Codex after the owner decided to finish this module instead of continuing handoff loops.

## Changed Files

- `analyst/knowledge_base.py`
- `analyst/run_analyst.py`
- `analyst/prompt_templates.py`
- `analyst/extractor.py`
- `etl/db_manager.py`
- `.agents/design/analyst-memory-system.md`
- `.agents/tasks/006-upgrade-analyst-obsidian-memory.md`
- `.agents/plan.md`

## Behavior Summary

### Daily Brief

Daily brief behavior remains lightweight. It is still a quick radar and does not update long-term memory by default.

### Weekly Memory Context

Weekly reports now read more useful historical context:

- recent district observations, not just observation counts;
- active hypotheses;
- latest weekly report excerpt;
- current buyer decision page if present.

### Evidence-Backed Observations

District and biz-circle memory writers now accept both:

- old string observations;
- new dict observations with fields like `claim`, `evidence`, `source`, `confidence`, and `decision_impact`.

Structured observations render as Markdown:

```md
### 2026-06-10
Claim: [[海淀区]] 挂牌量增加但价格尚未明显松动
Evidence: 近3日挂牌量 MA3 较 7-9 日前 +2.4%，中位价 -0.2%
Source: district_wow
Confidence: medium
Decision Impact: 继续观察，不因单周供应增加就急于出手
```

### Hypothesis Ledger

Added ledger-style helpers:

- `append_new_hypothesis()`
- `append_hypothesis_update()`

New hypotheses receive stable IDs when no ID is provided.

Hypothesis updates now write status, confidence, evidence, and decision impact instead of only loose prose.

### Buyer Decision Page

Buying assessment now writes to:

```text
reports/05-买房决策/当前行动建议.md
```

The weekly context reads this page back in future runs.

### Data Extraction Status

`DataExtractor` now returns `QueryResult` objects so analyst formatting can distinguish:

- `ok`
- `empty`
- `error`

`DatabaseManager.execute_query()` gained a backward-compatible `strict=False` parameter. Analyst extraction uses `strict=True`.

## New `kb-update` Shape

The weekly prompt now asks for evidence-backed JSON:

```json
{
  "district_observations": {
    "海淀区": {
      "claim": "海淀挂牌量增加但价格尚未明显松动",
      "evidence": "近3日挂牌量MA3较7-9日前+2.4%，中位价-0.2%",
      "source": "district_wow",
      "confidence": "medium",
      "decision_impact": "继续观察，不因单周供应增加就急于出手"
    }
  },
  "hypothesis_updates": [
    {
      "id": "H-001",
      "title": "西城核心区抗跌假设",
      "status": "strengthened",
      "confidence": "medium",
      "evidence": "西城中位价周环比弱跌，挂牌量变化小于全市",
      "decision_impact": "继续关注西城核心区，但避免高溢价"
    }
  ],
  "buyer_decision_update": {
    "overall_favorability": "buyers_market",
    "recommended_action": "开始看",
    "reasoning": "降价信号持续但核心区域尚未全面松动",
    "watch_next": ["目标板块挂牌量", "核心小区降价比例"]
  }
}
```

Old string-style observations remain compatible.

## Commands Run

```bash
venv/bin/python -m py_compile analyst/run_analyst.py analyst/knowledge_base.py analyst/prompt_templates.py analyst/extractor.py etl/db_manager.py
venv/bin/python run_analyst.py --dry-run
venv/bin/python run_analyst.py --mode weekly --dry-run
```

All passed.

## Risks

- This does not yet validate `kb-update` against a formal JSON schema.
- Existing hypothesis text is preserved; it is not migrated into full ledger blocks.
- Real LLM weekly output was not run, so the first live weekly report should be inspected.
