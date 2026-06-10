# Analyst Memory System Design

status: draft
owner: Codex
created: 2026-06-10

## Purpose

The analyst should become a compounding research partner for a future Beijing home purchase decision.

The system should not rely on one model's hidden memory. The memory lives in Markdown/Obsidian files. Models are replaceable weekly analysts that read the same external memory, inspect fresh data, and update the memory through structured outputs.

Core principle:

```text
Daily brief = radar
Weekly report = hypothesis audit
Obsidian = long-term memory
LLM = replaceable reasoning engine
```

## Non-Goals

- Do not turn every daily signal into long-term memory.
- Do not create a large knowledge graph before the research workflow is proven.
- Do not depend on a single API/model vendor.
- Do not let the model write unsupported market commentary into long-term files.

## Operating Cadence

### Daily Brief

Purpose: a fast glance.

The daily brief answers:

```text
Is there anything unusual enough that I should notice today?
```

It should be short and disposable:

- 0-3 notable signals;
- specific listings, communities, or numbers when relevant;
- no forced conclusion if the day is quiet;
- no knowledge-base update by default.

Daily output belongs in:

```text
reports/01-每日简报/YYYY-MM-DD.md
```

Daily brief should not:

- update hypotheses;
- rewrite region profiles;
- make broad buying recommendations;
- create long-term memories from one-day noise.

### Weekly Report

Purpose: compounding judgment.

The weekly report answers:

```text
Did this week's data change our understanding of the market or buying strategy?
```

It should:

- summarize the week's market state;
- compare fresh data against prior hypotheses;
- strengthen, weaken, confirm, or refute hypotheses;
- update regional and biz-circle memory only when there is evidence;
- update the buyer decision page.

Weekly output belongs in:

```text
reports/02-每周深度/YYYY-Www.md
```

The weekly report is a report plus a structured `kb-update` payload. The report is human-readable; the payload is machine-validated and used to update Obsidian memory.

## Obsidian Memory Layers

### 1. Raw Reports

Raw reports are immutable historical records.

```text
reports/01-每日简报/
reports/02-每周深度/
```

They are useful for replay and audit, but they are not the primary long-term brain.

### 2. Entity Memory

Entity memory stores compact, evidence-backed observations about stable objects:

```text
reports/03-区域档案/东城区.md
reports/03-区域档案/西城区.md
reports/03-区域档案/海淀区.md
reports/03-区域档案/朝阳区.md
reports/03-区域档案/丰台区.md
reports/03-区域档案/石景山区.md
reports/06-板块档案/望京.md
reports/06-板块档案/金融街.md
```

Entity memory should be append-oriented, but not noisy. Add observations only when they are decision-relevant or hypothesis-relevant.

Recommended observation shape:

```md
### 2026-W24

Claim: [[海淀区]] 挂牌量增加但价格尚未明显松动。
Evidence: 近3日挂牌量 MA3 较 7-9 日前 +2.4%，中位价 -0.2%。
Source: district_wow
Confidence: medium
Decision Impact: 继续观察，不因单周供应增加就急于出手。
```

### 3. Hypothesis Ledger

The hypothesis ledger is the core compounding mechanism.

File:

```text
reports/04-假设追踪/假设台账.md
```

Each hypothesis should have:

- stable ID;
- status;
- confidence;
- claim;
- evidence log;
- decision impact.

Recommended shape:

```md
## H-001 西城核心区抗跌假设

status: active
confidence: medium
created: 2026-W22
last_updated: 2026-W24

### Claim

[[西城区]] 核心板块在全市下行时回撤幅度小于非核心区。

### Evidence Log

- 2026-W23: 西城中位价周环比 -0.1%，挂牌量 +0.3%，弱于全市跌幅。
- 2026-W24: 继续支持，但需要排除高价房源结构变化。

### Decision Impact

预算足够时可优先跟踪西城核心区，但不能仅因“抗跌”支付过高溢价。
```

Allowed statuses:

```text
active
strengthened
weakened
confirmed
refuted
paused
```

Confidence:

```text
low
medium
high
```

### 4. Buyer Decision Memory

This layer connects market analysis to the user's actual purchase decision.

Files:

```text
reports/05-买房决策/当前行动建议.md
reports/05-买房决策/预算与目标.md
```

`当前行动建议.md` should answer:

- current market favors buyer / seller / neutral?
- should the user wait, start watching listings, or actively bid?
- what evidence changed this week?
- what would change the recommendation?

Recommended shape:

```md
# 当前行动建议

last_updated: 2026-W24
overall_favorability: buyers_market
recommended_action: 开始看

## Recommendation

可以开始系统看房，但不急于出手。

## Evidence

- 降价占比连续两周上升，但核心区价格回撤仍小。
- 租赁端海淀租金稳定，尚未出现明显基本面恶化。

## What Would Change This

- 若目标板块连续两周出现挂牌增加且中位价下行，可提高出价积极性。
- 若核心小区成交/挂牌价企稳并挂牌减少，则继续观望。
```

## Weekly Input Context

The weekly analyst should read a bounded context:

1. latest weekly report summary;
2. active hypotheses from the ledger;
3. recent 3-5 observations per district;
4. current buyer decision page;
5. fresh SQL tables.

It should not read every historical report in full.

## `kb-update` Schema

The LLM may write a free-form weekly report, but memory updates should use a structured JSON block.

Recommended schema:

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
  "biz_circle_observations": {
    "望京": {
      "claim": "望京改善盘议价信号增强",
      "evidence": "近7天降价房源集中在改善户型",
      "source": "biz_resilience / price_adjustments_7day",
      "confidence": "low",
      "decision_impact": "纳入下周重点观察"
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
  "new_hypotheses": [
    {
      "title": "朝阳改善盘先于刚需盘松动",
      "confidence": "low",
      "claim": "朝阳大面积改善房源可能先出现议价空间",
      "evidence": "本周降价TOP中朝阳改善房源占比上升",
      "decision_impact": "关注朝阳改善盘报价弹性"
    }
  ],
  "buyer_decision_update": {
    "overall_favorability": "buyers_market",
    "recommended_action": "开始看",
    "reasoning": "降价信号持续但核心区域尚未全面松动",
    "watch_next": ["目标板块挂牌量", "核心小区降价比例", "租金稳定性"]
  }
}
```

Backward compatibility:

- existing string observations should still be accepted;
- new dict observations are preferred;
- invalid JSON should not update memory automatically.

## Model/API Portability

The system should continue if the LLM provider changes.

Rules:

- Memory is stored in Markdown, not model-specific hidden state.
- Prompts explain the role and update schema every run.
- Structured `kb-update` is validated before writing.
- If model output fails schema validation, save the raw report but skip memory writes.
- Keep model-specific config in settings, not in memory files.

## Implementation Plan

### Phase 1: Conservative Memory Upgrade

- Read recent district observations into weekly context.
- Support dict-shaped observations while preserving string compatibility.
- Render evidence-backed observations in district/biz-circle files.
- Add a ledger-style hypothesis append/update helper.
- Update prompt schema.

### Phase 2: Validation And Safety

- Add JSON schema validation for `kb-update`.
- Separate SQL query errors from legitimate empty data.
- Add a clear "data quality" section in weekly reports.

### Phase 3: Decision Support

- Add `reports/05-买房决策/当前行动建议.md`.
- Make weekly report explicitly update buying recommendation.
- Track what evidence would change the recommendation.

## Open Questions

- How many weeks of evidence are needed before a hypothesis can be `confirmed`?
- Should low-confidence new hypotheses enter the ledger automatically, or wait for one week of follow-up?
- Should the buyer decision page encode the user's budget and target districts, or should that stay manual?
- Should daily anomalies ever promote to weekly memory automatically, or only through weekly review?
