# Current Brief

## Mission

Build and maintain a reliable Beijing real-estate research agent:

- scrape Beijing 5i5j sale and rent listings;
- write trustworthy PostgreSQL history tables and daily snapshots;
- generate AI market reports and persistent Markdown knowledge;
- support finder workflows for rental screening.

## Collaboration Setup

Codex is the planner/reviewer. Claude Code is the implementation agent.
All cross-agent handoff happens through `.agents/`.

## Current Priority

Stabilize correctness before adding new features. The first pass focuses on:

1. preventing crawler failures from causing false delisting;
2. fixing analysis SQL that can produce incorrect week-over-week metrics;
3. making required database views/schema explicit;
4. reducing duplicate history rows and connection-pool fragility.

## Non-Negotiables

- A failed or suspicious crawl must not mark active listings as delisted.
- Market reports must not silently treat missing critical data as real zero-data signals.
- Schema changes must be repeatable and safe on an existing PostgreSQL database.
- Real credentials must stay in `.env` or local secret stores, not in committed source.
- Metabase export JSON is read-only; dashboard changes go through the API workflow.

## Working Agreement

Claude Code should implement one task at a time, write a result report, and stop for review.
Codex will review before the next task is started.
