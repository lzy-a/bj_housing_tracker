# Project Invariants

These rules protect the research data from subtle corruption.

## Crawler and Status Lifecycle

- A listing may be marked `status = 0` only after the corresponding region crawl completed successfully.
- Anti-crawl pages, login prompts, navigation failures, parser failures, and unknown empty pages are not valid evidence of delisting.
- Normal end-of-pagination is valid only when detected by known no-data signals or another explicit completion condition.
- Sale and rent crawlers should use the same lifecycle semantics unless a task explicitly defines a difference.
- Region-level crawl outcome should be observable in logs or structured state: completed, failed, skipped delist.

## Price and Rent History

- History tables represent real observed changes, not every crawl.
- Re-running a crawler on the same day should not duplicate identical history facts.
- If same-day overwrite behavior is introduced, it must be explicit and reflected in analysis SQL.

## Analysis and Reports

- Critical SQL failures must be visible. Do not let missing views/tables become indistinguishable from empty market data.
- Week-over-week windows must rank dates, not arbitrary region rows.
- Aggregations should remain in SQL where practical; LLM prompts should consume compact tables.
- Knowledge base updates must be append-only unless the task explicitly asks for replacement.

## Database

- Schema initialization must be idempotent.
- Connection-pool connections must be returned to the pool, not directly closed, unless the pool itself is being shut down.
- Existing user data must not be dropped or reset in migrations.

## Metabase

- `export/` JSON is reference material, not an edit target.
- Metabase card/dashboard changes must use API workflow and be tested with all relevant parameters.

## Security

- Do not add real credentials, phone numbers, API keys, or passwords to source files.
- Defaults in code should be empty, local-only, or harmless placeholders.
