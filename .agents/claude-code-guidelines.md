# Claude Code Operating Guidelines

These guidelines clarify how Claude Code should work with Codex reviews in this repository.

## Primary Rule

Implement only the active task. When Codex review asks for revisions, apply only the requested revision and then stop for review again.

## Git Rules

Read `.agents/git-workflow.md` before doing any branch, commit, checkout, or merge operation.

Short version:

- `main` is the stable runnable baseline.
- Task work happens on `codex/<task-slug>` branches.
- Do not develop directly on `main` unless the human explicitly asks for a baseline snapshot.
- Do not commit a task until Codex marks it `ACCEPTED`, unless the human explicitly asks for a WIP/baseline commit.
- Never run destructive Git commands such as `git reset --hard`, `git checkout -- .`, or `git clean -fd` unless the human explicitly requests that exact operation.
- Never push unless the human explicitly asks.

## Business Policy Tradeoffs

Some changes are not just code mechanics; they change how the research system interprets data. Examples:

- whether a crawler run is trustworthy enough to mark listings as delisted;
- whether empty data means "no market activity" or "pipeline failure";
- whether same-day price history should overwrite or append;
- whether an LLM memory update should replace or append prior claims.

For these policy choices:

- Do not invent a stricter or looser policy unless the task/review explicitly says so.
- If Codex review seems to change a business policy, follow the written review only after the human owner has confirmed the direction or the review states that the human owner confirmed it.
- If behavior has meaningful operational cost, write a `QUESTION` in the result report instead of guessing.

## Crawler Delisting Policy

Delisting is a destructive data-state update. It must be conservative, but it must also reflect how 5i5j behaves in practice.

For this project:

- Recovered click verification is normal operating noise.
- A page that shows click verification, recovers after the simulated click, shows `ul.pList`, and parses normally may count as success.
- A page that cannot recover from click verification is a hard failure.
- Login failure, unknown empty page, navigation/page exception, parser exception, and worker exception are hard failures.
- A region may mark disappeared listings only when it reaches normal pagination end and has no hard failures.

Useful taxonomy:

- `success`: normal listing page parsed.
- `no_data`: known empty/end-of-pagination page.
- `soft_challenge`: click verification recovered; may be logged but should not block delisting.
- `hard_failure` or `suspicious`: unsafe page/run condition; should block delisting for that region.

## Reports

Result reports should describe behavior in operational terms, not only code terms. For crawler tasks, explicitly state:

- what conditions allow delisting;
- what conditions skip delisting;
- what is logged for recovered challenges or failures;
- what checks were run.

## Stop Conditions

After writing the result report and setting the task to `SUBMITTED_FOR_REVIEW`, stop. Do not start the next task until Codex accepts the current task.
