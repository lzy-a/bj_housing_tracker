# Git Workflow

This repository keeps `main` as the stable runnable baseline. Codex/Claude Code
task work should happen on short-lived branches and be merged back only after
review.

## Branch Roles

- `main`: stable baseline. Daily crawler/analyst runs should use this branch.
- `codex/<task-slug>`: task branch for Codex/Claude Code collaboration.

Examples:

- `codex/crawler-delist-guard`
- `codex/analyst-wow-sql`
- `codex/analyst-memory-upgrade`

## Before Starting Work

Claude Code must run and report:

```bash
git branch --show-current
git status --short
```

If on `main` and code changes are needed, create a task branch first:

```bash
git switch -c codex/<task-slug>
```

If the branch already exists, stop and report. Do not force overwrite branches.

## During Work

- Keep each task's changes scoped to its `.agents/tasks/NNN-*.md` file.
- Do not mix unrelated tasks in one branch.
- Do not start the next task until Codex accepts the current one.
- Do not push unless the human explicitly asks.

## Commit Policy

Preferred shape: one accepted task, one commit.

Commit only after:

- the task status is `ACCEPTED`, or
- the human explicitly asks for a baseline/WIP snapshot.

Commit message format:

```text
<area>: <short summary>
```

Examples:

```text
agents: add collaboration workflow
crawler: guard delisting on failed region crawls
analyst: fix wow date ranking
analyst: upgrade obsidian memory protocol
```

Before committing, Claude Code must report:

```bash
git branch --show-current
git status --short
git diff --stat
```

After committing, Claude Code must report:

```bash
git log -1 --oneline
git status --short
```

## Baseline Snapshot Exception

Sometimes the human may decide the current working tree is the new stable
baseline. In that case, Claude Code may commit all current changes on `main`
only when explicitly instructed.

Use a message like:

```text
baseline: current real estate research agent
```

After the baseline commit, future work should move to a `codex/<task-slug>`
branch.

## Dangerous Commands

Claude Code must not run these unless the human explicitly asks for the exact
operation:

```bash
git reset --hard
git checkout -- .
git clean -fd
git rebase
git push --force
```

If a command might discard uncommitted work, stop and ask.

## Running Versions

- To run the stable system: use `main`.
- To test an in-progress task: use that task's `codex/<task-slug>` branch.
- If a task branch is not accepted yet, do not treat it as the daily production
  crawler version.
