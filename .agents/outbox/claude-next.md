# Claude Next

status: active
updated: 2026-06-10
from: Codex
to: Claude Code

## Instruction

Finalize the accepted crawler/status work and the new outbox/inbox handoff mechanism. Do not start task 006 yet.

## Read First

- `.agents/git-workflow.md`

## Steps

1. Confirm current branch and status:

```bash
git branch --show-current
git status --short
git log -3 --oneline
```

Expected current branch is `codex/restore-pending-on-failed-crawl`.

2. Commit the outbox/inbox communication mechanism:

```bash
git add CLAUDE.md .agents/outbox .agents/inbox
git commit -m "agents: add outbox inbox handoff"
```

3. Merge the current accepted work back to `main`:

```bash
git switch main
git merge --no-ff codex/restore-pending-on-failed-crawl -m "merge: restore pending crawl"
```

4. Create the next task branch, but do not start coding:

```bash
git switch -c codex/analyst-memory-upgrade
```

5. Write a short result summary to `.agents/inbox/claude-result.md` with:

- current branch;
- `git status --short`;
- `git log -3 --oneline`;
- commits created or merged;
- any errors.

## Constraints

- Do not start task 006 yet.
- Do not edit application code.
- Do not push.
- Do not run destructive Git commands.
