# Agent Collaboration Protocol

This directory is the file-based coordination layer between Codex and Claude Code.
Chat can start work, but files here are the source of truth.

## Roles

- Codex: planner, architect, reviewer, and release gate.
- Claude Code: implementation agent. Claude Code executes only READY tasks and reports results here.
- Human: final priority setter and owner of credentials, production runs, and deployment.

## State Machine

Every task in `.agents/tasks/` has a `status` field:

- `DRAFT`: Codex is still shaping the task.
- `READY_FOR_CLAUDE`: Claude Code may start this task.
- `IN_PROGRESS`: Claude Code has started work.
- `SUBMITTED_FOR_REVIEW`: Claude Code has finished and written a report.
- `NEEDS_REVISION`: Codex found blocking issues and wrote a review.
- `ACCEPTED`: Codex accepted the implementation.
- `CANCELLED`: The task is no longer needed.

Claude Code may change `READY_FOR_CLAUDE` to `IN_PROGRESS` and then to
`SUBMITTED_FOR_REVIEW`. Codex owns `DRAFT`, `NEEDS_REVISION`, `ACCEPTED`, and
`CANCELLED`.

## Standard Loop

1. Codex writes or updates `.agents/brief.md`, `.agents/plan.md`, and one task file.
2. Claude Code reads `.agents/brief.md`, `.agents/invariants.md`, `.agents/test-protocol.md`, and its task file.
3. Claude Code implements only the requested scope.
4. Claude Code writes `.agents/reports/<task-id>-result.md`.
5. Claude Code updates the task status to `SUBMITTED_FOR_REVIEW`.
6. Codex reviews the diff, commands, and report.
7. Codex writes `.agents/reviews/<task-id>-review.md`.
8. If accepted, Codex marks the task `ACCEPTED`; if not, Codex marks it `NEEDS_REVISION` and writes a fixup task or revision notes.

## File Naming

- Tasks: `.agents/tasks/001-short-slug.md`
- Reports: `.agents/reports/001-short-slug-result.md`
- Reviews: `.agents/reviews/001-short-slug-review.md`

Keep IDs stable. Do not renumber tasks after creation.

## Claude Code Rules

- Do not pick up `DRAFT`, `NEEDS_REVISION`, `ACCEPTED`, or `CANCELLED` tasks unless explicitly told.
- Do not expand scope without writing a `QUESTION` or `BLOCKED` section in the report.
- Do not silently ignore failing tests or commands.
- Do not rewrite unrelated files.
- Do not change credentials, secrets, or local environment files unless the task explicitly asks for it.
- When blocked, stop after writing a report instead of guessing.

## Codex Review Rules

- Review behavior, tests, and data safety before style.
- Treat crawler delisting, schema changes, and analysis SQL as high-risk surfaces.
- Prefer small follow-up tasks over broad rewrites.
- Before issuing or recording a revision that changes a business/data policy tradeoff, explain the tradeoff to the human owner and get direction.
- Archive accepted task/report/review sets only after the human agrees or the queue gets noisy.
