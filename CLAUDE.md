# CLAUDE.md

Instructions for Claude Code in this repo. I use Claude Code every day, and this file is how I keep its output at the same standard as my own code.

## What this repo is

A payments pipeline that shows three ways Kafka pipelines lose data without crashing (`naive` mode), how to fix them (`safe` mode), and how to detect them (the reconciler). See `README.md` and `docs/specs/`.

## How changes are made

1. **Spec first.** A non-trivial change starts as a short design in `docs/specs/YYYY-MM-DD-<topic>-design.md`, committed before any code.
2. **Plan.** Steps go in `docs/plans/`. Each step has a test that fails first.
3. **TDD.** Failing test, then code, then green. Pure logic (`FaultInjector`, `PaymentEventCodec`, `ReconciliationReport`) has unit tests. The pipeline has integration tests with real Kafka and Postgres (Testcontainers).
4. **Numbers come from runs.** Every number in the README comes from `./mvnw verify` or `scripts/run-scenario.sh`. If a change moves a number, update the README in the same commit.

## Commands

- `./mvnw test`: unit tests (fast, no Docker).
- `./mvnw verify`: unit and integration tests (needs Docker).
- `node --test ".claude/hooks/*.test.mjs"`: tests for the hook below.
- `scripts/run-scenario.sh naive|safe`: full stack with Grafana on http://localhost:3001.

## Rules

- **Never edit a committed Flyway migration.** Add `V<n+1>__<what>.sql`. The hook in `.claude/hooks/guard.mjs` blocks it.
- **No force push and no `--no-verify`.** The same hook blocks them.
- **Naive mode stays broken on purpose.** Do not "fix" it: it is the "before" picture. Fixes go in safe mode.
- **The expected counts in `ScenarioExpectations` are an independent model.** Do not compute them from the database or from the pipeline code, or the integration tests check nothing.
- Keep the English simple and the comments short. Explain *why*, not *what*.
