# Kafka silent failures — design

## Intent

A public, runnable portfolio repo that reproduces the three most common ways an
event pipeline loses or corrupts data **without crashing**, fixes each one, and
ships a detector that turns the silence into a metric and an alert. It doubles
as a code sample for job applications (Java / Spring Boot / Kafka roles) and as
the backing material for system-design interviews.

The thesis: "pipelines don't fail with a stack trace, they fail quietly; the
work is building the checks that make the quiet loud."

## Goals

- One `docker compose up` brings up Kafka, Postgres, Prometheus, Grafana and the app.
- The app runs in two modes, `naive` and `safe`, selected by `pipeline.mode`.
- Three failure scenarios, each reproducible deterministically through fault
  injection, each measurable in numbers:
  1. **Dual-write loss (producer).** `naive`: the payment row is committed, then
     the event is sent; if the send fails it is logged and forgotten.
     `safe`: transactional outbox (payment + outbox row in one DB transaction,
     a relay publishes and marks them sent, retrying until it succeeds).
  2. **Redelivery duplicates (consumer).** `naive`: the ledger write commits,
     then the listener fails before the offset is committed, Kafka redelivers
     and the payment is applied twice. `safe`: idempotent consumer — a
     `processed_events(event_id PK)` insert in the same transaction as the
     ledger write; a redelivered event is a no-op.
  3. **Swallowed poison message (consumer).** `naive`: a payload that does not
     parse or validate is caught, logged and its offset committed — the payment
     vanishes. `safe`: bounded retries, then a dead-letter topic; DLT records
     are persisted and counted, so they are accounted for, not lost.
- A **reconciler** that runs in both modes and compares the source of truth
  (`payments`) with the ledger: missing events, duplicate entries, balance
  drift, dead letters. Exposed as Prometheus gauges, with alert rules and a
  Grafana dashboard that shows consumer lag next to reconciliation — the lag
  panel says "healthy" while the reconciliation panel says "37 missing".
- Integration tests with Testcontainers (real Kafka + Postgres) that assert,
  for each mode, the numbers the README quotes. CI on GitHub Actions.
- A visible AI-assisted workflow: `CLAUDE.md`, Claude Code hooks used as
  guardrails, and this spec → plan → TDD trail in the git history.

## Non-goals

- Kafka transactions / exactly-once semantics: they don't cover the database
  side; the README explains why the outbox + idempotent consumer is used.
- Multiple deployable services, Kubernetes, auth, a UI beyond Grafana.
- Throughput tuning or benchmarks beyond the scenario numbers.

## Anti-goals

- Any reference to an employer or client, or any claim that this is production
  code from a job. It is a reproduction of a class of problems.
- Numbers in the README that did not come from a test run in the repo.
- A toy CRUD with Kafka bolted on: every component exists to show a failure,
  its fix, or its detection.

## Constraints and decisions

- Java 25 (LTS), Spring Boot 4.1, Spring Kafka, JDBC (`JdbcClient`), Flyway,
  Micrometer + Prometheus, Testcontainers 2. Maven with the wrapper.
- One Spring app plays both the upstream payments service and the ledger
  consumer, to keep the demo one container. The boundary is the Kafka topic
  and the two sets of tables; nothing else is shared.
- Domain: `POST /payments` creates a payment (account, amount in cents);
  `POST /simulate?count=N&accounts=M` generates a batch. Event
  `PaymentCreated{eventId, paymentId, accountId, amountCents, createdAt}` as
  JSON on topic `payments`, keyed by account.
- Fault injection (`faults.*` properties) is deterministic: an event is
  selected for a fault by a hash of its id modulo 100 against a rate, so a run
  with the same ids reproduces the same failures. Faults: `producer-send-failure-rate`,
  `consumer-crash-after-write-rate` (fails once per selected event),
  `poison-rate` (producer emits a corrupted payload for that event).
- Reconciliation only looks at payments older than a grace period (default 10 s),
  so in-flight events are not reported as missing.
- Repo language: English (international recruiters read it).

## Left to the builder

Package layout, table names beyond those above, dashboard styling, exact
alert thresholds and timings, how the scenario numbers are printed.
