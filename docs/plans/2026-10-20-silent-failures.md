# Kafka silent failures — implementation plan

> Executed natively, task by task, TDD. Steps use checkbox syntax.

**Goal:** a runnable repo that reproduces dual-write loss, redelivery duplicates and swallowed poison messages in a `naive` mode, fixes them in a `safe` mode, and detects them in both with a reconciler exposed to Prometheus/Grafana.

**Architecture:** one Spring Boot app with two halves that only share the Kafka topic: `payments` (upstream, source of truth, outbox) and `ledger` (consumer). A scheduled reconciler compares both sets of tables. Fault injection is deterministic per event id and fault name.

**Tech stack:** Java 25, Spring Boot 4.1 (webmvc, kafka, jdbc, flyway, actuator), Micrometer Prometheus, Postgres 16, Kafka (KRaft), Testcontainers 2, Awaitility.

**Spec:** `docs/specs/2026-10-20-silent-failures-design.md`

## Global constraints

- Package root `dev.paulguevara.silentfailures`. Repo language English.
- `pipeline.mode` = `naive` | `safe`. Topic `payments`, DLT `payments.DLT`, record key = account id, header `event-id` on every record.
- Faults (percent, 0–100): `faults.producer-send-failure-rate`, `faults.consumer-crash-after-write-rate`, `faults.poison-rate`. Default 0.
- Every number in the README comes from a test run.
- No attribution lines in commits.

## Review focus

1. Poison payload that is not even JSON → still dead-lettered (safe) and still linked to its payment through the `event-id` header.
2. An event that is both poison-selected and crash-selected → poison wins (fails before any write); expected counts must model that.
3. Reconciler during in-flight traffic → payments younger than the grace period are never reported missing.
4. Outbox relay with a send that fails once → row stays unsent and is published on the next poll, exactly once in the ledger.
5. Missing and duplicate entries cancelling out in the balance drift → counts are reported separately so drift 0 does not mean healthy.

---

### Task 1: Scaffold + pure building blocks

**Files:** `pom.xml`, Maven wrapper, `SilentFailuresApplication.java`, `application.yml`, `db/migration/V1__schema.sql`, `faults/FaultInjector.java`, `payments/PaymentEvent.java`, `payments/PaymentEventCodec.java`, `payments/InvalidEventException.java`; tests `FaultInjectorTest`, `PaymentEventCodecTest`.

**Interfaces (produced):**
- `record PaymentEvent(UUID eventId, UUID paymentId, String accountId, long amountCents, Instant createdAt)`
- `PaymentEventCodec.encode(PaymentEvent) -> String`, `decode(String) -> PaymentEvent` throws `InvalidEventException` on unparseable JSON, missing fields, or `amountCents <= 0`; `corrupt(PaymentEvent) -> String` (JSON with `"amountCents":"NaN"`).
- `FaultInjector.selected(String fault, UUID eventId, int ratePercent) -> boolean` (static, pure: `floorMod(Objects.hash(fault, eventId), 100) < rate`); instance `failOnce(String fault, UUID eventId, int rate) -> boolean` (true only the first time for a selected event).
- Schema: `payments(event_id uuid pk, payment_id uuid, account_id, amount_cents, created_at)`, `outbox(id bigserial, event_id, payload, created_at, sent_at)`, `ledger_entries(id bigserial, event_id — not unique, account_id, amount_cents, applied_at)`, `processed_events(event_id pk)`, `dead_letters(id, event_id null, payload, error, received_at)`.

- [ ] Tests: rate 0 selects none and 100 selects all of 1000 ids; rate 10 over 10 000 seeded ids selects 900–1100; same id + different fault names are independent; `failOnce` true then false. Codec round-trip; `decode(corrupt(e))`, `decode("not json")`, negative amount → `InvalidEventException`.
- [ ] Run `./mvnw -q test` → red; implement; green; commit.

### Task 2: Reconciliation logic (pure)

**Files:** `reconciliation/ReconciliationCounts.java`, `reconciliation/ReconciliationReport.java`; test `ReconciliationReportTest`.

**Interfaces:** `record ReconciliationCounts(long payments, long missing, long duplicates, long deadLetters, long driftCents)`; `ReconciliationReport.of(counts)` → `healthy()` (missing = 0 and duplicates = 0 and drift = 0) and `findings()` (human sentences; dead letters are a finding but not unhealthy).

- [ ] Tests: all zero → healthy, no findings; missing 3 + duplicates 3 with drift 0 → unhealthy with two findings (review focus 5); dead letters only → healthy with one finding.
- [ ] Red → implement → green → commit.

### Task 3: Pipeline end to end (producer, consumer, reconciler)

**Files:** `config/PipelineProperties`, `config/FaultProperties`, `config/KafkaConfig`, `payments/PaymentService`, `payments/OutboxRelay`, `payments/PaymentController`, `payments/Simulator`, `ledger/LedgerListener`, `ledger/DeadLetterListener`, `reconciliation/Reconciler`, `reconciliation/ReconciliationController`; tests `AbstractPipelineIT`, `NaiveModeIT`, `SafeModeIT`, `ScenarioExpectations`.

**Interfaces:**
- `POST /payments {accountId, amountCents}`; `POST /simulate?count&accounts&seed` (event ids from a seeded `Random`, so a run is reproducible); `GET /reconciliation` → `ReconciliationReport` JSON.
- `Simulator.eventIds(long seed, int count) -> List<UUID>` (static, shared with the tests to compute expected counts).
- `Reconciler.counts() -> ReconciliationCounts` over payments older than `pipeline.reconciliation-grace` (default 10s), refreshed every `pipeline.reconciliation-interval` (5s) into gauges `reconciliation.missing.events`, `reconciliation.duplicate.entries`, `reconciliation.dead.letters`, `reconciliation.balance.drift.cents`.
- `ScenarioExpectations.naive(ids, faults)` / `safe(ids, faults)` → expected `ReconciliationCounts`, built only from `FaultInjector.selected` (poison wins over crash; naive: missing = send-failed + poison, duplicates = crash-selected among the rest; safe: deadLetters = poison, rest 0).

Behaviour per mode: naive producer commits the payment then sends, catching and logging a failed send; safe producer writes payment + outbox in one transaction and `OutboxRelay` (every 200 ms, `FOR UPDATE SKIP LOCKED`) sends synchronously and marks `sent_at`, leaving the row unsent on failure. Naive listener catches `InvalidEventException` and returns; safe listener rethrows it, not retryable, so `DeadLetterPublishingRecoverer` sends it to `payments.DLT`, where `DeadLetterListener` stores it with the `event-id` header. Safe listener inserts `processed_events ON CONFLICT DO NOTHING` and skips if it was already there, in the same transaction as the ledger insert. Both throw after commit when `failOnce("consumer-crash", …)` fires; `DefaultErrorHandler` redelivers.

- [ ] Write both ITs (Testcontainers Kafka + Postgres via `@ServiceConnection`, grace 1 s, faults 5/5/2, 1000 events, seed 42): await `GET /reconciliation` counts == expectations; naive must be unhealthy, safe healthy with dead letters = poison count. Plus `SafeModeIT.notJsonPoisonIsDeadLettered` (review focus 1) and `inFlightPaymentsAreNotMissing` with the default grace (review focus 3).
- [ ] Red → implement → green (`./mvnw verify`) → commit. Print the scenario numbers to the log for the README.

### Task 4: Ops — compose, Prometheus, alerts, Grafana

**Files:** `Dockerfile`, `docker-compose.yml`, `ops/prometheus/prometheus.yml`, `ops/prometheus/alerts.yml`, `ops/grafana/provisioning/**`, `ops/grafana/dashboards/pipeline.json`, `scripts/run-scenario.sh`.

- Alerts: `LedgerMissingEvents` (> 0 for 1m), `LedgerDuplicateEntries` (> 0 for 1m), `DeadLettersPending` (> 0, warning). Dashboard: consumer lag, events published, the four reconciliation gauges.
- [ ] `docker compose up -d --build`, run the script in each mode, check `/actuator/prometheus` has the gauges and Prometheus shows the alerts firing in naive; screenshot the dashboard for the README; commit.

### Task 5: Harness + CI

**Files:** `CLAUDE.md`, `.claude/settings.json`, `.claude/hooks/guard.mjs`, `.claude/hooks/guard.test.mjs`, `.github/workflows/ci.yml`.

- Hook (PreToolUse on Edit/Write/Bash): blocks editing a Flyway migration already committed (`git ls-files` match on `db/migration/V*.sql`), and `git push --force` / `--no-verify`.
- [ ] Tests with `node --test`: committed migration blocked, new migration allowed, force push blocked, normal push allowed. CI: JDK 25 `./mvnw verify` + `node --test .claude/hooks`. Commit.

### Task 6: README

- [ ] Problem → three scenarios with the measured numbers → how detection works → run it → design notes (why not Kafka transactions, why drift alone lies, Spring Boot 3 → 4 changes met). Commit.
