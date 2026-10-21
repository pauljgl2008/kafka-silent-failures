# Review fixes — design

A code review of the first version found problems. Each one below was checked against the
running stack or the code before it was accepted.

## Confirmed in a live run

1. **Dead-letter loop.** The safe-mode error handler is the default for every listener,
   including the dead-letter listener. When saving a dead letter fails, the record is
   published to the dead-letter topic again. Measured: with the `dead_letters` table
   renamed, one bad payment made the DLT grow from 39 to 55 records in 20 seconds.
   A record with a null value would loop forever.
   **Fix:** the dead-letter listener gets its own container factory. Its error handler never
   republishes; it waits and retries (exponential backoff, no limit). If saving keeps failing,
   the DLT consumer lag grows, which is visible. A null value is stored as an empty payload.

2. **The detector can fail silently.** While the table was renamed, the reconciler query failed
   9 times and the gauges kept their old values. Nothing showed that the data was old.
   **Fix:** a gauge `reconciliation.last.success.age.seconds` and a counter
   `reconciliation.failures`, plus an alert `ReconcilerStale` (age > 60 s).

3. **The API creates poison messages.** `POST /payments` accepts `amountCents <= 0` (201, then
   the consumer rejects it) and returns 500 for a missing account. `POST /simulate` returns 500
   for `accounts=0`, a negative count, or a seed that was already used (the README example did
   exactly that).
   **Fix:** validate input and return 400. A seed that was already used returns 409 before
   anything is written.

4. **`scripts/run-scenario.sh` was not executable** in git. **Fix:** mode 100755.

5. **The guard hook missed common forms:** `git -C . push --force`, `git push origin +main`,
   `git push -fu`, `git commit -n`, and editing a migration from Bash (`sed -i` and similar).
   **Fix:** wider patterns, with a test for each form.

## Confirmed by reading the code (not reproduced)

6. **Short retry window in the main listener.** `FixedBackOff(100, 3)` gives up after ~300 ms,
   so a database blip longer than that would dead-letter valid payments. (A Postgres restart
   under load did not trigger it in my run, but the window is clearly too short.)
   **Fix:** exponential backoff (200 ms → 5 s, up to 2 minutes) for errors that can recover.
   Invalid messages still go to the DLT at once.

7. **Outbox relay can block and reorder.** `send().get()` has no timeout, the relay shares one
   scheduler thread with the reconciler, and a failed row is skipped while later rows for the
   same account are sent.
   **Fix:** a timeout on each send, a scheduler pool of 2 threads, and the batch stops at the
   first failure so the order is kept. Failed sends are counted (`outbox.send.failures`).

8. **Dead letters without an event id were not counted.** **Fix:** a separate count
   `unlinkedDeadLetters`, reported as a finding and included in the dead-letter alert.

9. **Tests reused seed 42 in one Spring context,** so the one-time faults had already fired and
   the second run did not exercise them. **Fix:** each test uses its own seed.

## Not changed

- `FaultInjector` keeps one id per fired fault in memory. It is test tooling and is bounded by the
  number of events in a demo run.
