package dev.paulguevara.silentfailures.reconciliation;

import dev.paulguevara.silentfailures.config.PipelineProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Compares the payments table (source of truth) with the ledger and publishes the result as
 * gauges. Consumer lag can only tell you that messages are waiting; this tells you whether the
 * money actually arrived. It runs the same way in both modes.
 *
 * <p>The detector can fail too. If the query stops working, the gauges would keep showing the
 * last good numbers, so it also publishes how old those numbers are.
 */
@Component
public class Reconciler {

    private static final Logger log = LoggerFactory.getLogger(Reconciler.class);

    // Only payments older than the grace period are checked, so in-flight events are not "missing".
    private static final String COUNTS_SQL = """
            WITH checked AS (
                SELECT event_id, amount_cents FROM payments WHERE created_at < :cutoff
            ),
            entries AS (
                SELECT event_id, count(*) AS copies, sum(amount_cents) AS total
                FROM ledger_entries
                WHERE event_id IN (SELECT event_id FROM checked)
                GROUP BY event_id
            ),
            parked AS (
                SELECT DISTINCT event_id FROM dead_letters
                WHERE event_id IN (SELECT event_id FROM checked)
            )
            SELECT
                (SELECT count(*) FROM checked) AS payments,
                (SELECT count(*) FROM checked c
                    WHERE c.event_id NOT IN (SELECT event_id FROM entries)
                      AND c.event_id NOT IN (SELECT event_id FROM parked)) AS missing,
                (SELECT coalesce(sum(copies - 1), 0) FROM entries) AS duplicates,
                (SELECT count(*) FROM parked) AS dead_letters,
                (SELECT coalesce(sum(total), 0) FROM entries)
                  - (SELECT coalesce(sum(amount_cents), 0) FROM checked c
                        WHERE c.event_id NOT IN (SELECT event_id FROM parked)) AS drift_cents,
                (SELECT count(*) FROM dead_letters d
                    WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.event_id = d.event_id)) AS unlinked
            """;

    private final JdbcClient jdbc;
    private final PipelineProperties pipeline;
    private final Clock clock;
    private final Counter failures;
    private final AtomicReference<ReconciliationCounts> latest =
            new AtomicReference<>(new ReconciliationCounts(0, 0, 0, 0, 0, 0));
    private final AtomicReference<Instant> lastSuccess;

    public Reconciler(JdbcClient jdbc, PipelineProperties pipeline, MeterRegistry meters, Clock clock) {
        this.jdbc = jdbc;
        this.pipeline = pipeline;
        this.clock = clock;
        this.lastSuccess = new AtomicReference<>(clock.instant());
        gauge(meters, "reconciliation.missing.events", "Payments that never reached the ledger",
                ReconciliationCounts::missing);
        gauge(meters, "reconciliation.duplicate.entries", "Ledger entries applied more than once",
                ReconciliationCounts::duplicates);
        gauge(meters, "reconciliation.dead.letters", "Payments parked in the dead-letter table",
                ReconciliationCounts::deadLetters);
        gauge(meters, "reconciliation.unlinked.dead.letters", "Dead letters that match no payment",
                ReconciliationCounts::unlinkedDeadLetters);
        gauge(meters, "reconciliation.balance.drift.cents", "Ledger total minus expected total",
                ReconciliationCounts::driftCents);
        gauge(meters, "reconciliation.checked.payments", "Payments old enough to be checked",
                ReconciliationCounts::payments);
        Gauge.builder("reconciliation.last.success.age.seconds", this, r -> r.secondsSinceLastSuccess())
                .description("Seconds since the reconciler last ran without errors")
                .register(meters);
        this.failures = Counter.builder("reconciliation.failures")
                .description("Reconciliation runs that failed")
                .register(meters);
    }

    public ReconciliationCounts counts() {
        return counts(pipeline.reconciliationGrace());
    }

    public ReconciliationCounts counts(Duration grace) {
        return jdbc.sql(COUNTS_SQL)
                .param("cutoff", Timestamp.from(clock.instant().minus(grace)))
                .query((rs, row) -> new ReconciliationCounts(
                        rs.getLong("payments"),
                        rs.getLong("missing"),
                        rs.getLong("duplicates"),
                        rs.getLong("dead_letters"),
                        rs.getLong("drift_cents"),
                        rs.getLong("unlinked")))
                .single();
    }

    @Scheduled(fixedDelayString = "${pipeline.reconciliation-interval}")
    public void refresh() {
        try {
            latest.set(counts());
            lastSuccess.set(clock.instant());
        } catch (RuntimeException e) {
            failures.increment();
            log.error("Reconciliation failed, the gauges keep the last good numbers: {}", e.getMessage());
        }
    }

    double secondsSinceLastSuccess() {
        return Duration.between(lastSuccess.get(), clock.instant()).toMillis() / 1000.0;
    }

    private void gauge(MeterRegistry meters, String name, String description,
                       ToLongFunction<ReconciliationCounts> value) {
        Gauge.builder(name, latest, ref -> value.applyAsLong(ref.get()))
                .description(description)
                .register(meters);
    }
}
