package dev.paulguevara.silentfailures.reconciliation;

import dev.paulguevara.silentfailures.config.PipelineProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Compares the payments table (source of truth) with the ledger and publishes the result as
 * gauges. Consumer lag can only tell you that messages are waiting; this tells you whether the
 * money actually arrived. It runs the same way in both modes.
 */
@Component
public class Reconciler {

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
                        WHERE c.event_id NOT IN (SELECT event_id FROM parked)) AS drift_cents
            """;

    private final JdbcClient jdbc;
    private final PipelineProperties pipeline;
    private final AtomicReference<ReconciliationCounts> latest =
            new AtomicReference<>(new ReconciliationCounts(0, 0, 0, 0, 0));

    public Reconciler(JdbcClient jdbc, PipelineProperties pipeline, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.pipeline = pipeline;
        gauge(meters, "reconciliation.missing.events", "Payments that never reached the ledger",
                ReconciliationCounts::missing);
        gauge(meters, "reconciliation.duplicate.entries", "Ledger entries applied more than once",
                ReconciliationCounts::duplicates);
        gauge(meters, "reconciliation.dead.letters", "Payments parked in the dead-letter table",
                ReconciliationCounts::deadLetters);
        gauge(meters, "reconciliation.balance.drift.cents", "Ledger total minus expected total",
                ReconciliationCounts::driftCents);
        gauge(meters, "reconciliation.checked.payments", "Payments old enough to be checked",
                ReconciliationCounts::payments);
    }

    public ReconciliationCounts counts() {
        return counts(pipeline.reconciliationGrace());
    }

    public ReconciliationCounts counts(Duration grace) {
        return jdbc.sql(COUNTS_SQL)
                .param("cutoff", Timestamp.from(Instant.now().minus(grace)))
                .query((rs, row) -> new ReconciliationCounts(
                        rs.getLong("payments"),
                        rs.getLong("missing"),
                        rs.getLong("duplicates"),
                        rs.getLong("dead_letters"),
                        rs.getLong("drift_cents")))
                .single();
    }

    @Scheduled(fixedDelayString = "${pipeline.reconciliation-interval}")
    public void refresh() {
        latest.set(counts());
    }

    private void gauge(MeterRegistry meters, String name, String description,
                       ToLongFunction<ReconciliationCounts> value) {
        Gauge.builder(name, latest, ref -> value.applyAsLong(ref.get()))
                .description(description)
                .register(meters);
    }
}
