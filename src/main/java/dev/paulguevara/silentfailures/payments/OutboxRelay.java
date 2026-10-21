package dev.paulguevara.silentfailures.payments;

import dev.paulguevara.silentfailures.config.FaultProperties;
import dev.paulguevara.silentfailures.faults.FaultInjector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Safe mode only. Reads unsent outbox rows and publishes them. A row is marked as sent only
 * after the broker confirms it, so a failed send is simply tried again on the next poll.
 * If the app dies between the send and the update, the event is sent twice: that is fine,
 * because the consumer is idempotent.
 */
@Component
@ConditionalOnProperty(name = "pipeline.mode", havingValue = "safe", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH_SIZE = 500;

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;
    private final PaymentService payments;
    private final FaultInjector faultInjector;
    private final FaultProperties faults;

    private final Counter sendFailures;

    public OutboxRelay(JdbcClient jdbc, TransactionTemplate tx, PaymentService payments,
                       FaultInjector faultInjector, FaultProperties faults, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.payments = payments;
        this.faultInjector = faultInjector;
        this.faults = faults;
        this.sendFailures = Counter.builder("outbox.send.failures")
                .description("Outbox sends that failed and will be retried")
                .register(meters);
    }

    record OutboxRow(long id, UUID eventId, String accountId, String payload) {
    }

    @Scheduled(fixedDelayString = "${pipeline.outbox-poll-interval}")
    public void publishPending() {
        tx.executeWithoutResult(status -> {
            List<OutboxRow> rows = jdbc.sql("""
                    SELECT id, event_id, account_id, payload FROM outbox
                    WHERE sent_at IS NULL ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED
                    """)
                    .param(BATCH_SIZE)
                    .query(OutboxRow.class)
                    .list();
            for (OutboxRow row : rows) {
                try {
                    if (faultInjector.failOnce(FaultInjector.PRODUCER_SEND, row.eventId(), faults.producerSendFailureRate())) {
                        throw new IllegalStateException("simulated broker timeout");
                    }
                    payments.send(row.eventId().toString(), row.accountId(), row.payload());
                    jdbc.sql("UPDATE outbox SET sent_at = now() WHERE id = ?").param(row.id()).update();
                } catch (Exception e) {
                    // Stop here: sending the next rows would put later events for the same
                    // account ahead of this one. The whole batch is tried again on the next poll.
                    sendFailures.increment();
                    log.warn("Outbox row {} not sent, will retry: {}", row.id(), e.getMessage());
                    return;
                }
            }
        });
    }
}
