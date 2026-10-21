package dev.paulguevara.silentfailures.ledger;

import static dev.paulguevara.silentfailures.config.KafkaConfig.TOPIC;

import dev.paulguevara.silentfailures.config.FaultProperties;
import dev.paulguevara.silentfailures.config.PipelineProperties;
import dev.paulguevara.silentfailures.faults.FaultInjector;
import dev.paulguevara.silentfailures.payments.InvalidEventException;
import dev.paulguevara.silentfailures.payments.PaymentEvent;
import dev.paulguevara.silentfailures.payments.PaymentEventCodec;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/** The downstream ledger: it reads payment events and books them. */
@Component
public class LedgerListener {

    private static final Logger log = LoggerFactory.getLogger(LedgerListener.class);

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;
    private final PaymentEventCodec codec;
    private final PipelineProperties pipeline;
    private final FaultInjector faultInjector;
    private final FaultProperties faults;
    private final Counter applied;

    public LedgerListener(JdbcClient jdbc, TransactionTemplate tx, PaymentEventCodec codec,
                          PipelineProperties pipeline, FaultInjector faultInjector, FaultProperties faults,
                          MeterRegistry meters) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.codec = codec;
        this.pipeline = pipeline;
        this.faultInjector = faultInjector;
        this.faults = faults;
        this.applied = Counter.builder("pipeline.ledger.applied")
                .description("Ledger entries written")
                .register(meters);
    }

    @KafkaListener(topics = TOPIC)
    public void onPayment(String payload) {
        if (pipeline.naive()) {
            onPaymentNaive(payload);
        } else {
            onPaymentSafe(payload);
        }
    }

    /**
     * Naive: a bad message is logged and skipped (its offset is committed, so it is gone),
     * and a redelivered message is booked again.
     */
    private void onPaymentNaive(String payload) {
        PaymentEvent event;
        try {
            event = codec.decode(payload);
        } catch (InvalidEventException e) {
            log.warn("Skipping bad message: {}", e.getMessage());
            return;
        }
        tx.executeWithoutResult(status -> insertEntry(event));
        crashAfterCommitIfSelected(event);
    }

    /**
     * Safe: a bad message is thrown to the error handler, which sends it to the dead-letter topic.
     * The processed_events row is written in the same transaction as the entry, so a
     * redelivered message finds it and does nothing.
     */
    private void onPaymentSafe(String payload) {
        PaymentEvent event = codec.decode(payload);
        tx.executeWithoutResult(status -> {
            int firstTime = jdbc.sql("INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT DO NOTHING")
                    .param(event.eventId())
                    .update();
            if (firstTime == 1) {
                insertEntry(event);
            } else {
                log.debug("Event {} already applied, skipping", event.eventId());
            }
        });
        crashAfterCommitIfSelected(event);
    }

    /**
     * The database commit is done but the offset is not. Kafka will deliver the message again,
     * which is exactly what happens when a consumer dies or the group rebalances at this point.
     */
    private void crashAfterCommitIfSelected(PaymentEvent event) {
        if (faultInjector.failOnce(FaultInjector.CONSUMER_CRASH, event.eventId(), faults.consumerCrashAfterWriteRate())) {
            throw new IllegalStateException("simulated crash after the ledger write");
        }
    }

    private void insertEntry(PaymentEvent event) {
        jdbc.sql("INSERT INTO ledger_entries (event_id, account_id, amount_cents) VALUES (?, ?, ?)")
                .params(event.eventId(), event.accountId(), event.amountCents())
                .update();
        applied.increment();
    }
}
