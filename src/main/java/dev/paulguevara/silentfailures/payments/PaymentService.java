package dev.paulguevara.silentfailures.payments;

import static dev.paulguevara.silentfailures.config.KafkaConfig.EVENT_ID_HEADER;
import static dev.paulguevara.silentfailures.config.KafkaConfig.TOPIC;

import dev.paulguevara.silentfailures.config.FaultProperties;
import dev.paulguevara.silentfailures.config.PipelineProperties;
import dev.paulguevara.silentfailures.faults.FaultInjector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/** The upstream payments service: it saves the payment and tells the rest of the system. */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;
    private final KafkaTemplate<String, String> kafka;
    private final PaymentEventCodec codec;
    private final PipelineProperties pipeline;
    private final FaultProperties faults;
    private final Counter published;

    public PaymentService(JdbcClient jdbc, TransactionTemplate tx, KafkaTemplate<String, String> kafka,
                          PaymentEventCodec codec, PipelineProperties pipeline, FaultProperties faults,
                          MeterRegistry meters) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.kafka = kafka;
        this.codec = codec;
        this.pipeline = pipeline;
        this.faults = faults;
        this.published = Counter.builder("pipeline.events.published")
                .description("Events sent to the payments topic")
                .register(meters);
    }

    public PaymentEvent create(NewPayment payment) {
        PaymentEvent event = new PaymentEvent(
                payment.eventId(), payment.paymentId(), payment.accountId(), payment.amountCents(), Instant.now());
        String payload = payloadFor(event);
        if (pipeline.naive()) {
            createNaive(event, payload);
        } else {
            createSafe(event, payload);
        }
        return event;
    }

    /**
     * Dual write: the payment is committed first and the event is sent after.
     * If the send fails, the payment exists but nobody downstream will ever hear about it.
     */
    private void createNaive(PaymentEvent event, String payload) {
        insertPayment(event);
        try {
            if (FaultInjector.selected(FaultInjector.PRODUCER_SEND, event.eventId(), faults.producerSendFailureRate())) {
                throw new IllegalStateException("simulated broker timeout");
            }
            send(event.eventId().toString(), event.accountId(), payload);
        } catch (Exception e) {
            log.warn("Could not publish payment {}: {}", event.paymentId(), e.getMessage());
        }
    }

    /** Transactional outbox: the payment and the event are saved together, or not at all. */
    private void createSafe(PaymentEvent event, String payload) {
        tx.executeWithoutResult(status -> {
            insertPayment(event);
            jdbc.sql("INSERT INTO outbox (event_id, account_id, payload) VALUES (?, ?, ?)")
                    .params(event.eventId(), event.accountId(), payload)
                    .update();
        });
    }

    /** Sends one record and waits for the broker to confirm it. Used by both modes. */
    void send(String eventId, String accountId, String payload) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, accountId, payload);
        record.headers().add(EVENT_ID_HEADER, eventId.getBytes(StandardCharsets.UTF_8));
        kafka.send(record).get();
        published.increment();
    }

    private String payloadFor(PaymentEvent event) {
        // A bug upstream (here: a fault) can produce a message no consumer can read.
        return FaultInjector.selected(FaultInjector.POISON, event.eventId(), faults.poisonRate())
                ? codec.corrupt(event)
                : codec.encode(event);
    }

    private void insertPayment(PaymentEvent event) {
        jdbc.sql("""
                INSERT INTO payments (event_id, payment_id, account_id, amount_cents, created_at)
                VALUES (?, ?, ?, ?, ?)
                """)
                .params(event.eventId(), event.paymentId(), event.accountId(), event.amountCents(),
                        Timestamp.from(event.createdAt()))
                .update();
    }
}
