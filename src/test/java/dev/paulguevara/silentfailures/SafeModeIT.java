package dev.paulguevara.silentfailures;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.paulguevara.silentfailures.payments.NewPayment;
import dev.paulguevara.silentfailures.reconciliation.ReconciliationCounts;
import dev.paulguevara.silentfailures.reconciliation.ReconciliationReport;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = "pipeline.mode=safe")
class SafeModeIT extends AbstractPipelineIT {

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Test
    void safePipelineKeepsEveryPaymentAndParksPoisonMessages() {
        List<NewPayment> payments = runScenario();
        ReconciliationCounts expected = ScenarioExpectations.safe(payments, faults);

        ReconciliationCounts counts = awaitCounts(expected);

        assertThat(counts).isEqualTo(expected);
        assertThat(counts.deadLetters()).isPositive();
        assertThat(ReconciliationReport.of(counts).healthy()).isTrue();
    }

    @Test
    void messageThatIsNotEvenJsonIsStillLinkedToItsPayment() throws Exception {
        UUID eventId = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO payments (event_id, payment_id, account_id, amount_cents, created_at)
                VALUES (?, ?, 'acc-1', 500, ?)
                """)
                .params(eventId, UUID.randomUUID(), Timestamp.from(Instant.now().minusSeconds(60)))
                .update();
        ProducerRecord<String, String> garbage = new ProducerRecord<>("payments", "acc-1", "}{ not json");
        garbage.headers().add("event-id", eventId.toString().getBytes(StandardCharsets.UTF_8));
        kafka.send(garbage).get();

        await().atMost(Duration.ofSeconds(30))
                .until(() -> reconciler.counts().deadLetters() == 1);
        assertThat(reconciler.counts().missing()).isZero();
    }

    @Test
    void paymentsStillInFlightAreNotReportedAsMissing() {
        List<NewPayment> payments = runScenario();
        ReconciliationCounts counts = reconciler.counts(Duration.ofMinutes(10));
        assertThat(counts.payments()).isZero();
        assertThat(counts.missing()).isZero();

        // Let the run finish so it does not leak into the next test.
        awaitCounts(ScenarioExpectations.safe(payments, faults));
    }
}
