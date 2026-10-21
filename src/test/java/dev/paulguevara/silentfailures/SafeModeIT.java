package dev.paulguevara.silentfailures;

import static dev.paulguevara.silentfailures.config.KafkaConfig.DEAD_LETTER_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.paulguevara.silentfailures.payments.NewPayment;
import dev.paulguevara.silentfailures.reconciliation.ReconciliationCounts;
import dev.paulguevara.silentfailures.reconciliation.ReconciliationReport;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;

// A short give-up time makes a dead-letter loop show up within the test, if one comes back.
@TestPropertySource(properties = {"pipeline.mode=safe", "pipeline.retry-give-up-after=1s"})
class SafeModeIT extends AbstractPipelineIT {

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    KafkaAdmin kafkaAdmin;

    @Autowired
    MeterRegistry meters;

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
        UUID eventId = insertOldPayment();
        sendGarbage(eventId);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> reconciler.counts().deadLetters() == 1);
        assertThat(reconciler.counts().missing()).isZero();
        assertThat(reconciler.counts().unlinkedDeadLetters()).isZero();
    }

    @Test
    void deadLetterWithoutEventIdIsCountedAsUnlinked() throws Exception {
        sendGarbage(null);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> reconciler.counts().unlinkedDeadLetters() == 1);
        assertThat(ReconciliationReport.of(reconciler.counts()).findings())
                .anyMatch(f -> f.contains("no event id"));
    }

    @Test
    void deadLetterThatCannotBeSavedIsRetriedNotRepublished() throws Exception {
        long before = deadLetterTopicSize();
        jdbc.sql("ALTER TABLE dead_letters RENAME TO dead_letters_off").update();
        try {
            UUID eventId = insertOldPayment();
            sendGarbage(eventId);
            await().atMost(Duration.ofSeconds(30)).until(() -> deadLetterTopicSize() == before + 1);

            // Before the fix, every failed save published the record to the DLT again.
            await().pollDelay(Duration.ofSeconds(5)).until(() -> true);
            assertThat(deadLetterTopicSize()).isEqualTo(before + 1);
        } finally {
            jdbc.sql("ALTER TABLE dead_letters_off RENAME TO dead_letters").update();
        }
        await().atMost(Duration.ofSeconds(30))
                .until(() -> jdbc.sql("SELECT count(*) FROM dead_letters").query(Long.class).single() == 1);
    }

    @Test
    void reconcilerFailureIsVisibleInTheMetrics() {
        double failuresBefore = meters.get("reconciliation.failures").counter().count();
        jdbc.sql("ALTER TABLE ledger_entries RENAME TO ledger_entries_off").update();
        try {
            reconciler.refresh();
        } finally {
            jdbc.sql("ALTER TABLE ledger_entries_off RENAME TO ledger_entries").update();
        }
        assertThat(meters.get("reconciliation.failures").counter().count()).isEqualTo(failuresBefore + 1);

        reconciler.refresh();
        assertThat(meters.get("reconciliation.last.success.age.seconds").gauge().value()).isLessThan(1.0);
    }

    @Test
    void paymentsStillInFlightAreNotReportedAsMissing() {
        List<NewPayment> payments = runScenario(SEED + 1);
        ReconciliationCounts counts = reconciler.counts(Duration.ofMinutes(10));
        assertThat(counts.payments()).isZero();
        assertThat(counts.missing()).isZero();

        // Let the run finish so it does not leak into the next test.
        awaitCounts(ScenarioExpectations.safe(payments, faults));
    }

    private UUID insertOldPayment() {
        UUID eventId = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO payments (event_id, payment_id, account_id, amount_cents, created_at)
                VALUES (?, ?, 'acc-1', 500, ?)
                """)
                .params(eventId, UUID.randomUUID(), Timestamp.from(Instant.now().minusSeconds(60)))
                .update();
        return eventId;
    }

    private void sendGarbage(UUID eventId) throws Exception {
        ProducerRecord<String, String> garbage = new ProducerRecord<>("payments", "acc-1", "}{ not json");
        if (eventId != null) {
            garbage.headers().add("event-id", eventId.toString().getBytes(StandardCharsets.UTF_8));
        }
        kafka.send(garbage).get();
    }

    private long deadLetterTopicSize() throws Exception {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Map<TopicPartition, OffsetSpec> request = admin.describeTopics(List.of(DEAD_LETTER_TOPIC))
                    .allTopicNames().get().get(DEAD_LETTER_TOPIC).partitions().stream()
                    .collect(Collectors.toMap(
                            p -> new TopicPartition(DEAD_LETTER_TOPIC, p.partition()), p -> OffsetSpec.latest()));
            return admin.listOffsets(request).all().get().values().stream()
                    .mapToLong(info -> info.offset())
                    .sum();
        }
    }
}
