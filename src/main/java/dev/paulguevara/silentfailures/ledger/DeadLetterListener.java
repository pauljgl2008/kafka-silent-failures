package dev.paulguevara.silentfailures.ledger;

import static dev.paulguevara.silentfailures.config.KafkaConfig.DEAD_LETTER_CONTAINER_FACTORY;
import static dev.paulguevara.silentfailures.config.KafkaConfig.DEAD_LETTER_TOPIC;
import static dev.paulguevara.silentfailures.config.KafkaConfig.EVENT_ID_HEADER;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/**
 * Stores every dead-lettered message with its event id, taken from the header and not from
 * the payload: a broken payload may not even be JSON, but it can still be traced back.
 */
@Component
public class DeadLetterListener {

    private final JdbcClient jdbc;

    public DeadLetterListener(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @KafkaListener(topics = DEAD_LETTER_TOPIC, groupId = "ledger-dead-letters",
            containerFactory = DEAD_LETTER_CONTAINER_FACTORY)
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        // A record with no value (a tombstone) is still a dead letter, not a reason to fail.
        String payload = record.value() == null ? "" : record.value();
        jdbc.sql("INSERT INTO dead_letters (event_id, payload, error) VALUES (?, ?, ?)")
                .params(eventId(record), payload, header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE))
                .update();
    }

    private static UUID eventId(ConsumerRecord<String, String> record) {
        String value = header(record, EVENT_ID_HEADER);
        try {
            return value == null ? null : UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
