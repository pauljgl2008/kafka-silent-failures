package dev.paulguevara.silentfailures.config;

import dev.paulguevara.silentfailures.payments.InvalidEventException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
public class KafkaConfig {

    public static final String TOPIC = "payments";
    public static final String DEAD_LETTER_TOPIC = "payments.DLT";
    public static final String EVENT_ID_HEADER = "event-id";

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    NewTopic paymentsTopic() {
        return TopicBuilder.name(TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic deadLetterTopic() {
        return TopicBuilder.name(DEAD_LETTER_TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    CommonErrorHandler errorHandler(PipelineProperties pipeline, KafkaTemplate<String, String> kafka) {
        if (pipeline.naive()) {
            // Retry twice, then log and move on. The record is gone.
            DefaultErrorHandler handler = new DefaultErrorHandler(
                    (record, e) -> log.warn("Giving up on record at offset {}: {}", record.offset(), e.getMessage()),
                    new FixedBackOff(0, 2));
            return handler;
        }
        // Retry a few times; if it still fails, park it in the dead-letter topic.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafka, (record, e) -> new TopicPartition(DEAD_LETTER_TOPIC, -1));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(100, 3));
        // Retrying a message that can never be parsed only adds lag.
        handler.addNotRetryableExceptions(InvalidEventException.class);
        return handler;
    }
}
