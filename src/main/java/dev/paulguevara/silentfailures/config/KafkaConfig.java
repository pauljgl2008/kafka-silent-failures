package dev.paulguevara.silentfailures.config;

import dev.paulguevara.silentfailures.payments.InvalidEventException;
import java.time.Duration;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
public class KafkaConfig {

    public static final String TOPIC = "payments";
    public static final String DEAD_LETTER_TOPIC = "payments.DLT";
    public static final String EVENT_ID_HEADER = "event-id";
    public static final String DEAD_LETTER_CONTAINER_FACTORY = "deadLetterContainerFactory";

    // A database or broker blip should be waited out, not dead-lettered.
    private static final Duration RETRY_FIRST_WAIT = Duration.ofMillis(200);
    private static final Duration RETRY_MAX_WAIT = Duration.ofSeconds(5);
    private static final Duration DEAD_LETTER_RETRY_WAIT = Duration.ofSeconds(1);

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    NewTopic paymentsTopic() {
        return TopicBuilder.name(TOPIC).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic deadLetterTopic() {
        return TopicBuilder.name(DEAD_LETTER_TOPIC).partitions(3).replicas(1).build();
    }

    /** Error handler for the payments listener. Spring Boot puts it on the default container factory. */
    @Bean
    CommonErrorHandler errorHandler(PipelineProperties pipeline, KafkaTemplate<String, String> kafka) {
        if (pipeline.naive()) {
            // Retry twice, then log and move on. The record is gone.
            return new DefaultErrorHandler(
                    (record, e) -> log.warn("Giving up on record at offset {}: {}", record.offset(), e.getMessage()),
                    new FixedBackOff(0, 2));
        }
        // Retry with growing waits; if it still fails, park it in the dead-letter topic.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafka, (record, e) -> new TopicPartition(DEAD_LETTER_TOPIC, -1));
        ExponentialBackOff backOff = new ExponentialBackOff(RETRY_FIRST_WAIT.toMillis(), 2.0);
        backOff.setMaxInterval(RETRY_MAX_WAIT.toMillis());
        backOff.setMaxElapsedTime(pipeline.retryGiveUpAfter().toMillis());
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        // Retrying a message that can never be parsed only adds lag.
        handler.addNotRetryableExceptions(InvalidEventException.class);
        return handler;
    }

    /**
     * The dead-letter listener must never send a record back to the dead-letter topic: if saving
     * fails, that would loop forever. It retries until it works. While it can't, the DLT
     * consumer lag grows, and that is visible.
     */
    @Bean(DEAD_LETTER_CONTAINER_FACTORY)
    ConcurrentKafkaListenerContainerFactory<Object, Object> deadLetterContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                new FixedBackOff(DEAD_LETTER_RETRY_WAIT.toMillis(), FixedBackOff.UNLIMITED_ATTEMPTS)));
        return factory;
    }
}
