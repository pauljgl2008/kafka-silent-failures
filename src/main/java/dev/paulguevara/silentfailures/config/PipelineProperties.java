package dev.paulguevara.silentfailures.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("pipeline")
public record PipelineProperties(
        Mode mode,
        Duration reconciliationGrace,
        Duration reconciliationInterval,
        Duration outboxPollInterval,
        Duration retryGiveUpAfter) {

    public enum Mode {
        /** The common first version: dual write, no idempotency, errors logged and skipped. */
        NAIVE,
        /** Transactional outbox, idempotent consumer, dead-letter topic. */
        SAFE
    }

    public boolean naive() {
        return mode == Mode.NAIVE;
    }
}
