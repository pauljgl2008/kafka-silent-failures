package dev.paulguevara.silentfailures.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** How often each fault happens, in percent of events (0 to 100). */
@ConfigurationProperties("faults")
public record FaultProperties(
        int producerSendFailureRate,
        int consumerCrashAfterWriteRate,
        int poisonRate) {
}
