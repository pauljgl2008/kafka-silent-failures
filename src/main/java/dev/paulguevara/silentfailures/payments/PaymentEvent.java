package dev.paulguevara.silentfailures.payments;

import java.time.Instant;
import java.util.UUID;

/** The message on the {@code payments} topic. */
public record PaymentEvent(UUID eventId, UUID paymentId, String accountId, long amountCents, Instant createdAt) {
}
