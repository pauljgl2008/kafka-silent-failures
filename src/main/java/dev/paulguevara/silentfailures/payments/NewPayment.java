package dev.paulguevara.silentfailures.payments;

import java.util.UUID;

/** A payment to create. Ids come from the caller, so a simulated run can be repeated. */
public record NewPayment(UUID eventId, UUID paymentId, String accountId, long amountCents) {

    public static NewPayment random(String accountId, long amountCents) {
        return new NewPayment(UUID.randomUUID(), UUID.randomUUID(), accountId, amountCents);
    }
}
