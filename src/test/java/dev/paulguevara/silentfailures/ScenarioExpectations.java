package dev.paulguevara.silentfailures;

import static dev.paulguevara.silentfailures.faults.FaultInjector.CONSUMER_CRASH;
import static dev.paulguevara.silentfailures.faults.FaultInjector.POISON;
import static dev.paulguevara.silentfailures.faults.FaultInjector.PRODUCER_SEND;
import static dev.paulguevara.silentfailures.faults.FaultInjector.selected;

import dev.paulguevara.silentfailures.config.FaultProperties;
import dev.paulguevara.silentfailures.payments.NewPayment;
import dev.paulguevara.silentfailures.reconciliation.ReconciliationCounts;
import java.util.List;

/**
 * What the reconciler must find after a simulated run. It is computed from the
 * fault rules only, without touching Kafka or the database, so the tests check
 * the real pipeline against an independent answer.
 */
final class ScenarioExpectations {

    private ScenarioExpectations() {
    }

    static ReconciliationCounts naive(List<NewPayment> payments, FaultProperties faults) {
        long missing = 0;
        long duplicates = 0;
        long drift = 0;
        for (NewPayment p : payments) {
            boolean sendFails = selected(PRODUCER_SEND, p.eventId(), faults.producerSendFailureRate());
            boolean poison = selected(POISON, p.eventId(), faults.poisonRate());
            boolean crash = selected(CONSUMER_CRASH, p.eventId(), faults.consumerCrashAfterWriteRate());
            if (sendFails || poison) {
                // Never sent, or sent and then swallowed by the consumer.
                missing++;
                drift -= p.amountCents();
            } else if (crash) {
                // Written, then redelivered and written again.
                duplicates++;
                drift += p.amountCents();
            }
        }
        return new ReconciliationCounts(payments.size(), missing, duplicates, 0, drift, 0);
    }

    static ReconciliationCounts safe(List<NewPayment> payments, FaultProperties faults) {
        long deadLetters = payments.stream()
                .filter(p -> selected(POISON, p.eventId(), faults.poisonRate()))
                .count();
        return new ReconciliationCounts(payments.size(), 0, 0, deadLetters, 0, 0);
    }
}
