package dev.paulguevara.silentfailures.reconciliation;

/**
 * What the reconciler found, comparing the payments table (source of truth) with the ledger.
 *
 * @param payments    payments old enough to be checked
 * @param missing     payments with no ledger entry and no dead letter: lost without a trace
 * @param duplicates  ledger entries beyond the first one for the same event
 * @param deadLetters payments that failed and were parked in the dead-letter table
 * @param driftCents  ledger total minus the total of the payments that should be in it
 */
public record ReconciliationCounts(long payments, long missing, long duplicates, long deadLetters, long driftCents) {
}
