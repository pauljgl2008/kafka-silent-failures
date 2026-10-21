package dev.paulguevara.silentfailures.reconciliation;

import java.util.ArrayList;
import java.util.List;

/**
 * The counts plus a verdict. Missing events and duplicates are checked one by one,
 * because they can cancel each other out in the balance.
 */
public record ReconciliationReport(ReconciliationCounts counts, boolean healthy, List<String> findings) {

    public static ReconciliationReport of(ReconciliationCounts counts) {
        List<String> findings = new ArrayList<>();
        if (counts.missing() > 0) {
            findings.add(counts.missing() + " payments never reached the ledger");
        }
        if (counts.duplicates() > 0) {
            findings.add(counts.duplicates() + " ledger entries are duplicates");
        }
        if (counts.driftCents() != 0) {
            findings.add("ledger total is off by " + counts.driftCents() + " cents");
        }
        boolean healthy = findings.isEmpty();
        // A dead letter is visible and can be replayed, so it is not a silent failure.
        if (counts.deadLetters() > 0) {
            findings.add(counts.deadLetters() + " payments are in the dead-letter table");
        }
        if (counts.unlinkedDeadLetters() > 0) {
            findings.add(counts.unlinkedDeadLetters()
                    + " dead letters have no event id and cannot be matched to a payment");
        }
        return new ReconciliationReport(counts, healthy, List.copyOf(findings));
    }
}
