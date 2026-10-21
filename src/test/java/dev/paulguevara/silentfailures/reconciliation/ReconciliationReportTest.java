package dev.paulguevara.silentfailures.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ReconciliationReportTest {

    @Test
    void allZeroIsHealthyWithNoFindings() {
        ReconciliationReport report = ReconciliationReport.of(new ReconciliationCounts(100, 0, 0, 0, 0));
        assertThat(report.healthy()).isTrue();
        assertThat(report.findings()).isEmpty();
    }

    @Test
    void missingAndDuplicatesAreUnhealthyEvenWhenTheBalanceMatches() {
        // 3 lost payments and 3 applied twice can cancel out in the totals.
        ReconciliationReport report = ReconciliationReport.of(new ReconciliationCounts(100, 3, 3, 0, 0));
        assertThat(report.healthy()).isFalse();
        assertThat(report.findings())
                .containsExactly(
                        "3 payments never reached the ledger",
                        "3 ledger entries are duplicates");
    }

    @Test
    void balanceDriftAloneIsUnhealthy() {
        ReconciliationReport report = ReconciliationReport.of(new ReconciliationCounts(100, 0, 0, 0, -1250));
        assertThat(report.healthy()).isFalse();
        assertThat(report.findings()).containsExactly("ledger total is off by -1250 cents");
    }

    @Test
    void deadLettersAreReportedButStillHealthy() {
        ReconciliationReport report = ReconciliationReport.of(new ReconciliationCounts(100, 0, 0, 2, 0));
        assertThat(report.healthy()).isTrue();
        assertThat(report.findings()).containsExactly("2 payments are in the dead-letter table");
    }
}
