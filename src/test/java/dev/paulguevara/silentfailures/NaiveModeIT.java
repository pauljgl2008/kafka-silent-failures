package dev.paulguevara.silentfailures;

import static org.assertj.core.api.Assertions.assertThat;

import dev.paulguevara.silentfailures.payments.NewPayment;
import dev.paulguevara.silentfailures.reconciliation.ReconciliationCounts;
import dev.paulguevara.silentfailures.reconciliation.ReconciliationReport;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = "pipeline.mode=naive")
class NaiveModeIT extends AbstractPipelineIT {

    @Test
    void naivePipelineLosesAndDuplicatesPaymentsAndTheReconcilerSeesIt() {
        List<NewPayment> payments = runScenario();
        ReconciliationCounts expected = ScenarioExpectations.naive(payments, faults);

        ReconciliationCounts counts = awaitCounts(expected);

        assertThat(counts).isEqualTo(expected);
        assertThat(counts.missing()).isPositive();
        assertThat(counts.duplicates()).isPositive();
        assertThat(ReconciliationReport.of(counts).healthy()).isFalse();
    }
}
