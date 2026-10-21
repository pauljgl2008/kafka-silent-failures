package dev.paulguevara.silentfailures;

import static org.awaitility.Awaitility.await;

import dev.paulguevara.silentfailures.config.FaultProperties;
import dev.paulguevara.silentfailures.payments.NewPayment;
import dev.paulguevara.silentfailures.payments.PaymentService;
import dev.paulguevara.silentfailures.payments.Simulator;
import dev.paulguevara.silentfailures.reconciliation.ReconciliationCounts;
import dev.paulguevara.silentfailures.reconciliation.Reconciler;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@SpringBootTest(properties = {
        "pipeline.reconciliation-grace=1s",
        "faults.producer-send-failure-rate=3",
        "faults.consumer-crash-after-write-rate=5",
        "faults.poison-rate=2"
})
@Import(TestcontainersConfiguration.class)
abstract class AbstractPipelineIT {

    static final int EVENTS = 1000;
    static final int ACCOUNTS = 50;
    static final long SEED = 42;

    @Autowired
    PaymentService paymentService;

    @Autowired
    Reconciler reconciler;

    @Autowired
    FaultProperties faults;

    @Autowired
    JdbcClient jdbc;

    @BeforeEach
    void cleanTables() {
        // The relay and the listeners keep running between tests, so the lock can be busy.
        await().atMost(Duration.ofSeconds(30))
                .ignoreExceptions()
                .until(() -> {
                    jdbc.sql("TRUNCATE payments, outbox, ledger_entries, processed_events, dead_letters").update();
                    return true;
                });
    }

    List<NewPayment> runScenario() {
        return runScenario(SEED);
    }

    /**
     * Tests in one class share a Spring context, and each fault fires only once per event id.
     * A second run with the same seed would not break anything, so other tests use other seeds.
     */
    List<NewPayment> runScenario(long seed) {
        List<NewPayment> payments = Simulator.plan(seed, EVENTS, ACCOUNTS);
        payments.forEach(paymentService::create);
        return payments;
    }

    /** Waits until the pipeline settles on the expected numbers, then checks they stay there. */
    ReconciliationCounts awaitCounts(ReconciliationCounts expected) {
        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> reconciler.counts().equals(expected));
        await().pollDelay(Duration.ofSeconds(2)).until(() -> true);
        ReconciliationCounts settled = reconciler.counts();
        System.out.println("SCENARIO " + getClass().getSimpleName() + " " + settled);
        return settled;
    }
}
