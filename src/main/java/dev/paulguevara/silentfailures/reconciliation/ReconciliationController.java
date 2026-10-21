package dev.paulguevara.silentfailures.reconciliation;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReconciliationController {

    private final Reconciler reconciler;

    public ReconciliationController(Reconciler reconciler) {
        this.reconciler = reconciler;
    }

    @GetMapping("/reconciliation")
    public ReconciliationReport report() {
        return ReconciliationReport.of(reconciler.counts());
    }
}
