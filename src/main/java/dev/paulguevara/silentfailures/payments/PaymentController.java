package dev.paulguevara.silentfailures.payments;

import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class PaymentController {

    private static final int MAX_SIMULATED_PAYMENTS = 100_000;
    private static final int MIN_SIMULATED_PAYMENTS = 1;
    private static final int MIN_ACCOUNTS = 1;
    // Annotation values must be string constants.
    private static final String DEFAULT_COUNT = "1000";
    private static final String DEFAULT_ACCOUNTS = "50";
    private static final String DEFAULT_SEED = "42";

    private final PaymentService payments;

    public PaymentController(PaymentService payments) {
        this.payments = payments;
    }

    record CreatePayment(String accountId, long amountCents) {
    }

    /**
     * Rejects here what the ledger would reject later. Accepting it would save a payment
     * and then turn its event into a poison message.
     */
    @PostMapping("/payments")
    @ResponseStatus(HttpStatus.CREATED)
    public PaymentEvent create(@RequestBody CreatePayment request) {
        if (request.accountId() == null || request.accountId().isBlank()) {
            throw badRequest("accountId is required");
        }
        if (request.amountCents() <= 0) {
            throw badRequest("amountCents must be positive");
        }
        return payments.create(NewPayment.random(request.accountId(), request.amountCents()));
    }

    @PostMapping("/simulate")
    public Map<String, Object> simulate(@RequestParam(defaultValue = DEFAULT_COUNT) int count,
                                        @RequestParam(defaultValue = DEFAULT_ACCOUNTS) int accounts,
                                        @RequestParam(defaultValue = DEFAULT_SEED) long seed) {
        if (count < MIN_SIMULATED_PAYMENTS || count > MAX_SIMULATED_PAYMENTS) {
            throw badRequest("count must be between " + MIN_SIMULATED_PAYMENTS + " and " + MAX_SIMULATED_PAYMENTS);
        }
        if (accounts < MIN_ACCOUNTS) {
            throw badRequest("accounts must be at least " + MIN_ACCOUNTS);
        }
        List<NewPayment> plan = Simulator.plan(seed, count, accounts);
        // The same seed gives the same ids. Check before writing, so a batch is never half done.
        if (payments.exists(plan.getFirst().eventId())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "seed " + seed + " was already used, pick another one");
        }
        plan.forEach(payments::create);
        return Map.of("created", count, "accounts", accounts, "seed", seed);
    }

    private static ResponseStatusException badRequest(String reason) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, reason);
    }
}
