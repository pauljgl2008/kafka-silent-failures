package dev.paulguevara.silentfailures.payments;

import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PaymentController {

    private final PaymentService payments;

    public PaymentController(PaymentService payments) {
        this.payments = payments;
    }

    record CreatePayment(String accountId, long amountCents) {
    }

    @PostMapping("/payments")
    @ResponseStatus(HttpStatus.CREATED)
    public PaymentEvent create(@RequestBody CreatePayment request) {
        return payments.create(NewPayment.random(request.accountId(), request.amountCents()));
    }

    @PostMapping("/simulate")
    public Map<String, Object> simulate(@RequestParam(defaultValue = "1000") int count,
                                        @RequestParam(defaultValue = "50") int accounts,
                                        @RequestParam(defaultValue = "42") long seed) {
        Simulator.plan(seed, count, accounts).forEach(payments::create);
        return Map.of("created", count, "accounts", accounts, "seed", seed);
    }
}
