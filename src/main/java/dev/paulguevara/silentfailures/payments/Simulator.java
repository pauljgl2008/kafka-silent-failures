package dev.paulguevara.silentfailures.payments;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/** Builds a batch of payments from a seed. The same seed always gives the same batch. */
public final class Simulator {

    private static final long MIN_AMOUNT_CENTS = 100;
    private static final int AMOUNT_RANGE_CENTS = 100_000;

    private Simulator() {
    }

    public static List<NewPayment> plan(long seed, int count, int accounts) {
        Random random = new Random(seed);
        List<NewPayment> payments = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            payments.add(new NewPayment(
                    new UUID(random.nextLong(), random.nextLong()),
                    new UUID(random.nextLong(), random.nextLong()),
                    "acc-" + random.nextInt(accounts),
                    MIN_AMOUNT_CENTS + random.nextInt(AMOUNT_RANGE_CENTS)));
        }
        return payments;
    }
}
