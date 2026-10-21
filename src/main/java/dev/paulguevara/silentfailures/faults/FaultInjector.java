package dev.paulguevara.silentfailures.faults;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * Decides which events get a fault. The choice depends only on the fault name
 * and the event id, so the same run always breaks the same events.
 */
@Component
public class FaultInjector {

    public static final String PRODUCER_SEND = "producer-send";
    public static final String CONSUMER_CRASH = "consumer-crash";
    public static final String POISON = "poison";

    private final Set<String> alreadyFailed = ConcurrentHashMap.newKeySet();

    public static boolean selected(String fault, UUID eventId, int ratePercent) {
        if (ratePercent <= 0) {
            return false;
        }
        // MD5 spreads sequential ids well; this is not about security.
        UUID hash = UUID.nameUUIDFromBytes((fault + ":" + eventId).getBytes(StandardCharsets.UTF_8));
        return Math.floorMod(hash.getLeastSignificantBits(), 100) < ratePercent;
    }

    /** True the first time a selected event asks, false after that. */
    public boolean failOnce(String fault, UUID eventId, int ratePercent) {
        return selected(fault, eventId, ratePercent) && alreadyFailed.add(fault + ":" + eventId);
    }
}
