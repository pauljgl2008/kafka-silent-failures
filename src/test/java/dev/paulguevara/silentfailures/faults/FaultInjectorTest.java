package dev.paulguevara.silentfailures.faults;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class FaultInjectorTest {

    private static List<UUID> ids(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> new UUID(42L, i))
                .toList();
    }

    @Test
    void rateZeroSelectsNoEvent() {
        assertThat(ids(1000)).noneMatch(id -> FaultInjector.selected("poison", id, 0));
    }

    @Test
    void rateHundredSelectsEveryEvent() {
        assertThat(ids(1000)).allMatch(id -> FaultInjector.selected("poison", id, 100));
    }

    @Test
    void rateTenSelectsAboutTenPercent() {
        long selected = ids(10_000).stream()
                .filter(id -> FaultInjector.selected("poison", id, 10))
                .count();
        assertThat(selected).isBetween(900L, 1100L);
    }

    @Test
    void differentFaultsPickDifferentEvents() {
        List<UUID> ids = ids(10_000);
        List<UUID> poison = ids.stream().filter(id -> FaultInjector.selected("poison", id, 10)).toList();
        List<UUID> crash = ids.stream().filter(id -> FaultInjector.selected("consumer-crash", id, 10)).toList();
        assertThat(poison).isNotEqualTo(crash);
        long both = poison.stream().filter(crash::contains).count();
        assertThat(both).isLessThan(poison.size() / 4);
    }

    @Test
    void sameInputAlwaysGivesSameAnswer() {
        UUID id = UUID.randomUUID();
        boolean first = FaultInjector.selected("poison", id, 50);
        assertThat(IntStream.range(0, 100)).allMatch(i -> FaultInjector.selected("poison", id, 50) == first);
    }

    @Test
    void failOnceFiresOnlyTheFirstTime() {
        FaultInjector faults = new FaultInjector();
        UUID id = UUID.randomUUID();
        assertThat(faults.failOnce("consumer-crash", id, 100)).isTrue();
        assertThat(faults.failOnce("consumer-crash", id, 100)).isFalse();
    }

    @Test
    void failOnceNeverFiresForEventsThatAreNotSelected() {
        FaultInjector faults = new FaultInjector();
        assertThat(faults.failOnce("consumer-crash", UUID.randomUUID(), 0)).isFalse();
    }
}
