package dev.paulguevara.silentfailures.payments;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class PaymentEventCodecTest {

    private final PaymentEventCodec codec = new PaymentEventCodec(JsonMapper.builder().build());

    private final PaymentEvent event = new PaymentEvent(
            UUID.randomUUID(), UUID.randomUUID(), "acc-7", 1250, Instant.parse("2026-10-20T10:15:30Z"));

    @Test
    void encodeAndDecodeGiveBackTheSameEvent() {
        assertThat(codec.decode(codec.encode(event))).isEqualTo(event);
    }

    @Test
    void corruptPayloadIsRejected() {
        assertThatThrownBy(() -> codec.decode(codec.corrupt(event)))
                .isInstanceOf(InvalidEventException.class);
    }

    @Test
    void textThatIsNotJsonIsRejected() {
        assertThatThrownBy(() -> codec.decode("not json"))
                .isInstanceOf(InvalidEventException.class);
    }

    @Test
    void missingFieldIsRejected() {
        assertThatThrownBy(() -> codec.decode("{\"eventId\":\"" + event.eventId() + "\"}"))
                .isInstanceOf(InvalidEventException.class);
    }

    @Test
    void amountBelowOneCentIsRejected() {
        PaymentEvent negative = new PaymentEvent(
                event.eventId(), event.paymentId(), event.accountId(), -5, event.createdAt());
        assertThatThrownBy(() -> codec.decode(codec.encode(negative)))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("amountCents");
    }
}
