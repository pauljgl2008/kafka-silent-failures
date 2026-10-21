package dev.paulguevara.silentfailures.payments;

import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/** Turns events into JSON and back, and rejects messages that are not valid. */
@Component
public class PaymentEventCodec {

    private final JsonMapper json;

    public PaymentEventCodec(JsonMapper json) {
        this.json = json;
    }

    public String encode(PaymentEvent event) {
        return json.writeValueAsString(event);
    }

    public PaymentEvent decode(String payload) {
        PaymentEvent event;
        try {
            event = json.readValue(payload, PaymentEvent.class);
        } catch (JacksonException e) {
            throw new InvalidEventException("payload is not a valid PaymentEvent", e);
        }
        if (event == null || event.eventId() == null || event.paymentId() == null
                || event.accountId() == null || event.createdAt() == null) {
            throw new InvalidEventException("payload has missing fields");
        }
        if (event.amountCents() <= 0) {
            throw new InvalidEventException("amountCents must be positive, got " + event.amountCents());
        }
        return event;
    }

    /** A payload that looks like the real one but can never be parsed: a "poison message". */
    public String corrupt(PaymentEvent event) {
        return encode(event).replaceFirst("\"amountCents\":-?\\d+", "\"amountCents\":\"NaN\"");
    }
}
