package dev.paulguevara.silentfailures.payments;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(PaymentController.class)
class PaymentControllerTest {

    @Autowired
    MockMvc mvc;

    @MockitoBean
    PaymentService payments;

    private void postPayment(String body, int expectedStatus) throws Exception {
        mvc.perform(post("/payments").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().is(expectedStatus));
    }

    @Test
    void validPaymentIsCreated() throws Exception {
        when(payments.create(any())).thenReturn(
                new PaymentEvent(UUID.randomUUID(), UUID.randomUUID(), "acc-1", 1250, Instant.now()));
        postPayment("{\"accountId\":\"acc-1\",\"amountCents\":1250}", 201);
    }

    @Test
    void paymentThatTheLedgerWouldRejectIsRejectedHere() throws Exception {
        postPayment("{\"accountId\":\"acc-1\",\"amountCents\":-500}", 400);
        postPayment("{\"accountId\":\"acc-1\",\"amountCents\":0}", 400);
        postPayment("{\"amountCents\":500}", 400);
        postPayment("{\"accountId\":\" \",\"amountCents\":500}", 400);
        verify(payments, never()).create(any());
    }

    @Test
    void simulateRejectsBadNumbers() throws Exception {
        mvc.perform(post("/simulate?count=0")).andExpect(status().isBadRequest());
        mvc.perform(post("/simulate?count=-5")).andExpect(status().isBadRequest());
        mvc.perform(post("/simulate?count=5&accounts=0")).andExpect(status().isBadRequest());
        mvc.perform(post("/simulate?count=1000000")).andExpect(status().isBadRequest());
        verify(payments, never()).create(any());
    }

    @Test
    void simulateWithASeedThatWasAlreadyUsedIsAConflict() throws Exception {
        when(payments.exists(any())).thenReturn(true);
        mvc.perform(post("/simulate?count=5&seed=42")).andExpect(status().isConflict());
        verify(payments, never()).create(any());
    }
}
