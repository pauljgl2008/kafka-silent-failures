-- Upstream side: the payments service. This is the source of truth.
CREATE TABLE payments (
    event_id     UUID PRIMARY KEY,
    payment_id   UUID        NOT NULL,
    account_id   TEXT        NOT NULL,
    amount_cents BIGINT      NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL
);

-- Transactional outbox (safe mode): written in the same transaction as the payment.
CREATE TABLE outbox (
    id         BIGSERIAL PRIMARY KEY,
    event_id   UUID        NOT NULL,
    account_id TEXT        NOT NULL,
    payload    TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at    TIMESTAMPTZ
);
CREATE INDEX outbox_unsent ON outbox (id) WHERE sent_at IS NULL;

-- Downstream side: the ledger. event_id is NOT unique on purpose:
-- in naive mode a redelivered event is applied twice, and the reconciler must see it.
CREATE TABLE ledger_entries (
    id           BIGSERIAL PRIMARY KEY,
    event_id     UUID        NOT NULL,
    account_id   TEXT        NOT NULL,
    amount_cents BIGINT      NOT NULL,
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ledger_entries_event ON ledger_entries (event_id);

-- Idempotent consumer (safe mode): one row per event that was already applied.
CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Messages that could not be processed (safe mode). They are counted, not lost.
CREATE TABLE dead_letters (
    id          BIGSERIAL PRIMARY KEY,
    event_id    UUID,
    payload     TEXT        NOT NULL,
    error       TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX dead_letters_event ON dead_letters (event_id);
