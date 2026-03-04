CREATE TABLE analytics.withdrawal_state (
    withdrawal_id          VARCHAR                     NOT NULL,
    party_id               VARCHAR                     NOT NULL,
    wallet_id              VARCHAR,
    destination_id         VARCHAR,
    currency               VARCHAR,
    requested_amount       BIGINT,
    amount                 BIGINT,
    system_fee             BIGINT,
    provider_fee           BIGINT,
    external_fee           BIGINT,
    withdrawal_created_at  TIMESTAMP WITHOUT TIME ZONE,
    provider_id            VARCHAR,
    terminal               VARCHAR,
    last_sequence_id       BIGINT                      NOT NULL,
    updated_at             TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT withdrawal_state_pkey PRIMARY KEY (withdrawal_id)
);
