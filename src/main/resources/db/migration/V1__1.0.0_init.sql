CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE an.balance_change
(
    id        BIGSERIAL                   NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    party_id  CHARACTER VARYING           NOT NULL,
    shop_id   CHARACTER VARYING           NOT NULL,
    amount    BIGINT                      NOT NULL,
    currency  CHARACTER VARYING           NOT NULL,
    CONSTRAINT balance_change_pkey PRIMARY KEY (id)
);

CREATE INDEX balance_change_timestamp on an.balance_change (timestamp);
CREATE INDEX balance_change_party_id on an.balance_change (party_id);
CREATE INDEX balance_change_party_id on an.balance_change (shop_id);
