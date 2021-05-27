--contractor--
ALTER TABLE analytics.contractor
    ADD COLUMN international_legal_entity_country_code character varying;

--shop--
ALTER TABLE analytics.shop
    ADD COLUMN international_legal_entity_country_code character varying;

--trade_bloc--
CREATE TABLE analytics.trade_bloc
(
    id            BIGSERIAL         NOT NULL,
    version_id    BIGINT            NOT NULL,
    trade_bloc_id CHARACTER VARYING NOT NULL,
    name          CHARACTER VARYING NOT NULL,
    description   CHARACTER VARYING NOT NULL,
    deleted       BOOLEAN DEFAULT false,
    CONSTRAINT trade_bloc_pkey PRIMARY KEY (id)
);

--country--
CREATE TABLE analytics.country
(
    id         BIGSERIAL         NOT NULL,
    version_id BIGINT            NOT NULL,
    country_id CHARACTER VARYING NOT NULL,
    name       CHARACTER VARYING NOT NULL,
    trade_bloc TEXT[]            NOT NULL,
    deleted    BOOLEAN DEFAULT false,
    CONSTRAINT country_pkey PRIMARY KEY (id)
);
