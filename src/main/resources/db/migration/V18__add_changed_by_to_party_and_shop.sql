ALTER TABLE analytics.party
    ADD COLUMN version_id BIGINT,
    ADD COLUMN changed_by_id VARCHAR(255),
    ADD COLUMN changed_by_email VARCHAR(255),
    ADD COLUMN changed_by_name VARCHAR(255),
    ADD COLUMN deleted BOOLEAN;

ALTER TABLE analytics.shop
    ADD COLUMN version_id BIGINT,
    ADD COLUMN changed_by_id VARCHAR(255),
    ADD COLUMN changed_by_email VARCHAR(255),
    ADD COLUMN changed_by_name VARCHAR(255),
    ADD COLUMN deleted BOOLEAN;