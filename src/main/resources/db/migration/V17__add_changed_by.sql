ALTER TABLE analytics.trade_bloc
    ADD COLUMN changed_by_id VARCHAR(255),
    ADD COLUMN changed_by_email VARCHAR(255),
    ADD COLUMN changed_by_name VARCHAR(255);

ALTER TABLE analytics.country
    ADD COLUMN changed_by_id VARCHAR(255),
    ADD COLUMN changed_by_email VARCHAR(255),
    ADD COLUMN changed_by_name VARCHAR(255);

ALTER TABLE analytics.category
    ADD COLUMN changed_by_id VARCHAR(255),
    ADD COLUMN changed_by_email VARCHAR(255),
    ADD COLUMN changed_by_name VARCHAR(255);
