DROP INDEX IF EXISTS analytics.rate_source_id_idx;

ALTER TABLE analytics.rate 
ALTER COLUMN event_id TYPE VARCHAR USING event_id::VARCHAR;

ALTER TABLE analytics.rate 
DROP COLUMN source_id,
DROP COLUMN lower_bound_inclusive,
DROP COLUMN upper_bound_exclusive;

CREATE UNIQUE INDEX rate_source_symbolic_code_destination_symbolic_code_event_time_idx
    ON analytics.rate (source_symbolic_code, destination_symbolic_code, event_time);
