-- clickhouse/init.sql
CREATE DATABASE IF NOT EXISTS space_events;

CREATE TABLE IF NOT EXISTS space_events.raw_events (
    event_id UInt32,
    name String,
    description String,
    location String,
    event_date DateTime,
    type String,
    raw_data String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (event_date, event_id);