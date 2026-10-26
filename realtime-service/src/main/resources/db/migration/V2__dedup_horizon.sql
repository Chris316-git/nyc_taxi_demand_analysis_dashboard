-- Events older than this instant are rejected instead of counted: their trip ids may already
-- have been purged from processed_trips, so a duplicate could no longer be detected.
-- Single row; the retention job advances it before deleting old ids.
CREATE TABLE dedup_horizon (
    id      SMALLINT PRIMARY KEY CHECK (id = 1),
    horizon TIMESTAMPTZ NOT NULL
);

INSERT INTO dedup_horizon (id, horizon) VALUES (1, '1970-01-01T00:00:00Z');
