-- Every trip id that has been counted. The primary key is what makes ingestion idempotent:
-- a redelivered event hits ON CONFLICT DO NOTHING and is never counted twice.
CREATE TABLE processed_trips (
    trip_id     VARCHAR(32) PRIMARY KEY,
    pickup_time TIMESTAMPTZ NOT NULL
);

-- Supports the retention job that keeps this table bounded.
CREATE INDEX idx_processed_trips_pickup_time ON processed_trips (pickup_time);

-- Pre-aggregated demand: one row per pickup zone per 5-minute bucket.
-- A sliding window is a SUM over the buckets it covers.
CREATE TABLE zone_demand_buckets (
    zone_id      INT            NOT NULL,
    bucket_start TIMESTAMPTZ    NOT NULL,
    trips        INT            NOT NULL CHECK (trips >= 0),
    fare_total   NUMERIC(14, 2) NOT NULL DEFAULT 0,
    PRIMARY KEY (zone_id, bucket_start)
);

-- Window scans across all zones ("top zones in the last hour") and the watermark lookup.
CREATE INDEX idx_zone_demand_bucket_start ON zone_demand_buckets (bucket_start);
