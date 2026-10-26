-- Newest event time seen per Kafka partition, written in the same transaction as the counts.
-- MIN over partitions is the low watermark: no partition will still deliver events much older
-- than it, so ids behind (low watermark - retention) are safe to purge.
CREATE TABLE partition_progress (
    partition_id   INT PRIMARY KEY,
    max_event_time TIMESTAMPTZ NOT NULL
);
