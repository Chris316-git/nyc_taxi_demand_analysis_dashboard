package io.github.chris316.taxidemand.ingest;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import io.github.chris316.taxidemand.config.AppProperties;

/**
 * Keeps the dedup table bounded without breaking exactly-once counting.
 *
 * <p>The horizon is based on the <em>low watermark</em>: the newest event time of the slowest
 * Kafka partition. Partitions progress at different speeds (especially during a fast replay),
 * and basing the horizon on the fastest one would reject first-time events that a lagging
 * partition has not delivered yet. Until every partition has reported progress, nothing is purged.
 *
 * <p>In one transaction the job first advances {@code dedup_horizon} to
 * (low watermark - retention), then deletes ids older than it. The UPDATE needs an exclusive
 * lock on the horizon row, so it waits for every in-flight batch (which hold FOR SHARE locks)
 * to commit, and every later batch sees the new horizon and rejects events behind it.
 * Without the horizon, replaying old data after a purge would count those trips twice.
 */
@Component
@Profile("!replayer")
public class DedupRetentionJob {

    private static final Logger log = LoggerFactory.getLogger(DedupRetentionJob.class);

    private final JdbcTemplate jdbc;
    private final AppProperties props;
    private final TransactionTemplate tx;

    public DedupRetentionJob(JdbcTemplate jdbc, AppProperties props, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.props = props;
        this.tx = tx;
    }

    @Scheduled(fixedDelayString = "${app.dedup-cleanup-interval:PT1H}", initialDelayString = "PT1M")
    public void scheduledPurge() {
        purgeExpiredIds();
    }

    /** @return number of ids deleted */
    public int purgeExpiredIds() {
        return lowWatermark().map(watermark -> {
            Instant cutoff = watermark.minus(props.dedupRetention());
            Integer deleted = tx.execute(status -> {
                jdbc.update("UPDATE dedup_horizon SET horizon = GREATEST(horizon, ?) WHERE id = 1",
                        Timestamp.from(cutoff));
                return jdbc.update(
                        "DELETE FROM processed_trips WHERE pickup_time < (SELECT horizon FROM dedup_horizon WHERE id = 1)");
            });
            if (deleted != null && deleted > 0) {
                log.info("Dedup horizon advanced to {}; purged {} ids", cutoff, deleted);
            }
            return deleted == null ? 0 : deleted;
        }).orElse(0);
    }

    /** Newest event time of the slowest partition, once all partitions have reported. */
    Optional<Instant> lowWatermark() {
        Timestamp ts = jdbc.queryForObject(
                "SELECT CASE WHEN COUNT(*) >= ? THEN MIN(max_event_time) END FROM partition_progress",
                Timestamp.class, props.partitions());
        return Optional.ofNullable(ts).map(Timestamp::toInstant);
    }
}
