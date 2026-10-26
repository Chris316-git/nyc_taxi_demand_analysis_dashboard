package io.github.chris316.taxidemand.query;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@Profile("!replayer")
public class DemandRepository {

    private final JdbcTemplate jdbc;

    public DemandRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Total trips per zone for buckets in {@code [from, to)}. Uses idx_zone_demand_bucket_start. */
    public List<ZoneCount> tripsByZone(Instant from, Instant to) {
        return jdbc.query("""
                        SELECT zone_id, SUM(trips) AS trips
                          FROM zone_demand_buckets
                         WHERE bucket_start >= ? AND bucket_start < ?
                         GROUP BY zone_id
                        """,
                (rs, i) -> new ZoneCount(rs.getInt("zone_id"), rs.getLong("trips")),
                Timestamp.from(from), Timestamp.from(to));
    }

    /** Per-bucket trips for one zone in {@code [from, to)}, oldest first. Uses the primary key. */
    public List<BucketCount> zoneSeries(int zoneId, Instant from, Instant to) {
        return jdbc.query("""
                        SELECT bucket_start, trips
                          FROM zone_demand_buckets
                         WHERE zone_id = ? AND bucket_start >= ? AND bucket_start < ?
                         ORDER BY bucket_start
                        """,
                (rs, i) -> new BucketCount(rs.getTimestamp("bucket_start").toInstant(), rs.getLong("trips")),
                zoneId, Timestamp.from(from), Timestamp.from(to));
    }

    /**
     * Start of the newest bucket that has data: the stream's event-time "now".
     * An index-only backward scan, so it stays cheap as the table grows.
     */
    public Optional<Instant> watermark() {
        Timestamp ts = jdbc.queryForObject("SELECT MAX(bucket_start) FROM zone_demand_buckets", Timestamp.class);
        return Optional.ofNullable(ts).map(Timestamp::toInstant);
    }

    public record BucketCount(Instant bucketStart, long trips) {
    }
}
