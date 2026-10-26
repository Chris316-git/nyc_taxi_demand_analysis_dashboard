package io.github.chris316.taxidemand.ingest;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import io.github.chris316.taxidemand.config.AppProperties;

/**
 * Applies a batch of trip events to the per-zone demand buckets exactly once.
 *
 * <p>All work happens in one database transaction:
 * <ol>
 *   <li>Insert every trip id into {@code processed_trips} with {@code ON CONFLICT DO NOTHING
 *       RETURNING trip_id}. Only ids that come back are new; everything else is a duplicate.</li>
 *   <li>Aggregate the new trips per (zone, bucket) in memory.</li>
 *   <li>Upsert the aggregates into {@code zone_demand_buckets}, adding to existing counts.</li>
 * </ol>
 *
 * <p>Ids are only kept for a retention window, so events older than the stored dedup
 * horizon are rejected (and counted as late) instead of risking a second count.
 *
 * <p>Why this is safe under concurrency: the primary key on {@code processed_trips} is the
 * arbiter. If two consumers race on the same trip id, Postgres makes the second insert wait
 * for the first transaction; if that commits, the second sees a conflict and skips the trip.
 * Rows are always written in sorted key order (trip ids, then zone/bucket) so concurrent
 * batches acquire row locks in the same order and cannot deadlock each other.
 */
@Component
@Profile("!replayer")
public class TripBatchProcessor {

    private static final String INSERT_PROCESSED = """
            INSERT INTO processed_trips (trip_id, pickup_time)
            SELECT * FROM unnest(?::varchar[], ?::timestamptz[])
            ON CONFLICT (trip_id) DO NOTHING
            RETURNING trip_id
            """;

    private static final String UPSERT_BUCKETS = """
            INSERT INTO zone_demand_buckets (zone_id, bucket_start, trips, fare_total)
            SELECT * FROM unnest(?::int[], ?::timestamptz[], ?::int[], ?::numeric[])
            ON CONFLICT (zone_id, bucket_start) DO UPDATE
               SET trips      = zone_demand_buckets.trips + EXCLUDED.trips,
                   fare_total = zone_demand_buckets.fare_total + EXCLUDED.fare_total
            """;

    private final JdbcTemplate jdbc;
    private final TimeBuckets buckets;
    private final Counter appliedCounter;
    private final Counter duplicateCounter;
    private final Counter lateCounter;

    public TripBatchProcessor(JdbcTemplate jdbc, AppProperties props, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.buckets = new TimeBuckets(props.bucket());
        this.appliedCounter = Counter.builder("trips.applied")
                .description("Trip events counted for the first time").register(registry);
        this.duplicateCounter = Counter.builder("trips.duplicates")
                .description("Trip events skipped as duplicates").register(registry);
        this.lateCounter = Counter.builder("trips.late")
                .description("Trip events rejected for arriving behind the dedup horizon").register(registry);
    }

    /** Processes events without recording partition progress (used by tests and tools). */
    @Transactional
    public BatchResult process(List<TripEvent> events) {
        return process(events, Map.of());
    }

    /**
     * @param partitionProgress newest event time seen per Kafka partition in this batch; stored in
     *                          the same transaction so the retention job can compute a low watermark
     */
    @Transactional
    public BatchResult process(List<TripEvent> events, Map<Integer, Instant> partitionProgress) {
        if (!partitionProgress.isEmpty()) {
            recordProgress(partitionProgress);
        }
        if (events.isEmpty()) {
            return new BatchResult(0, 0, 0, 0);
        }

        // 1. Events older than the dedup horizon can no longer be checked against processed_trips
        //    (their ids may have been purged), so counting them could double count. Reject them.
        //    FOR SHARE blocks the retention job from moving the horizon until this batch commits.
        Instant horizon = jdbc.queryForObject(
                "SELECT horizon FROM dedup_horizon WHERE id = 1 FOR SHARE", Timestamp.class).toInstant();

        // 2. Collapse duplicates inside the batch and sort by id for a stable lock order.
        Map<String, TripEvent> byId = new LinkedHashMap<>();
        int late = 0;
        for (TripEvent e : events) {
            if (e.pickupTime().isBefore(horizon)) {
                late++;
                continue;
            }
            byId.putIfAbsent(e.tripId(), e);
        }
        List<TripEvent> unique = new ArrayList<>(byId.values());
        unique.sort(Comparator.comparing(TripEvent::tripId));

        // 3. Claim the ids; Postgres returns only the ones nobody claimed before.
        Set<String> newIds = unique.isEmpty() ? Set.of() : claimTripIds(unique);

        // 4. Aggregate the genuinely new trips per (zone, bucket), sorted for lock order.
        Map<BucketKey, Aggregate> aggregates = new TreeMap<>();
        for (TripEvent e : unique) {
            if (newIds.contains(e.tripId())) {
                BucketKey key = new BucketKey(e.pickupZoneId(), buckets.floor(e.pickupTime()));
                aggregates.computeIfAbsent(key, k -> new Aggregate()).add(e.fareAmount());
            }
        }

        // 5. Add the aggregates to the running bucket totals.
        if (!aggregates.isEmpty()) {
            upsertBuckets(aggregates);
        }

        int applied = newIds.size();
        int duplicates = events.size() - applied - late;
        appliedCounter.increment(applied);
        duplicateCounter.increment(duplicates);
        lateCounter.increment(late);
        return new BatchResult(events.size(), applied, duplicates, late);
    }

    private void recordProgress(Map<Integer, Instant> partitionProgress) {
        // Sorted by partition so concurrent consumers lock progress rows in the same order.
        new TreeMap<>(partitionProgress).forEach((partition, maxEventTime) -> jdbc.update("""
                INSERT INTO partition_progress (partition_id, max_event_time) VALUES (?, ?)
                ON CONFLICT (partition_id) DO UPDATE
                   SET max_event_time = GREATEST(partition_progress.max_event_time, EXCLUDED.max_event_time)
                """, partition, Timestamp.from(maxEventTime)));
    }

    private Set<String> claimTripIds(List<TripEvent> unique) {
        String[] ids = new String[unique.size()];
        Timestamp[] times = new Timestamp[unique.size()];
        for (int i = 0; i < unique.size(); i++) {
            ids[i] = unique.get(i).tripId();
            times[i] = Timestamp.from(unique.get(i).pickupTime());
        }
        List<String> inserted = jdbc.query(con -> {
            var ps = con.prepareStatement(INSERT_PROCESSED);
            ps.setArray(1, con.createArrayOf("varchar", ids));
            ps.setArray(2, con.createArrayOf("timestamptz", times));
            return ps;
        }, (rs, rowNum) -> rs.getString(1));
        return new HashSet<>(inserted);
    }

    private void upsertBuckets(Map<BucketKey, Aggregate> aggregates) {
        int n = aggregates.size();
        Integer[] zones = new Integer[n];
        Timestamp[] starts = new Timestamp[n];
        Integer[] trips = new Integer[n];
        BigDecimal[] fares = new BigDecimal[n];
        int i = 0;
        for (Map.Entry<BucketKey, Aggregate> entry : aggregates.entrySet()) {
            zones[i] = entry.getKey().zoneId();
            starts[i] = Timestamp.from(entry.getKey().bucketStart());
            trips[i] = entry.getValue().trips;
            fares[i] = entry.getValue().fareTotal;
            i++;
        }
        jdbc.update(con -> {
            var ps = con.prepareStatement(UPSERT_BUCKETS);
            Array a1 = con.createArrayOf("int4", zones);
            Array a2 = con.createArrayOf("timestamptz", starts);
            Array a3 = con.createArrayOf("int4", trips);
            Array a4 = con.createArrayOf("numeric", fares);
            ps.setArray(1, a1);
            ps.setArray(2, a2);
            ps.setArray(3, a3);
            ps.setArray(4, a4);
            return ps;
        });
    }

    record BucketKey(int zoneId, Instant bucketStart) implements Comparable<BucketKey> {
        private static final Comparator<BucketKey> ORDER =
                Comparator.comparingInt(BucketKey::zoneId).thenComparing(BucketKey::bucketStart);

        @Override
        public int compareTo(BucketKey o) {
            return ORDER.compare(this, o);
        }
    }

    private static final class Aggregate {
        int trips;
        BigDecimal fareTotal = BigDecimal.ZERO;

        void add(BigDecimal fare) {
            trips++;
            fareTotal = fareTotal.add(fare);
        }
    }
}
