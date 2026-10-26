package io.github.chris316.taxidemand.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.github.chris316.taxidemand.config.AppProperties;
import io.github.chris316.taxidemand.support.IntegrationTest;
import io.github.chris316.taxidemand.support.TestTrips;

/**
 * Regression tests for two bugs found by replaying a full month of TLC data (3.3M trips) twice:
 *
 * <ol>
 *   <li>The retention job purged old trip ids, the second replay re-sent those trips, and ~109K
 *       of them were counted again. Fixed with a dedup horizon: events behind it are rejected.</li>
 *   <li>The first horizon was derived from the newest event across all partitions. During a fast
 *       replay some partitions ran days ahead of others, so the horizon overtook the slow ones and
 *       ~538K first-time trips were wrongly rejected. Fixed by using the slowest partition
 *       (low watermark).</li>
 * </ol>
 */
class DedupRetentionIT extends IntegrationTest {

    private static final Instant FEB_15 = Instant.parse("2025-02-15T12:00:00Z");

    @Autowired
    private TripBatchProcessor processor;

    @Autowired
    private DedupRetentionJob retention;

    @Autowired
    private AppProperties props;

    /** Reports every partition at {@code t}, except the overrides. */
    private Map<Integer, Instant> allPartitionsAt(Instant t, Map<Integer, Instant> overrides) {
        Map<Integer, Instant> progress = new HashMap<>();
        for (int p = 0; p < props.partitions(); p++) {
            progress.put(p, overrides.getOrDefault(p, t));
        }
        return progress;
    }

    private static List<TripEvent> tripsAt(String prefix, int n, int zone, Instant t) {
        List<TripEvent> trips = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            trips.add(new TripEvent(prefix + "-" + i, zone, 1, t.plusSeconds(i), BigDecimal.ONE));
        }
        return trips;
    }

    @Test
    void replayingTripsWhoseIdsWerePurgedDoesNotDoubleCount() {
        List<TripEvent> january = TestTrips.generate(1_000, 6); // 2025-01-15
        processor.process(january);
        // Every partition moves a month ahead, so January falls out of the 7-day retention window.
        processor.process(tripsAt("feb", 10, 161, FEB_15), allPartitionsAt(FEB_15, Map.of()));

        assertThat(retention.purgeExpiredIds()).isEqualTo(1_000);

        // A backfill re-sends January. Those ids are gone, so dedup alone would count them again.
        BatchResult replay = processor.process(january);

        assertThat(replay.applied()).isZero();
        assertThat(replay.late()).isEqualTo(1_000);
        assertThat(totalTrips()).isEqualTo(1_010);
    }

    @Test
    void aLaggingPartitionHoldsBackTheHorizon() {
        Instant jan15 = Instant.parse("2025-01-15T12:00:00Z");
        processor.process(tripsAt("early", 50, 48, jan15));
        // Eleven partitions are at Feb 15, but partition 11 is still on Jan 15.
        processor.process(tripsAt("fast", 10, 161, FEB_15), allPartitionsAt(FEB_15, Map.of(11, jan15)));

        retention.purgeExpiredIds();

        // The slow partition now delivers more January trips: they are new and must be counted.
        BatchResult slow = processor.process(tripsAt("slow", 40, 132, jan15.plusSeconds(3600)),
                Map.of(11, jan15.plusSeconds(3700)));
        assertThat(slow.late()).isZero();
        assertThat(slow.applied()).isEqualTo(40);
        assertThat(totalTrips()).isEqualTo(100);
    }

    @Test
    void nothingIsPurgedUntilEveryPartitionHasReported() {
        processor.process(tripsAt("early", 20, 48, Instant.parse("2025-01-15T12:00:00Z")));
        processor.process(tripsAt("one", 5, 161, FEB_15), Map.of(0, FEB_15)); // 1 of 12 partitions

        assertThat(retention.purgeExpiredIds()).isZero();
        assertThat(processedIds()).isEqualTo(25);
    }

    @Test
    void eventsInsideTheRetentionWindowAreStillDeduplicatedAfterAPurge() {
        List<TripEvent> recent = tripsAt("recent", 100, 48, FEB_15.minus(Duration.ofDays(1)));
        processor.process(recent);
        processor.process(tripsAt("now", 1, 48, FEB_15), allPartitionsAt(FEB_15, Map.of()));

        retention.purgeExpiredIds();
        BatchResult again = processor.process(recent);

        assertThat(again.duplicates()).isEqualTo(100);
        assertThat(again.late()).isZero();
        assertThat(totalTrips()).isEqualTo(101);
    }
}
