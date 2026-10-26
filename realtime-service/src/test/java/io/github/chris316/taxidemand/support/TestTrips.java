package io.github.chris316.taxidemand.support;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import io.github.chris316.taxidemand.ingest.TripEvent;

/** Deterministic synthetic trips for integration tests. */
public final class TestTrips {

    public static final Instant START = Instant.parse("2025-01-15T17:00:00Z");

    private TestTrips() {
    }

    /** {@code n} distinct trips spread over 2 hours and 20 zones. */
    public static List<TripEvent> generate(int n, long seed) {
        Random random = new Random(seed);
        List<TripEvent> trips = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int zone = 1 + random.nextInt(20);
            Instant pickup = START.plus(Duration.ofSeconds(random.nextInt(2 * 3600)));
            trips.add(new TripEvent("trip-" + seed + "-" + i, zone, 1 + random.nextInt(265), pickup,
                    BigDecimal.valueOf(5 + random.nextInt(60))));
        }
        return trips;
    }

    /** Expected trip count per zone, computed independently of the service. */
    public static Map<Integer, Long> countByZone(List<TripEvent> trips) {
        Map<Integer, Long> counts = new TreeMap<>();
        for (TripEvent t : trips) {
            counts.merge(t.pickupZoneId(), 1L, Long::sum);
        }
        return counts;
    }
}
