package io.github.chris316.taxidemand.ingest;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * One taxi trip as it travels through Kafka.
 *
 * @param tripId        deterministic id derived from the raw TLC record (see {@link TripIds});
 *                      the same trip always gets the same id, which is what makes
 *                      redelivered or replayed events detectable
 * @param pickupZoneId  TLC taxi zone (1-265) where the trip started; also the Kafka key
 * @param dropoffZoneId TLC taxi zone where the trip ended
 * @param pickupTime    event time used for windowing
 * @param fareAmount    metered fare in USD
 */
public record TripEvent(
        String tripId,
        int pickupZoneId,
        int dropoffZoneId,
        Instant pickupTime,
        BigDecimal fareAmount) {

    public static final int MIN_ZONE_ID = 1;
    public static final int MAX_ZONE_ID = 265;

    public TripEvent {
        Objects.requireNonNull(tripId, "tripId");
        Objects.requireNonNull(pickupTime, "pickupTime");
        if (tripId.isBlank()) {
            throw new IllegalArgumentException("tripId must not be blank");
        }
        if (pickupZoneId < MIN_ZONE_ID || pickupZoneId > MAX_ZONE_ID) {
            throw new IllegalArgumentException("pickupZoneId out of range: " + pickupZoneId);
        }
        if (fareAmount == null) {
            fareAmount = BigDecimal.ZERO;
        }
    }
}
