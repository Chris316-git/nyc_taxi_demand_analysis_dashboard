package io.github.chris316.taxidemand.query;

import java.time.Duration;
import java.time.Instant;

/**
 * A half-open event-time window {@code [start, end)} aligned to bucket boundaries.
 *
 * @param live true when the window contains the newest bucket, which may still receive
 *             events; such results are cached only briefly
 */
public record DemandWindow(Instant start, Instant end, Duration length, boolean live) {
}
