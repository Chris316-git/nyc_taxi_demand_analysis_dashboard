package io.github.chris316.taxidemand.query;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import io.github.chris316.taxidemand.ingest.TimeBuckets;

/** Turns API parameters (window length, optional as-of time) into a bucket-aligned window. */
public final class WindowResolver {

    private final TimeBuckets buckets;

    public WindowResolver(TimeBuckets buckets) {
        this.buckets = buckets;
    }

    /**
     * @param length    window length, a whole number of buckets
     * @param asOf      the window ends with the bucket containing this instant; defaults to the watermark
     * @param watermark start of the newest bucket with data, if any data exists
     */
    public DemandWindow resolve(Duration length, Optional<Instant> asOf, Optional<Instant> watermark) {
        if (!buckets.isMultipleOfBucket(length)) {
            throw new IllegalArgumentException(
                    "window must be a positive multiple of " + buckets.width().toMinutes() + " minutes");
        }
        Instant anchor = asOf.or(() -> watermark).orElse(Instant.EPOCH);
        Instant end = buckets.floor(anchor).plus(buckets.width());
        Instant start = end.minus(length);
        boolean live = watermark.map(w -> !end.isBefore(w.plus(buckets.width()))).orElse(true);
        return new DemandWindow(start, end, length, live);
    }
}
