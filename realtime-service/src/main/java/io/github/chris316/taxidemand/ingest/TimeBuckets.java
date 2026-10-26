package io.github.chris316.taxidemand.ingest;

import java.time.Duration;
import java.time.Instant;

/** Aligns event times to fixed-width buckets (e.g. 5 minutes) anchored at the Unix epoch. */
public final class TimeBuckets {

    private final long bucketMillis;

    public TimeBuckets(Duration bucket) {
        if (bucket.isNegative() || bucket.isZero()) {
            throw new IllegalArgumentException("bucket must be positive");
        }
        if (Duration.ofHours(1).toMillis() % bucket.toMillis() != 0) {
            throw new IllegalArgumentException("bucket must divide one hour evenly: " + bucket);
        }
        this.bucketMillis = bucket.toMillis();
    }

    public Duration width() {
        return Duration.ofMillis(bucketMillis);
    }

    /** Start of the bucket containing {@code t}. */
    public Instant floor(Instant t) {
        long ms = t.toEpochMilli();
        return Instant.ofEpochMilli(Math.floorDiv(ms, bucketMillis) * bucketMillis);
    }

    /** True when {@code d} is a positive whole number of buckets. */
    public boolean isMultipleOfBucket(Duration d) {
        long ms = d.toMillis();
        return ms > 0 && ms % bucketMillis == 0;
    }
}
