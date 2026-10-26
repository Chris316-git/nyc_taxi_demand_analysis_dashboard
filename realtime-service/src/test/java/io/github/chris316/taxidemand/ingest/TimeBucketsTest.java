package io.github.chris316.taxidemand.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

class TimeBucketsTest {

    private final TimeBuckets fiveMinutes = new TimeBuckets(Duration.ofMinutes(5));

    @Test
    void floorsToBucketStart() {
        assertThat(fiveMinutes.floor(Instant.parse("2025-01-15T18:07:59Z")))
                .isEqualTo(Instant.parse("2025-01-15T18:05:00Z"));
        assertThat(fiveMinutes.floor(Instant.parse("2025-01-15T18:05:00Z")))
                .isEqualTo(Instant.parse("2025-01-15T18:05:00Z"));
    }

    @Test
    void floorsInstantsBeforeEpochDownwards() {
        assertThat(fiveMinutes.floor(Instant.parse("1969-12-31T23:58:00Z")))
                .isEqualTo(Instant.parse("1969-12-31T23:55:00Z"));
    }

    @Test
    void checksWindowIsWholeNumberOfBuckets() {
        assertThat(fiveMinutes.isMultipleOfBucket(Duration.ofMinutes(60))).isTrue();
        assertThat(fiveMinutes.isMultipleOfBucket(Duration.ofMinutes(7))).isFalse();
        assertThat(fiveMinutes.isMultipleOfBucket(Duration.ZERO)).isFalse();
    }

    @Test
    void rejectsBucketsThatDoNotDivideAnHour() {
        assertThatThrownBy(() -> new TimeBuckets(Duration.ofMinutes(7)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TimeBuckets(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
