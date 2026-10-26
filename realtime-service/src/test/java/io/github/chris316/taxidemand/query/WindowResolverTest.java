package io.github.chris316.taxidemand.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.github.chris316.taxidemand.ingest.TimeBuckets;

class WindowResolverTest {

    private final WindowResolver resolver = new WindowResolver(new TimeBuckets(Duration.ofMinutes(5)));
    private final Instant watermark = Instant.parse("2025-01-15T18:05:00Z");

    @Test
    void defaultsToWindowEndingWithNewestBucket() {
        DemandWindow w = resolver.resolve(Duration.ofMinutes(60), Optional.empty(), Optional.of(watermark));
        assertThat(w.end()).isEqualTo(Instant.parse("2025-01-15T18:10:00Z"));
        assertThat(w.start()).isEqualTo(Instant.parse("2025-01-15T17:10:00Z"));
        assertThat(w.live()).isTrue();
    }

    @Test
    void historicalWindowIsClosed() {
        DemandWindow w = resolver.resolve(Duration.ofMinutes(30),
                Optional.of(Instant.parse("2025-01-15T12:02:00Z")), Optional.of(watermark));
        assertThat(w.start()).isEqualTo(Instant.parse("2025-01-15T11:35:00Z"));
        assertThat(w.end()).isEqualTo(Instant.parse("2025-01-15T12:05:00Z"));
        assertThat(w.live()).isFalse();
    }

    @Test
    void emptyStreamIsTreatedAsLive() {
        assertThat(resolver.resolve(Duration.ofMinutes(5), Optional.empty(), Optional.empty()).live()).isTrue();
    }

    @Test
    void rejectsWindowsThatAreNotWholeBuckets() {
        assertThatThrownBy(() -> resolver.resolve(Duration.ofMinutes(7), Optional.empty(), Optional.of(watermark)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiple of 5 minutes");
    }
}
