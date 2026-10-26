package io.github.chris316.taxidemand.replay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import io.github.chris316.taxidemand.ingest.TripEvent;

class TripCsvParserTest {

    @Test
    void parsesLineAndConvertsNewYorkTimeToUtc() {
        TripEvent e = TripCsvParser.parse("1,2025-01-01 00:18:38,2025-01-01 00:26:59,229,237,1.60,10.0,18.0");
        assertThat(e.pickupZoneId()).isEqualTo(229);
        assertThat(e.dropoffZoneId()).isEqualTo(237);
        assertThat(e.pickupTime()).isEqualTo(Instant.parse("2025-01-01T05:18:38Z")); // EST = UTC-5
        assertThat(e.fareAmount()).isEqualByComparingTo(new BigDecimal("10.0"));
        assertThat(e.tripId()).hasSize(32);
    }

    @Test
    void handlesDaylightSavingTime() {
        TripEvent e = TripCsvParser.parse("1,2025-07-04 12:00:00,2025-07-04 12:10:00,132,48,10.2,40.0,55.5");
        assertThat(e.pickupTime()).isEqualTo(Instant.parse("2025-07-04T16:00:00Z")); // EDT = UTC-4
    }

    @Test
    void rejectsMalformedLines() {
        assertThatThrownBy(() -> TripCsvParser.parse("1,2025-01-01 00:18:38,229"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TripCsvParser.parse("1,2025-01-01 00:18:38,2025-01-01 00:26:59,0,237,1.6,10.0,18.0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pickupZoneId");
    }
}
