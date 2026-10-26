package io.github.chris316.taxidemand.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TripIdsTest {

    private static String id(String total) {
        return TripIds.of("2", "2025-01-01 00:18:38", "2025-01-01 00:26:59", 229, 237, "1.6", total);
    }

    @Test
    void isDeterministicAnd128BitsOfHex() {
        assertThat(id("18.0")).isEqualTo(id("18.0")).hasSize(32).matches("[0-9a-f]{32}");
    }

    @Test
    void anyFieldChangeProducesADifferentId() {
        assertThat(id("18.0")).isNotEqualTo(id("18.5"));
        assertThat(TripIds.of("2", "2025-01-01 00:18:38", "2025-01-01 00:26:59", 229, 236, "1.6", "18.0"))
                .isNotEqualTo(id("18.0"));
    }
}
