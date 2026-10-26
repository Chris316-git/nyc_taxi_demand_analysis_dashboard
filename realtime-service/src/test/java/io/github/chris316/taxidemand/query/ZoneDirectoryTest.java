package io.github.chris316.taxidemand.query;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.github.chris316.taxidemand.zones.ZoneDirectory;

class ZoneDirectoryTest {

    private final ZoneDirectory zones = new ZoneDirectory();

    @Test
    void loadsAllTlcZones() {
        assertThat(zones.all()).hasSize(265);
        assertThat(zones.lookup(161).name()).isEqualTo("Midtown Center");
        assertThat(zones.lookup(161).borough()).isEqualTo("Manhattan");
    }

    @Test
    void unknownZoneFallsBackInsteadOfFailing() {
        assertThat(zones.exists(999)).isFalse();
        assertThat(zones.lookup(999).name()).isEqualTo("Unknown");
    }
}
