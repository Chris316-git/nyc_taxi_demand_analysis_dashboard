package io.github.chris316.taxidemand.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import io.github.chris316.taxidemand.ingest.TripBatchProcessor;
import io.github.chris316.taxidemand.ingest.TripEvent;
import io.github.chris316.taxidemand.query.TopZonesResponse;
import io.github.chris316.taxidemand.support.IntegrationTest;

class TopZonesApiIT extends IntegrationTest {

    @Autowired
    private TestRestTemplate http;

    @Autowired
    private TripBatchProcessor processor;

    private static final Instant T0 = Instant.parse("2025-01-15T18:00:00Z");

    private void seed() {
        List<TripEvent> trips = new ArrayList<>();
        addTrips(trips, 161, 30, T0.plusSeconds(600));   // Midtown Center: 30 in the last hour
        addTrips(trips, 237, 20, T0.plusSeconds(1200));  // Upper East Side South: 20
        addTrips(trips, 48, 10, T0.plusSeconds(1800));   // Clinton East: 10
        addTrips(trips, 132, 99, T0.minusSeconds(7200)); // JFK: 99, but two hours earlier
        addTrips(trips, 138, 1, T0.plusSeconds(3300));   // LaGuardia: newest bucket (18:55)
        processor.process(trips);
    }

    private static void addTrips(List<TripEvent> trips, int zone, int n, Instant at) {
        for (int i = 0; i < n; i++) {
            trips.add(new TripEvent(zone + "-" + at + "-" + i, zone, 1, at.plusSeconds(i), BigDecimal.TEN));
        }
    }

    @Test
    void ranksZonesInTheSlidingWindowAndServesRepeatsFromCache() {
        seed();

        ResponseEntity<TopZonesResponse> first =
                http.getForEntity("/api/v1/zones/top?k=3&windowMinutes=60", TopZonesResponse.class);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(first.getHeaders().getFirst("X-Cache")).isEqualTo("MISS");
        assertThat(first.getBody().windowEnd()).isEqualTo(Instant.parse("2025-01-15T19:00:00Z"));
        assertThat(first.getBody().zones())
                .extracting(TopZonesResponse.RankedZone::zone)
                .containsExactly("Midtown Center", "Upper East Side South", "Clinton East");

        ResponseEntity<TopZonesResponse> second =
                http.getForEntity("/api/v1/zones/top?k=3&windowMinutes=60", TopZonesResponse.class);
        assertThat(second.getHeaders().getFirst("X-Cache")).isEqualTo("HIT");
        assertThat(second.getBody()).isEqualTo(first.getBody());
    }

    @Test
    void historicalWindowSeesEarlierDemand() {
        seed();

        TopZonesResponse body = http.getForObject(
                "/api/v1/zones/top?k=1&windowMinutes=30&asOf=2025-01-15T16:10:00Z", TopZonesResponse.class);

        assertThat(body.zones()).singleElement()
                .satisfies(z -> {
                    assertThat(z.zone()).isEqualTo("JFK Airport");
                    assertThat(z.trips()).isEqualTo(99);
                });
    }

    @Test
    void zoneSeriesReturnsBuckets() {
        seed();

        DemandController.ZoneSeriesResponse body = http.getForObject(
                "/api/v1/zones/161/demand?windowMinutes=60", DemandController.ZoneSeriesResponse.class);

        assertThat(body.zone()).isEqualTo("Midtown Center");
        assertThat(body.buckets()).hasSize(1);
        assertThat(body.buckets().get(0).trips()).isEqualTo(30);
    }

    @Test
    void invalidParametersAreRejectedWith400() {
        assertThat(http.getForEntity("/api/v1/zones/top?k=0", String.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.getForEntity("/api/v1/zones/top?windowMinutes=7", String.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.getForEntity("/api/v1/zones/top?asOf=yesterday", String.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.getForEntity("/api/v1/zones/999/demand", String.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
