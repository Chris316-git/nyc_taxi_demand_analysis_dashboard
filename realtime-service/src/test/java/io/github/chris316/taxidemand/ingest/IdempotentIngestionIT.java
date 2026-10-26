package io.github.chris316.taxidemand.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import io.github.chris316.taxidemand.config.AppProperties;
import io.github.chris316.taxidemand.support.IntegrationTest;
import io.github.chris316.taxidemand.support.TestTrips;

/**
 * End to end through Kafka: every trip is published twice, in shuffled order, plus a few
 * malformed records. The aggregates must match the true per-zone counts exactly.
 */
class IdempotentIngestionIT extends IntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafka;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private AppProperties props;

    @Test
    void duplicateDeliveriesAreCountedExactlyOnce() throws Exception {
        List<TripEvent> trips = TestTrips.generate(2_000, 1);
        List<TripEvent> deliveries = new ArrayList<>(trips);
        deliveries.addAll(trips);
        Collections.shuffle(deliveries, new Random(3));

        for (TripEvent t : deliveries) {
            kafka.send(props.topic(), Integer.toString(t.pickupZoneId()), mapper.writeValueAsString(t));
        }
        kafka.send(props.topic(), "1", "{not json");
        kafka.send(props.topic(), "1", "{\"tripId\":\"x\",\"pickupZoneId\":999}");
        kafka.flush();

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(processedIds()).isEqualTo(2_000));
        // Wait until every duplicate has been consumed too, then check none of them counted.
        awaitConsumerCaughtUp();

        assertThat(totalTrips()).isEqualTo(2_000);
        assertThat(countsByZoneInDb()).isEqualTo(TestTrips.countByZone(trips));
    }

    private Map<Integer, Long> countsByZoneInDb() {
        Map<Integer, Long> counts = new TreeMap<>();
        jdbc.query("SELECT zone_id, SUM(trips) AS trips FROM zone_demand_buckets GROUP BY zone_id",
                rs -> {
                    counts.put(rs.getInt("zone_id"), rs.getLong("trips"));
                });
        return counts;
    }
}
