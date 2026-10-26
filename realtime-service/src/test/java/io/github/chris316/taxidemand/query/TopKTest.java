package io.github.chris316.taxidemand.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

class TopKTest {

    @Test
    void returnsLargestElementsInDescendingOrder() {
        assertThat(TopK.largest(List.of(5, 1, 9, 3, 7), 3, Comparator.naturalOrder()))
                .containsExactly(9, 7, 5);
    }

    @Test
    void returnsEverythingSortedWhenKExceedsSize() {
        assertThat(TopK.largest(List.of(2, 8, 4), 10, Comparator.naturalOrder()))
                .containsExactly(8, 4, 2);
    }

    @Test
    void returnsEmptyForNonPositiveKOrEmptyInput() {
        assertThat(TopK.largest(List.of(1, 2), 0, Comparator.naturalOrder())).isEmpty();
        assertThat(TopK.largest(List.<Integer>of(), 5, Comparator.naturalOrder())).isEmpty();
    }

    @Test
    void breaksTiesByLowerZoneIdFirst() {
        List<ZoneCount> counts = List.of(
                new ZoneCount(161, 50), new ZoneCount(48, 50), new ZoneCount(237, 50), new ZoneCount(1, 10));
        assertThat(TopK.largest(counts, 3, ZoneCount.BY_TRIPS))
                .extracting(ZoneCount::zoneId)
                .containsExactly(48, 161, 237);
    }

    @Test
    void matchesFullSortOnRandomInput() {
        Random random = new Random(7);
        for (int round = 0; round < 200; round++) {
            List<ZoneCount> counts = new ArrayList<>();
            int n = random.nextInt(300);
            for (int zone = 1; zone <= n; zone++) {
                counts.add(new ZoneCount(zone, random.nextInt(40)));
            }
            int k = 1 + random.nextInt(20);
            List<ZoneCount> expected = counts.stream()
                    .sorted(ZoneCount.BY_TRIPS.reversed())
                    .limit(k)
                    .toList();
            assertThat(TopK.largest(counts, k, ZoneCount.BY_TRIPS)).isEqualTo(expected);
        }
    }
}
