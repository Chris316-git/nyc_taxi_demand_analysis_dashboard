package io.github.chris316.taxidemand.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.github.chris316.taxidemand.cache.TopZonesCache;
import io.github.chris316.taxidemand.config.AppProperties;
import io.github.chris316.taxidemand.zones.ZoneDirectory;

class TopZonesServiceTest {

    private static final Instant WATERMARK = Instant.parse("2025-01-15T18:05:00Z");

    private final DemandRepository repository = mock(DemandRepository.class);
    private final TopZonesCache cache = mock(TopZonesCache.class);
    private final AppProperties props = new AppProperties("trip-events", 12, Duration.ofMinutes(5),
            new AppProperties.Cache(Duration.ofSeconds(5), Duration.ofMinutes(10)), Duration.ofDays(7));
    private final TopZonesService service = new TopZonesService(repository, cache, new ZoneDirectory(), props);

    @Test
    void cacheMissAggregatesRanksAndCachesWithLiveTtl() {
        when(repository.watermark()).thenReturn(Optional.of(WATERMARK));
        when(cache.get(anyString())).thenReturn(Optional.empty());
        when(repository.tripsByZone(Instant.parse("2025-01-15T17:10:00Z"), Instant.parse("2025-01-15T18:10:00Z")))
                .thenReturn(List.of(new ZoneCount(237, 300), new ZoneCount(161, 812), new ZoneCount(48, 120)));

        TopZonesService.Result result = service.topZones(2, Duration.ofMinutes(60), Optional.empty());

        assertThat(result.cacheHit()).isFalse();
        assertThat(result.body().zones())
                .extracting(TopZonesResponse.RankedZone::rank, TopZonesResponse.RankedZone::zone,
                        TopZonesResponse.RankedZone::trips)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(1, "Midtown Center", 812L),
                        org.assertj.core.groups.Tuple.tuple(2, "Upper East Side South", 300L));
        verify(cache).put(eq("topzones:v1:1736964600:60:2"), any(), eq(Duration.ofSeconds(5)));
    }

    @Test
    void closedHistoricalWindowIsCachedWithLongTtl() {
        when(repository.watermark()).thenReturn(Optional.of(WATERMARK));
        when(cache.get(anyString())).thenReturn(Optional.empty());
        when(repository.tripsByZone(any(), any())).thenReturn(List.of());

        service.topZones(10, Duration.ofMinutes(60), Optional.of(Instant.parse("2025-01-15T09:00:00Z")));

        verify(cache).put(anyString(), any(), eq(Duration.ofMinutes(10)));
    }

    @Test
    void cacheHitSkipsTheDatabaseAggregation() {
        TopZonesResponse cached = new TopZonesResponse(null, null, 60, 10, WATERMARK, List.of());
        when(repository.watermark()).thenReturn(Optional.of(WATERMARK));
        when(cache.get(anyString())).thenReturn(Optional.of(cached));

        TopZonesService.Result result = service.topZones(10, Duration.ofMinutes(60), Optional.empty());

        assertThat(result.cacheHit()).isTrue();
        assertThat(result.body()).isSameAs(cached);
        verify(repository, never()).tripsByZone(any(), any());
    }
}
