package io.github.chris316.taxidemand.query;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import io.github.chris316.taxidemand.cache.TopZonesCache;
import io.github.chris316.taxidemand.config.AppProperties;
import io.github.chris316.taxidemand.ingest.TimeBuckets;
import io.github.chris316.taxidemand.zones.ZoneDirectory;

/**
 * Read path for "busiest zones in the last N minutes". Cache-aside: look in Redis first,
 * otherwise aggregate in Postgres, rank with a min-heap, and populate the cache.
 */
@Service
@Profile("!replayer")
public class TopZonesService {

    private final DemandRepository repository;
    private final TopZonesCache cache;
    private final ZoneDirectory zones;
    private final WindowResolver windows;
    private final AppProperties props;

    public TopZonesService(DemandRepository repository, TopZonesCache cache, ZoneDirectory zones, AppProperties props) {
        this.repository = repository;
        this.cache = cache;
        this.zones = zones;
        this.windows = new WindowResolver(new TimeBuckets(props.bucket()));
        this.props = props;
    }

    public Result topZones(int k, Duration window, Optional<Instant> asOf) {
        Optional<Instant> watermark = repository.watermark();
        DemandWindow w = windows.resolve(window, asOf, watermark);

        String key = TopZonesCache.key(w, k);
        Optional<TopZonesResponse> cached = cache.get(key);
        if (cached.isPresent()) {
            return new Result(cached.get(), true);
        }

        List<ZoneCount> counts = repository.tripsByZone(w.start(), w.end());
        List<ZoneCount> top = TopK.largest(counts, k, ZoneCount.BY_TRIPS);

        List<TopZonesResponse.RankedZone> ranked = new ArrayList<>(top.size());
        for (int i = 0; i < top.size(); i++) {
            ZoneCount c = top.get(i);
            ZoneDirectory.Zone z = zones.lookup(c.zoneId());
            ranked.add(new TopZonesResponse.RankedZone(i + 1, c.zoneId(), z.name(), z.borough(), c.trips()));
        }
        TopZonesResponse response = new TopZonesResponse(
                w.start(), w.end(), window.toMinutes(), k, watermark.orElse(null), ranked);

        cache.put(key, response, w.live() ? props.cache().liveTtl() : props.cache().closedTtl());
        return new Result(response, false);
    }

    public record Result(TopZonesResponse body, boolean cacheHit) {
    }
}
