package io.github.chris316.taxidemand.query;

import java.time.Instant;
import java.util.List;

public record TopZonesResponse(
        Instant windowStart,
        Instant windowEnd,
        long windowMinutes,
        int k,
        Instant watermark,
        List<RankedZone> zones) {

    public record RankedZone(int rank, int zoneId, String zone, String borough, long trips) {
    }
}
