package io.github.chris316.taxidemand.query;

import java.util.Comparator;

/** Trips for one zone over a window. */
public record ZoneCount(int zoneId, long trips) {

    /** More trips ranks higher; on a tie the lower zone id wins, so rankings are deterministic. */
    public static final Comparator<ZoneCount> BY_TRIPS =
            Comparator.comparingLong(ZoneCount::trips)
                    .thenComparing(ZoneCount::zoneId, Comparator.reverseOrder());
}
