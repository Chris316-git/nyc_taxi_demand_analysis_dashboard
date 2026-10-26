package io.github.chris316.taxidemand.replay;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import io.github.chris316.taxidemand.ingest.TripEvent;
import io.github.chris316.taxidemand.ingest.TripIds;

/**
 * Parses one line of the prepared replay CSV:
 * {@code vendor_id,pickup_datetime,dropoff_datetime,pu_location_id,do_location_id,trip_distance,fare_amount,total_amount}.
 * TLC timestamps are New York local time; they become UTC instants here.
 */
public final class TripCsvParser {

    public static final String HEADER =
            "vendor_id,pickup_datetime,dropoff_datetime,pu_location_id,do_location_id,trip_distance,fare_amount,total_amount";

    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");
    private static final DateTimeFormatter TLC_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private TripCsvParser() {
    }

    public static TripEvent parse(String line) {
        String[] c = line.split(",", -1);
        if (c.length != 8) {
            throw new IllegalArgumentException("expected 8 columns, got " + c.length + ": " + line);
        }
        int pu = Integer.parseInt(c[3].trim());
        int dropoff = Integer.parseInt(c[4].trim());
        String tripId = TripIds.of(c[0].trim(), c[1].trim(), c[2].trim(), pu, dropoff, c[5].trim(), c[7].trim());
        var pickup = LocalDateTime.parse(c[1].trim(), TLC_TIME).atZone(NEW_YORK).toInstant();
        return new TripEvent(tripId, pu, dropoff, pickup, new BigDecimal(c[6].trim()));
    }
}
