package io.github.chris316.taxidemand.ingest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * TLC trip records have no primary key, so we derive one: a SHA-256 over the fields that
 * together identify a physical trip, truncated to 128 bits (32 hex chars).
 *
 * Deterministic ids are the foundation of idempotency: if the replayer, a producer retry,
 * or a consumer rebalance delivers the same trip twice, both copies hash to the same id
 * and the second insert into processed_trips is rejected.
 */
public final class TripIds {

    private static final HexFormat HEX = HexFormat.of();

    private TripIds() {
    }

    public static String of(String vendorId,
                            String pickupDatetime,
                            String dropoffDatetime,
                            int pickupZoneId,
                            int dropoffZoneId,
                            String tripDistance,
                            String totalAmount) {
        String canonical = String.join("|",
                vendorId, pickupDatetime, dropoffDatetime,
                Integer.toString(pickupZoneId), Integer.toString(dropoffZoneId),
                tripDistance, totalAmount);
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HEX.formatHex(digest, 0, 16);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the JDK spec", e);
        }
    }
}
