package io.github.chris316.taxidemand.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * All tunables in one place, bound from the {@code app.*} namespace.
 *
 * @param topic        Kafka topic carrying trip events (keyed by pickup zone)
 * @param partitions   partition count used when the service creates the topic
 * @param bucket       width of an aggregation bucket; windows are whole multiples of it
 * @param cache        TTLs for cached top-K responses
 * @param dedupRetention how long processed trip ids are kept for duplicate detection
 */
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        @DefaultValue("trip-events") String topic,
        @DefaultValue("12") int partitions,
        @DefaultValue("5m") Duration bucket,
        @DefaultValue Cache cache,
        @DefaultValue("7d") Duration dedupRetention) {

    /**
     * @param liveTtl   TTL for windows that include the newest (still filling) bucket
     * @param closedTtl TTL for windows that end before the newest bucket and can no longer change
     */
    public record Cache(
            @DefaultValue("5s") Duration liveTtl,
            @DefaultValue("10m") Duration closedTtl) {
    }
}
