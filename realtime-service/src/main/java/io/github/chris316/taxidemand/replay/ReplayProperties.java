package io.github.chris316.taxidemand.replay;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param file          prepared trips CSV (optionally .gz), sorted by pickup time
 * @param speedup       event-time compression: 60 replays one hour of trips per minute;
 *                      0 sends as fast as the producer allows (used for throughput benchmarks)
 * @param limit         stop after this many trips (0 = whole file)
 * @param duplicateRate fraction of trips sent a second time, to exercise idempotent consumption
 * @param seed          RNG seed so duplicate injection is reproducible
 */
@ConfigurationProperties(prefix = "replay")
public record ReplayProperties(
        String file,
        @DefaultValue("0") double speedup,
        @DefaultValue("0") long limit,
        @DefaultValue("0") double duplicateRate,
        @DefaultValue("42") long seed) {
}
