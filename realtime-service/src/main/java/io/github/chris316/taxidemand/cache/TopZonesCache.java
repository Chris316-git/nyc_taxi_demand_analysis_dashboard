package io.github.chris316.taxidemand.cache;

import java.time.Duration;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import io.github.chris316.taxidemand.query.DemandWindow;
import io.github.chris316.taxidemand.query.TopZonesResponse;

/**
 * Redis cache-aside for top-K responses.
 *
 * <p>Keys embed the bucket-aligned window and k, so a new bucket naturally produces a new
 * key and old entries are never served for a newer window. Only the TTL differs: windows
 * still receiving events expire in seconds, closed windows can live for minutes.
 *
 * <p>The cache is fail-open: if Redis is down or slow, reads and writes log, count the
 * error, and fall through to Postgres. A cache outage degrades latency, never availability.
 */
@Component
@Profile("!replayer")
public class TopZonesCache {

    private static final Logger log = LoggerFactory.getLogger(TopZonesCache.class);
    static final String PREFIX = "topzones:v1:";

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final Counter hits;
    private final Counter misses;
    private final Counter errors;

    public TopZonesCache(StringRedisTemplate redis, ObjectMapper mapper, MeterRegistry registry) {
        this.redis = redis;
        this.mapper = mapper;
        this.hits = Counter.builder("cache.topzones").tag("result", "hit").register(registry);
        this.misses = Counter.builder("cache.topzones").tag("result", "miss").register(registry);
        this.errors = Counter.builder("cache.topzones").tag("result", "error").register(registry);
    }

    public static String key(DemandWindow window, int k) {
        return PREFIX + window.end().getEpochSecond() + ":" + window.length().toMinutes() + ":" + k;
    }

    public Optional<TopZonesResponse> get(String key) {
        try {
            String json = redis.opsForValue().get(key);
            if (json == null) {
                misses.increment();
                return Optional.empty();
            }
            hits.increment();
            return Optional.of(mapper.readValue(json, TopZonesResponse.class));
        } catch (Exception e) {
            errors.increment();
            log.warn("Cache read failed for {}, falling back to database: {}", key, e.toString());
            return Optional.empty();
        }
    }

    public void put(String key, TopZonesResponse value, Duration ttl) {
        try {
            redis.opsForValue().set(key, mapper.writeValueAsString(value), ttl);
        } catch (Exception e) {
            errors.increment();
            log.warn("Cache write failed for {}: {}", key, e.toString());
        }
    }
}
