package io.github.chris316.taxidemand.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import io.github.chris316.taxidemand.query.DemandWindow;
import io.github.chris316.taxidemand.query.TopZonesResponse;

class TopZonesCacheTest {

    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> ops = mock(ValueOperations.class);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final TopZonesCache cache;

    TopZonesCacheTest() {
        when(redis.opsForValue()).thenReturn(ops);
        cache = new TopZonesCache(redis, new ObjectMapper().registerModule(new JavaTimeModule()), registry);
    }

    @Test
    void keyEncodesWindowEndLengthAndK() {
        DemandWindow w = new DemandWindow(Instant.parse("2025-01-15T17:10:00Z"),
                Instant.parse("2025-01-15T18:10:00Z"), Duration.ofMinutes(60), true);
        assertThat(TopZonesCache.key(w, 10)).isEqualTo("topzones:v1:1736964600:60:10");
    }

    @Test
    void roundTripsResponses() throws Exception {
        TopZonesResponse response = new TopZonesResponse(Instant.parse("2025-01-15T17:10:00Z"),
                Instant.parse("2025-01-15T18:10:00Z"), 60, 1, Instant.parse("2025-01-15T18:05:00Z"),
                List.of(new TopZonesResponse.RankedZone(1, 161, "Midtown Center", "Manhattan", 812)));
        String json = new ObjectMapper().registerModule(new JavaTimeModule()).writeValueAsString(response);
        when(ops.get("k")).thenReturn(json);

        assertThat(cache.get("k")).contains(response);
    }

    @Test
    void readFailureFallsThroughInsteadOfThrowing() {
        when(ops.get(anyString())).thenThrow(new RedisConnectionFailureException("redis down"));

        assertThat(cache.get("k")).isEmpty();
        assertThat(registry.get("cache.topzones").tag("result", "error").counter().count()).isEqualTo(1);
    }

    @Test
    void writeFailureIsSwallowed() {
        doThrow(new RedisConnectionFailureException("redis down"))
                .when(ops).set(anyString(), anyString(), any(Duration.class));

        assertThatCode(() -> cache.put("k", new TopZonesResponse(null, null, 60, 1, null, List.of()), Duration.ofSeconds(5)))
                .doesNotThrowAnyException();
    }
}
