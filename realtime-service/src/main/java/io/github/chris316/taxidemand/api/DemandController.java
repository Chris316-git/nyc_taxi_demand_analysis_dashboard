package io.github.chris316.taxidemand.api;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.context.annotation.Profile;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.github.chris316.taxidemand.config.AppProperties;
import io.github.chris316.taxidemand.ingest.TimeBuckets;
import io.github.chris316.taxidemand.query.DemandRepository;
import io.github.chris316.taxidemand.query.DemandWindow;
import io.github.chris316.taxidemand.query.TopZonesService;
import io.github.chris316.taxidemand.query.WindowResolver;
import io.github.chris316.taxidemand.zones.ZoneDirectory;

@RestController
@RequestMapping("/api/v1")
@Validated
@CrossOrigin(origins = "${app.cors-origins:*}")
@Profile("!replayer")
public class DemandController {

    private final TopZonesService topZones;
    private final DemandRepository repository;
    private final ZoneDirectory zones;
    private final WindowResolver windows;

    public DemandController(TopZonesService topZones, DemandRepository repository,
                            ZoneDirectory zones, AppProperties props) {
        this.topZones = topZones;
        this.repository = repository;
        this.zones = zones;
        this.windows = new WindowResolver(new TimeBuckets(props.bucket()));
    }

    /**
     * Busiest pickup zones in a sliding event-time window.
     * The {@code X-Cache} header reports whether Redis served the response.
     */
    @GetMapping("/zones/top")
    public ResponseEntity<?> top(
            @RequestParam(defaultValue = "10") @Min(1) @Max(50) int k,
            @RequestParam(defaultValue = "60") @Min(5) @Max(1440) int windowMinutes,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant asOf) {
        TopZonesService.Result result = topZones.topZones(k, Duration.ofMinutes(windowMinutes), Optional.ofNullable(asOf));
        return ResponseEntity.ok()
                .header("X-Cache", result.cacheHit() ? "HIT" : "MISS")
                .body(result.body());
    }

    /** Bucket-by-bucket demand for one zone, for sparklines. */
    @GetMapping("/zones/{zoneId}/demand")
    public ZoneSeriesResponse zoneDemand(
            @PathVariable @Min(1) @Max(265) int zoneId,
            @RequestParam(defaultValue = "180") @Min(5) @Max(1440) int windowMinutes,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant asOf) {
        if (!zones.exists(zoneId)) {
            throw new NotFoundException("Unknown zone " + zoneId);
        }
        DemandWindow w = windows.resolve(Duration.ofMinutes(windowMinutes), Optional.ofNullable(asOf), repository.watermark());
        ZoneDirectory.Zone z = zones.lookup(zoneId);
        return new ZoneSeriesResponse(zoneId, z.name(), z.borough(), w.start(), w.end(),
                repository.zoneSeries(zoneId, w.start(), w.end()));
    }

    public record ZoneSeriesResponse(int zoneId, String zone, String borough,
                                     Instant windowStart, Instant windowEnd,
                                     List<DemandRepository.BucketCount> buckets) {
    }
}
