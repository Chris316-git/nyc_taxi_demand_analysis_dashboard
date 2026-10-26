package io.github.chris316.taxidemand.api;

import java.time.Instant;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.github.chris316.taxidemand.query.DemandRepository;

/** Ingestion progress for the dashboard and the benchmark script (per instance, since startup). */
@RestController
@RequestMapping("/api/v1")
@CrossOrigin(origins = "${app.cors-origins:*}")
@Profile("!replayer")
public class StatsController {

    private final MeterRegistry registry;
    private final DemandRepository repository;

    public StatsController(MeterRegistry registry, DemandRepository repository) {
        this.registry = registry;
        this.repository = repository;
    }

    @GetMapping("/stats")
    public Stats stats() {
        return new Stats(
                repository.watermark().orElse(null),
                count("trips.applied"),
                count("trips.duplicates"),
                count("trips.malformed"),
                count("trips.late"));
    }

    private long count(String name) {
        Counter c = registry.find(name).counter();
        return c == null ? 0 : (long) c.count();
    }

    public record Stats(Instant watermark, long tripsApplied, long duplicatesSkipped, long malformedSkipped,
                        long lateRejected) {
    }
}
