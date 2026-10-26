package io.github.chris316.taxidemand.replay;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import io.github.chris316.taxidemand.config.AppProperties;
import io.github.chris316.taxidemand.ingest.TripEvent;

/**
 * Streams historical TLC trips into Kafka as if they were happening live.
 *
 * <p>Events are keyed by pickup zone, so all trips for a zone land on the same partition
 * and stay in order. With {@code replay.speedup > 0} the replayer sleeps so that event-time
 * gaps are compressed by that factor; with 0 it sends as fast as possible.
 */
@Component
@Profile("replayer")
public class TripReplayer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(TripReplayer.class);
    private static final int MAX_IN_FLIGHT = 10_000;

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final AppProperties app;
    private final ReplayProperties replay;

    public TripReplayer(KafkaTemplate<String, String> kafka, ObjectMapper mapper,
                        AppProperties app, ReplayProperties replay) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.app = app;
        this.replay = replay;
    }

    @Override
    public void run(String... args) throws Exception {
        if (replay.file() == null || replay.file().isBlank()) {
            throw new IllegalStateException("Set replay.file to a prepared trips CSV (see tools/prepare_replay.py)");
        }
        Path path = Path.of(replay.file());
        Random random = new Random(replay.seed());
        List<CompletableFuture<?>> inFlight = new ArrayList<>(MAX_IN_FLIGHT);

        long sent = 0;
        long duplicates = 0;
        long skipped = 0;
        Instant firstEventTime = null;
        long wallStartNanos = System.nanoTime();

        try (BufferedReader reader = open(path)) {
            String header = reader.readLine();
            if (!TripCsvParser.HEADER.equals(header)) {
                throw new IllegalStateException("Unexpected header: " + header);
            }
            String line;
            while ((line = reader.readLine()) != null) {
                if (replay.limit() > 0 && sent >= replay.limit()) {
                    break;
                }
                TripEvent event;
                try {
                    event = TripCsvParser.parse(line);
                } catch (RuntimeException e) {
                    skipped++;
                    continue;
                }
                if (firstEventTime == null) {
                    firstEventTime = event.pickupTime();
                }
                pace(firstEventTime, event.pickupTime(), wallStartNanos);

                String key = Integer.toString(event.pickupZoneId());
                String json = mapper.writeValueAsString(event);
                inFlight.add(kafka.send(app.topic(), key, json));
                sent++;
                if (replay.duplicateRate() > 0 && random.nextDouble() < replay.duplicateRate()) {
                    inFlight.add(kafka.send(app.topic(), key, json));
                    duplicates++;
                }
                if (inFlight.size() >= MAX_IN_FLIGHT) {
                    awaitAll(inFlight);
                }
            }
        }
        kafka.flush();
        awaitAll(inFlight);

        double seconds = (System.nanoTime() - wallStartNanos) / 1e9;
        log.info("Replay finished: {} trips + {} injected duplicates sent in {}s ({} msgs/s), {} unparseable lines skipped",
                sent, duplicates, String.format("%.1f", seconds),
                String.format("%.0f", (sent + duplicates) / Math.max(seconds, 1e-9)), skipped);
    }

    private void pace(Instant firstEventTime, Instant eventTime, long wallStartNanos) throws InterruptedException {
        if (replay.speedup() <= 0) {
            return;
        }
        long eventOffsetNanos = Duration.between(firstEventTime, eventTime).toNanos();
        long targetWallNanos = (long) (eventOffsetNanos / replay.speedup());
        long sleepNanos = targetWallNanos - (System.nanoTime() - wallStartNanos);
        if (sleepNanos > 1_000_000) {
            Thread.sleep(Duration.ofNanos(sleepNanos));
        }
    }

    private static void awaitAll(List<CompletableFuture<?>> futures) {
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        futures.clear();
    }

    private static BufferedReader open(Path path) throws IOException {
        InputStream in = Files.newInputStream(path);
        if (path.toString().endsWith(".gz")) {
            in = new GZIPInputStream(in, 1 << 16);
        }
        return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 16);
    }
}
