package io.github.chris316.taxidemand.ingest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes trip events in batches. Malformed records are counted and skipped (they can
 * never succeed, so retrying them would only block the partition); everything else goes
 * to {@link TripBatchProcessor} in a single transaction, and the offsets are acknowledged
 * only after that transaction commits.
 */
@Component
@Profile("!replayer")
public class TripEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(TripEventConsumer.class);

    private final TripBatchProcessor processor;
    private final ObjectMapper mapper;
    private final Counter malformedCounter;

    public TripEventConsumer(TripBatchProcessor processor, ObjectMapper mapper, MeterRegistry registry) {
        this.processor = processor;
        this.mapper = mapper;
        this.malformedCounter = Counter.builder("trips.malformed")
                .description("Records that could not be parsed into a TripEvent").register(registry);
    }

    @KafkaListener(
            id = "trip-demand-aggregator",
            topics = "${app.topic}",
            concurrency = "${app.consumer-concurrency:3}",
            autoStartup = "${app.ingest-enabled:true}")
    public void onBatch(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        List<TripEvent> events = new ArrayList<>(records.size());
        Map<Integer, Instant> progress = new HashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            try {
                TripEvent event = mapper.readValue(record.value(), TripEvent.class);
                events.add(event);
                progress.merge(record.partition(), event.pickupTime(), (a, b) -> a.isAfter(b) ? a : b);
            } catch (Exception e) {
                malformedCounter.increment();
                log.warn("Skipping malformed record at {}-{}@{}: {}",
                        record.topic(), record.partition(), record.offset(), e.getMessage());
            }
        }
        BatchResult result = processor.process(events, progress);
        ack.acknowledge();
        if (log.isDebugEnabled()) {
            log.debug("batch received={} applied={} duplicates={}",
                    result.received(), result.applied(), result.duplicates());
        }
    }
}
