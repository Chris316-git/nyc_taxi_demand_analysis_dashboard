package io.github.chris316.taxidemand.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.github.chris316.taxidemand.support.IntegrationTest;
import io.github.chris316.taxidemand.support.TestTrips;

/**
 * Simulates the failure modes of at-least-once delivery directly against the processor:
 * a rebalance that hands the same records to two consumers at once, and a consumer that
 * crashes after committing to Postgres but before committing its Kafka offsets.
 */
class ConcurrentProcessingIT extends IntegrationTest {

    @Autowired
    private TripBatchProcessor processor;

    @Test
    void overlappingBatchesOnEightThreadsCountEachTripOnce() throws Exception {
        List<TripEvent> trips = TestTrips.generate(5_000, 2);

        // Each trip appears in 4 different batches; batches are shuffled across 8 workers.
        List<List<TripEvent>> batches = new ArrayList<>();
        Random random = new Random(5);
        for (int copy = 0; copy < 4; copy++) {
            List<TripEvent> shuffled = new ArrayList<>(trips);
            Collections.shuffle(shuffled, random);
            for (int i = 0; i < shuffled.size(); i += 250) {
                batches.add(shuffled.subList(i, Math.min(i + 250, shuffled.size())));
            }
        }
        Collections.shuffle(batches, random);

        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch startGate = new CountDownLatch(1);
        List<Future<BatchResult>> futures = new ArrayList<>();
        for (List<TripEvent> batch : batches) {
            futures.add(pool.submit(() -> {
                startGate.await();
                return processor.process(batch);
            }));
        }
        startGate.countDown();

        long applied = 0;
        long duplicates = 0;
        for (Future<BatchResult> f : futures) {
            BatchResult r = f.get(); // rethrows deadlocks or constraint violations, failing the test
            applied += r.applied();
            duplicates += r.duplicates();
        }
        pool.shutdown();

        assertThat(applied).isEqualTo(5_000);
        assertThat(duplicates).isEqualTo(15_000);
        assertThat(totalTrips()).isEqualTo(5_000);
        assertThat(processedIds()).isEqualTo(5_000);
    }

    @Test
    void reprocessingACommittedBatchAfterACrashChangesNothing() {
        List<TripEvent> batch = TestTrips.generate(500, 3);

        BatchResult first = processor.process(batch);
        BatchResult redelivered = processor.process(batch);

        assertThat(first.applied()).isEqualTo(500);
        assertThat(redelivered.applied()).isZero();
        assertThat(redelivered.duplicates()).isEqualTo(500);
        assertThat(totalTrips()).isEqualTo(500);
    }

    @Test
    void duplicatesInsideOneBatchAreCollapsed() {
        List<TripEvent> trips = TestTrips.generate(100, 4);
        List<TripEvent> batch = new ArrayList<>(trips);
        batch.addAll(trips.subList(0, 30));

        BatchResult result = processor.process(batch);

        assertThat(result.applied()).isEqualTo(100);
        assertThat(result.duplicates()).isEqualTo(30);
        assertThat(totalTrips()).isEqualTo(100);
    }
}
