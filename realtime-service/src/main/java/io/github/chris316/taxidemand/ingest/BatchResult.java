package io.github.chris316.taxidemand.ingest;

/**
 * @param received   events handed to the processor
 * @param applied    events counted for the first time
 * @param duplicates events skipped because their trip id was already processed
 *                   (either earlier, or twice within the same batch)
 * @param late       events rejected because they are older than the dedup horizon,
 *                   where duplicates can no longer be detected
 */
public record BatchResult(int received, int applied, int duplicates, int late) {
}
