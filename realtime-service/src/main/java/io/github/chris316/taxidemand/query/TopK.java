package io.github.chris316.taxidemand.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Selects the k largest elements with a bounded min-heap: O(n log k) time, O(k) memory,
 * versus O(n log n) for a full sort. The heap's root is always the smallest of the
 * current top-k, so each new element only needs one comparison against it.
 */
public final class TopK {

    private TopK() {
    }

    /**
     * @return the {@code k} largest elements under {@code order}, largest first.
     *         Ties are broken by the comparator, so pass a total order for stable output.
     */
    public static <T> List<T> largest(Iterable<T> items, int k, Comparator<? super T> order) {
        if (k <= 0) {
            return List.of();
        }
        PriorityQueue<T> heap = new PriorityQueue<>(k, order);
        for (T item : items) {
            if (heap.size() < k) {
                heap.offer(item);
            } else if (order.compare(item, heap.peek()) > 0) {
                heap.poll();
                heap.offer(item);
            }
        }
        List<T> result = new ArrayList<>(heap);
        result.sort(Collections.reverseOrder(order));
        return result;
    }
}
