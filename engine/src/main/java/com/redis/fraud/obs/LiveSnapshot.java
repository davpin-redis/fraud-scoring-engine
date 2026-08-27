package com.redis.fraud.obs;

/**
 * A point-in-time metrics snapshot for the live load-test dashboard (§13.5).
 * Cumulative counters plus current-window latency percentiles; the dashboard
 * derives throughput (tx/s) from the delta between successive snapshots.
 */
public record LiveSnapshot(
        long processed,
        long pass,
        long fail,
        long approve,
        long review,
        long decline,
        long degraded,
        double p50Ms,
        double p99Ms,
        long serverTimeMs
) {
}
