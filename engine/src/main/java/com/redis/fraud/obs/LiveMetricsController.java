package com.redis.fraud.obs;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Live metrics feed for the test dashboard (§13.5), polled at ~1s. Reads the
 * scoring engine's own Micrometer counters, so it reflects what the service
 * measured, not what a load client observed. CORS-open so the standalone
 * dashboard file can read it directly.
 */
@RestController
public class LiveMetricsController {

    private final ScoringMetrics metrics;

    public LiveMetricsController(ScoringMetrics metrics) {
        this.metrics = metrics;
    }

    @CrossOrigin
    @GetMapping("/metrics/live")
    public LiveSnapshot live() {
        return metrics.snapshot(System.currentTimeMillis());
    }
}
