package com.redis.fraud.obs;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

/**
 * Micrometer instrumentation for the scoring path (design doc §10, §13.5).
 * Exposes latency percentiles, pass/fail counts, the decision mix, per-rule fire
 * rates, and the degraded-mode counter — all also visible on
 * {@code /actuator/prometheus}. {@link #snapshot(long)} feeds {@code /metrics/live}.
 */
@Component
public class ScoringMetrics {

    private final MeterRegistry registry;
    private final Counter pass;
    private final Counter fail;
    private final Counter degraded;
    private final Timer latency;

    public ScoringMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.pass = Counter.builder("fraud.score.pass")
                .description("Requests that returned a scored decision").register(registry);
        this.fail = Counter.builder("fraud.score.fail")
                .description("Requests that failed with a transport/5xx error").register(registry);
        this.degraded = Counter.builder("fraud.score.degraded")
                .description("Requests scored in degraded mode (§3.5)").register(registry);
        this.latency = Timer.builder("fraud.score.latency")
                .description("End-to-end scoring latency")
                .publishPercentiles(0.5, 0.99)
                .register(registry);
    }

    public void recordSuccess(String decision, boolean wasDegraded, List<String> firedRules, long nanos) {
        latency.record(nanos, TimeUnit.NANOSECONDS);
        pass.increment();
        registry.counter("fraud.score.decision", "decision", decision).increment();
        if (wasDegraded) {
            degraded.increment();
        }
        for (String rule : firedRules) {
            registry.counter("fraud.rule.fired", "rule", rule).increment();
        }
    }

    public void recordFailure() {
        fail.increment();
    }

    public LiveSnapshot snapshot(long serverTimeMs) {
        long passCount = (long) pass.count();
        long failCount = (long) fail.count();
        return new LiveSnapshot(
                passCount + failCount,
                passCount,
                failCount,
                decisionCount("approve"),
                decisionCount("review"),
                decisionCount("decline"),
                (long) degraded.count(),
                percentileMs(0.5),
                percentileMs(0.99),
                serverTimeMs);
    }

    private long decisionCount(String decision) {
        Counter c = registry.find("fraud.score.decision").tag("decision", decision).counter();
        return c == null ? 0L : (long) c.count();
    }

    private double percentileMs(double percentile) {
        for (ValueAtPercentile v : latency.takeSnapshot().percentileValues()) {
            if (Math.abs(v.percentile() - percentile) < 1e-9) {
                return v.value(TimeUnit.MILLISECONDS);
            }
        }
        return 0.0;
    }
}
