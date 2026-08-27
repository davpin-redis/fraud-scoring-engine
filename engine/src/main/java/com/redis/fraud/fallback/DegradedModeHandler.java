package com.redis.fraud.fallback;

import com.redis.fraud.api.dto.ScoreRequest;
import com.redis.fraud.feature.FeatureService;
import com.redis.fraud.feature.FeatureVector;
import com.redis.fraud.signal.SignalReader;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Graceful degradation for feature assembly (design doc §3.5): if a feature
 * source is unavailable or times out, score without it and flag the transaction
 * as degraded rather than failing it.
 *
 * <p>Two sources are assembled independently so one can fail without the other:
 * <ul>
 *   <li>the Redis <b>feature store</b> (the primary source; a failure here routes
 *       the transaction to review via {@link com.redis.fraud.scoring.ScoringService});</li>
 *   <li>the <b>hot-window signals</b> — 24h exact (feature store) + 90d approximate
 *       (signal store), read by {@link SignalReader}. A failure here just drops the
 *       hot-window signals (R012/R013/R005/velocity/amount), so a signal-store
 *       hiccup never blocks traffic on its own.</li>
 * </ul>
 */
@Component
public class DegradedModeHandler {

    private static final Logger log = LoggerFactory.getLogger(DegradedModeHandler.class);

    private final FeatureService featureService;
    private final SignalReader signalReader;

    public DegradedModeHandler(FeatureService featureService, SignalReader signalReader) {
        this.featureService = featureService;
        this.signalReader = signalReader;
    }

    public Assembled assembleFeatures(ScoreRequest request) {
        try {
            return new Assembled(featureService.assemble(request), false);
        } catch (RuntimeException e) {
            log.warn("Feature assembly failed for txn {}: {} — scoring in degraded mode",
                    request.transactionId(), e.toString());
            return new Assembled(new FeatureVector(new LinkedHashMap<>()), true);
        }
    }

    public HotWindow assembleHotWindow(ScoreRequest request) {
        try {
            return new HotWindow(signalReader.read(request), false);
        } catch (RuntimeException e) {
            log.warn("Hot-window read failed for txn {}: {} — scoring without hot-window signals",
                    request.transactionId(), e.toString());
            return new HotWindow(SignalReader.empty(), true);
        }
    }

    /** Assembled features plus whether assembly degraded. */
    public record Assembled(FeatureVector features, boolean degraded) {
    }

    /** Assembled hot-window signals plus whether the read degraded. */
    public record HotWindow(Map<String, Object> features, boolean degraded) {
    }

    /** Merges the primary feature vector with the hot-window signals into one vector. */
    public static FeatureVector merge(FeatureVector features, Map<String, Object> hotWindow) {
        Map<String, Object> merged = new LinkedHashMap<>(features.values());
        merged.putAll(hotWindow);
        return new FeatureVector(merged);
    }
}
