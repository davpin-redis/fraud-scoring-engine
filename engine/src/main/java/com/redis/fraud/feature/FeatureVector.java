package com.redis.fraud.feature;

import java.util.Collections;
import java.util.Map;

/**
 * The assembled feature vector for one transaction: metric values keyed by
 * metric_id, plus base request-derived fields (amount, countries, membership
 * and blacklist flags). Consumed by the rules engine and model in Phase 2.
 */
public record FeatureVector(Map<String, Object> values) {

    public FeatureVector {
        values = Collections.unmodifiableMap(values);
    }

    public Object get(String key) {
        return values.get(key);
    }

    public Long asLong(String key) {
        Object v = values.get(key);
        return v instanceof Number n ? n.longValue() : null;
    }

    public Double asDouble(String key) {
        Object v = values.get(key);
        return v instanceof Number n ? n.doubleValue() : null;
    }

    public Boolean asBoolean(String key) {
        Object v = values.get(key);
        return v instanceof Boolean b ? b : null;
    }
}
