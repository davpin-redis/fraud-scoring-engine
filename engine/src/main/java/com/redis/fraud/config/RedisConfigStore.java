package com.redis.fraud.config;

import tools.jackson.databind.ObjectMapper;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Loads window/metric/rule definitions from the {@code cfg:*} hashes into a
 * read-through in-memory cache (design doc §7.2, §3.6). The cache is a
 * performance optimization, not session state: any instance can rebuild it
 * from Redis at any time. {@link #onInvalidation()} reloads it (wired to a
 * Pub/Sub channel by the Spring configuration).
 */
@Component
public class RedisConfigStore {

    private static final Logger log = LoggerFactory.getLogger(RedisConfigStore.class);

    static final String KEY_WINDOWS = "cfg:windows";
    static final String KEY_METRICS = "cfg:metrics";
    static final String KEY_RULES = "cfg:rules";
    public static final String INVALIDATION_CHANNEL = "cfg:invalidate";

    private final StatefulRedisConnection<String, String> connection;
    private final ObjectMapper mapper;

    private volatile Map<String, WindowDef> windows;
    private volatile Map<String, MetricDef> metrics;
    private volatile Map<String, RuleDef> rules;

    public RedisConfigStore(StatefulRedisConnection<String, String> connection, ObjectMapper mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    private void ensureLoaded() {
        if (windows == null) {
            refresh();
        }
    }

    /** Reload all three config categories from Redis. Cheap; low-QPS path. */
    public synchronized void refresh() {
        RedisCommands<String, String> sync = connection.sync();
        this.windows = parse(sync.hgetall(KEY_WINDOWS), WindowDef.class);
        this.metrics = parse(sync.hgetall(KEY_METRICS), MetricDef.class);
        this.rules = parse(sync.hgetall(KEY_RULES), RuleDef.class);
        log.info("Loaded config: {} windows, {} metrics, {} rules",
                windows.size(), metrics.size(), rules.size());
    }

    /** Invoked when a Pub/Sub invalidation message is received (§7.2). */
    public void onInvalidation() {
        log.info("Config invalidation received; reloading");
        refresh();
    }

    private <T> Map<String, T> parse(Map<String, String> raw, Class<T> type) {
        Map<String, T> out = new LinkedHashMap<>();
        raw.forEach((id, json) -> {
            try {
                out.put(id, mapper.readValue(json, type));
            } catch (Exception e) {
                throw new IllegalStateException("Bad config JSON for " + type.getSimpleName() + " '" + id + "'", e);
            }
        });
        return out;
    }

    public Map<String, WindowDef> windows() {
        ensureLoaded();
        return windows;
    }

    public Map<String, MetricDef> metrics() {
        ensureLoaded();
        return metrics;
    }

    public Map<String, RuleDef> rules() {
        ensureLoaded();
        return rules;
    }

    public WindowDef window(String windowId) {
        WindowDef w = windows().get(windowId);
        if (w == null) {
            throw new IllegalArgumentException("Unknown window_id: " + windowId);
        }
        return w;
    }
}
