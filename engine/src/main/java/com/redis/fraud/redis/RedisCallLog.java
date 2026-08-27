package com.redis.fraud.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Emits one log line per Redis call — the command type (e.g. {@code MGET},
 * {@code FT.SEARCH}) and how long it took, measured from issuing the query to
 * receiving the result. Feature-store (Lettuce) commands are fed in by
 * {@link LoggingCommandListener} at the driver level (so even pipelined commands
 * are logged individually); transaction-store (Jedis/OM Spring) calls are timed
 * at their call sites, since that client has no per-command hook.
 *
 * <p>Gated by {@code fraud.redis.log-calls} (default {@code true}). Logged at
 * INFO on this class's own logger, so it can be silenced independently with
 * {@code logging.level.com.redis.fraud.redis.RedisCallLog=WARN}. It is verbose at
 * high TPS — turn it off (or down) for a full-scale load run.
 */
@Component
public class RedisCallLog {

    private static final Logger log = LoggerFactory.getLogger(RedisCallLog.class);

    private final boolean enabled;

    public RedisCallLog(@Value("${fraud.redis.log-calls:true}") boolean enabled) {
        this.enabled = enabled;
    }

    public boolean enabled() {
        return enabled;
    }

    /** Log a completed call. {@code store} is {@code feature} or {@code transaction}. */
    public void record(String store, String op, long nanos) {
        if (enabled) {
            log.info("redis-call store={} op={} took_ms={}", store, op, nanos / 1_000_000.0);
        }
    }

    /** Log a failed call (no reliable duration available). */
    public void recordFailure(String store, String op, String error) {
        if (enabled) {
            log.info("redis-call store={} op={} FAILED error={}", store, op, error);
        }
    }
}
