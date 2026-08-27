package com.redis.fraud.redis;

import com.redis.fraud.config.RedisConfigStore;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Wires the Lettuce client for the feature store (design doc §7; the hot path
 * uses Lettuce for its multiplexed, pipelined connection — IMPLEMENTATION_PLAN §3).
 * A single shared connection is thread-safe and pipelines concurrent commands.
 */
@Configuration
public class RedisConfig {

    private static final Logger log = LoggerFactory.getLogger(RedisConfig.class);

    @Value("${fraud.feature-store.uri:redis://localhost:6379}")
    private String featureStoreUri;

    @Bean(destroyMethod = "shutdown")
    @Primary
    public RedisClient redisClient(RedisCallLog callLog) {
        RedisClient client = RedisClient.create(featureStoreUri);
        // Per-command latency logging (§10): every feature-store command is logged
        // with its type and duration. Only registered when enabled, so there's no
        // event overhead when call logging is off.
        if (callLog.enabled()) {
            client.addListener(new LoggingCommandListener(callLog));
        }
        return client;
    }

    /**
     * Dedicated feature-store connections (separate TCP channels): reads on their own
     * channel so the latency-sensitive hot-path reads (idempotency GET, feature
     * assembly, 24h signals) are never head-of-line-blocked behind the bulk,
     * off-response-path write pipelines. The read connection is {@code @Primary}.
     */
    @Bean(name = "featureReadConnection", destroyMethod = "close")
    @Primary
    public StatefulRedisConnection<String, String> featureReadConnection(RedisClient client) {
        return client.connect();
    }

    @Bean(name = "featureWriteConnection", destroyMethod = "close")
    public StatefulRedisConnection<String, String> featureWriteConnection(RedisClient client) {
        return client.connect();
    }

    /**
     * Subscribes to the config-invalidation channel (§7.2) once the app is up,
     * reloading {@link RedisConfigStore} on each message. Tolerant of a Redis
     * that isn't reachable yet — invalidation is best-effort, and the store
     * also reloads lazily on first access.
     */
    @Bean
    public ApplicationListener<ApplicationReadyEvent> configInvalidationSubscriber(
            RedisClient client, RedisConfigStore configStore) {
        return event -> {
            try {
                StatefulRedisPubSubConnection<String, String> pubSub = client.connectPubSub();
                pubSub.addListener(new RedisPubSubAdapter<>() {
                    @Override
                    public void message(String channel, String message) {
                        configStore.onInvalidation();
                    }
                });
                pubSub.async().subscribe(RedisConfigStore.INVALIDATION_CHANNEL);
                log.info("Subscribed to config invalidation channel '{}'", RedisConfigStore.INVALIDATION_CHANNEL);
            } catch (RuntimeException e) {
                log.warn("Could not subscribe to config invalidation channel: {}", e.getMessage());
            }
        };
    }
}
