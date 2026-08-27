package com.redis.fraud.signal;

import com.redis.fraud.redis.LoggingCommandListener;
import com.redis.fraud.redis.RedisCallLog;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the client for the <b>signal store</b> — the RAM Redis database that holds
 * the 90-day fraud signals (design doc §7.12, replacing the Flex transaction store).
 *
 * <p>Uses <b>Lettuce</b> — the same client as the feature store — so both hot-path
 * stores share one connection model: a single multiplexed, pipelined connection
 * (no blocking pool, so no connection-storm risk under load). Every signal is built
 * on <b>core</b> Redis types (HyperLogLog, Set, counter, hash), so no Redis modules
 * are required on this DB.
 */
@Configuration
public class SignalStoreConfig {

    @Value("${fraud.signal-store.uri:redis://localhost:6381}")
    private String signalStoreUri;

    @Bean(destroyMethod = "shutdown")
    public RedisClient signalRedisClient(RedisCallLog callLog) {
        RedisClient client = RedisClient.create(signalStoreUri);
        if (callLog.enabled()) {
            client.addListener(new LoggingCommandListener(callLog));
        }
        return client;
    }

    // Dedicated signal-store connections (separate TCP channels): 90d reads (incl. the
    // heavier TS.RANGE aggregations) on their own channel, isolated from the write path.
    @Bean(name = "signalReadConnection", destroyMethod = "close")
    public StatefulRedisConnection<String, String> signalReadConnection(
            @Qualifier("signalRedisClient") RedisClient client) {
        return client.connect();
    }

    @Bean(name = "signalWriteConnection", destroyMethod = "close")
    public StatefulRedisConnection<String, String> signalWriteConnection(
            @Qualifier("signalRedisClient") RedisClient client) {
        return client.connect();
    }
}
