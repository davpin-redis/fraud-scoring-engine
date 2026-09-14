package com.redis.fraud.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.redis.testcontainers.RedisContainer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * Gate for §8.3 (Phase 4, B5): the decision-band thresholds live in {@code cfg:bands} and
 * hot-reload via {@code cfg:invalidate} — defaults apply when unset, and an operator change
 * takes effect on the next reload with no redeploy (threshold recalibration in the loop).
 */
@Testcontainers
class BandsReloadIT {

    @Container
    static final RedisContainer REDIS = new RedisContainer(DockerImageName.parse("redis:8"));

    static RedisClient client;
    static StatefulRedisConnection<String, String> connection;
    static RedisCommands<String, String> redis;
    static final ObjectMapper MAPPER = JsonMapper.builder().build();

    @BeforeAll
    static void setUp() {
        client = RedisClient.create("redis://" + REDIS.getRedisHost() + ":" + REDIS.getRedisPort());
        connection = client.connect();
        redis = connection.sync();
    }

    @AfterAll
    static void tearDown() {
        if (connection != null) connection.close();
        if (client != null) client.shutdown();
    }

    @Test
    void defaultBandsWhenUnset() {
        redis.del("cfg:bands");
        RedisConfigStore store = new RedisConfigStore(connection, MAPPER);
        assertThat(store.bands().reviewThreshold()).isEqualTo(0.30);
        assertThat(store.bands().declineThreshold()).isEqualTo(0.70);
        assertThat(store.bands().decide(0.5)).isEqualTo("review");
    }

    @Test
    void bandsHotReloadOnInvalidation() {
        redis.del("cfg:bands");
        RedisConfigStore store = new RedisConfigStore(connection, MAPPER);
        assertThat(store.bands().decide(0.95)).isEqualTo("decline");

        // operator tightens the decline threshold, then signals a reload
        redis.hset("cfg:bands", Map.of("review", "0.50", "decline", "0.98"));
        store.onInvalidation();

        assertThat(store.bands().reviewThreshold()).isEqualTo(0.50);
        assertThat(store.bands().declineThreshold()).isEqualTo(0.98);
        assertThat(store.bands().decide(0.95)).isEqualTo("review");   // 0.95 was decline, now review
    }
}
