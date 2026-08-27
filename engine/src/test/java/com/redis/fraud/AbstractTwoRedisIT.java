package com.redis.fraud;

import com.redis.testcontainers.RedisContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.utility.DockerImageName;

/**
 * Base for {@code @SpringBootTest} ITs that need the two-database topology
 * (design doc §7.12, revised — Flex removed): a RAM feature store and a separate
 * RAM signal store, both plain Redis (the signals use core types only, so no
 * modules are required). Shared singleton containers started once for the whole
 * suite, so the two DBs are genuinely distinct during tests.
 */
public abstract class AbstractTwoRedisIT {

    static final RedisContainer FEATURE_DB;
    static final RedisContainer SIGNAL_DB;

    static {
        FEATURE_DB = new RedisContainer(DockerImageName.parse("redis:8"));
        SIGNAL_DB = new RedisContainer(DockerImageName.parse("redis:8"));
        FEATURE_DB.start();
        SIGNAL_DB.start();
    }

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("fraud.feature-store.uri",
                () -> "redis://" + FEATURE_DB.getRedisHost() + ":" + FEATURE_DB.getRedisPort());
        registry.add("fraud.signal-store.uri",
                () -> "redis://" + SIGNAL_DB.getRedisHost() + ":" + SIGNAL_DB.getRedisPort());
    }
}
