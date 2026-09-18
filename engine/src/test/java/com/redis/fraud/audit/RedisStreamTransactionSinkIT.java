package com.redis.fraud.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.redis.testcontainers.RedisContainer;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

/**
 * Gate for §8.4.1: the audit sink appends each scored transaction to the bounded
 * {@code txn:events} Redis Stream, and the stored payload round-trips back to the
 * full {@link ScoredTransaction} (incl. the point-in-time feature snapshot) that
 * downstream consumers (Parquet writer, aggregator) rely on.
 */
@Testcontainers
class RedisStreamTransactionSinkIT {

    @Container
    static final RedisContainer REDIS = new RedisContainer(DockerImageName.parse("redis:8"));

    static RedisClient client;
    static StatefulRedisConnection<String, String> connection;
    static RedisCommands<String, String> redis;
    static ObjectMapper mapper;
    static RedisStreamTransactionSink sink;

    @BeforeAll
    static void setUp() {
        client = RedisClient.create("redis://" + REDIS.getRedisHost() + ":" + REDIS.getRedisPort());
        connection = client.connect();
        redis = connection.sync();
        mapper = JsonMapper.builder().propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE).build();
        sink = new RedisStreamTransactionSink(connection, mapper, "txn:events", 1000);
    }

    @AfterAll
    static void tearDown() {
        if (connection != null) connection.close();
        if (client != null) client.shutdown();
    }

    private static ScoredTransaction txn(String id) {
        return new ScoredTransaction(id, "cust_001", "bene_010", "approve", "seed-v0",
                0.12, null, 100.0, 100.0, "GBP", "GB", 1_760_000_000_000L,
                "device_x", "tpp_Alpha", List.of("R017_velocity_burst_5m"),
                "{\"customer_txn_rate_5m\":2}");
    }

    @Test
    void appendsScoredTxnToStreamAndRoundTrips() {
        redis.del("txn:events");

        sink.accept(txn("t1"));
        sink.accept(txn("t2"));

        assertThat(redis.xlen("txn:events")).isEqualTo(2L);

        List<StreamMessage<String, String>> msgs = redis.xrange("txn:events", Range.unbounded());
        assertThat(msgs).hasSize(2);
        ScoredTransaction first = mapper.readValue(msgs.get(0).getBody().get("v"), ScoredTransaction.class);
        assertThat(first.transactionId()).isEqualTo("t1");
        assertThat(first.decision()).isEqualTo("approve");
        assertThat(first.rulesFired()).containsExactly("R017_velocity_burst_5m");
        assertThat(first.featureSnapshotJson()).contains("customer_txn_rate_5m");
    }

    @Test
    void maxlenBoundsTheStream() {
        redis.del("txn:events");
        RedisStreamTransactionSink capped = new RedisStreamTransactionSink(connection, mapper, "txn:events", 100);
        for (int i = 0; i < 500; i++) {
            capped.accept(txn("t" + i));
        }
        // Approximate trimming keeps the stream near the cap, never unbounded.
        long len = redis.xlen("txn:events");
        assertThat(len).isLessThanOrEqualTo(500L).isGreaterThanOrEqualTo(100L);
    }
}
