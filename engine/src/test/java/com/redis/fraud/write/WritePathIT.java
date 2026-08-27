package com.redis.fraud.write;

import static org.assertj.core.api.Assertions.assertThat;

import com.redis.fraud.api.dto.ScoreRequest;
import com.redis.fraud.redis.KeyBuilder;
import com.redis.fraud.money.FxService;
import com.redis.fraud.scoring.ScoreResult;
import com.redis.testcontainers.RedisContainer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.Optional;
import java.util.concurrent.Executors;
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
 * Gate M3: the post-decision write (§7.9) applies exactly once. A first write
 * updates the customer bucket, ring, pair state, and beneficiary distinct-sender
 * set; replaying the same transaction_id is an idempotent no-op (the co-located
 * {@code SET … NX} guard, §7.8), so no aggregate is double-counted.
 */
@Testcontainers
class WritePathIT {

    @Container
    static final RedisContainer REDIS = new RedisContainer(DockerImageName.parse("redis:8"));

    static RedisClient client;
    static StatefulRedisConnection<String, String> connection;
    static RedisCommands<String, String> redis;
    static WritePath writePath;

    @BeforeAll
    static void setUp() {
        ObjectMapper mapper = JsonMapper.builder().propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE).build();
        client = RedisClient.create("redis://" + REDIS.getRedisHost() + ":" + REDIS.getRedisPort());
        connection = client.connect();
        redis = connection.sync();
        // Split read/write connections in prod; one connection is fine for the test.
        writePath = new WritePath(connection, connection, mapper, new FxService("GBP"),
                Executors.newVirtualThreadPerTaskExecutor());
    }

    @AfterAll
    static void tearDown() {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }

    private static ScoreRequest request(String txnId, String cid, String bene, double amount) {
        return new ScoreRequest(txnId, "2026-08-10T12:00:00+00:00", cid, "US", bene,
                "tpp_Alpha", "device_x", amount, "GBP", null);
    }

    @Test
    void firstWriteApplies_replayIsNoOp() {
        redis.flushdb();
        String cid = "cust_w1";
        String bene = "bene_w1";
        ScoreRequest req = request("txn_w1", cid, bene, 100.0);
        ScoreResult result = new ScoreResult("review", 0.4, null, "seed-v0");

        String bucket = KeyBuilder.customerAgg(cid, "amount", "1h", "2026-08-10T12");
        String pair = KeyBuilder.pairState(cid, bene);
        String ring = KeyBuilder.customerRing(cid);
        String distinct = KeyBuilder.beneDistinctSenders(bene);

        // first write
        assertThat(writePath.writeOnce(req, result)).isTrue();
        assertThat(redis.hget(bucket, "cnt")).isEqualTo("1");
        assertThat(Double.parseDouble(redis.hget(bucket, "sum"))).isEqualTo(100.0);
        assertThat(redis.hget(pair, "cnt_90d")).isEqualTo("1");
        assertThat(redis.llen(ring)).isEqualTo(1L);
        assertThat(redis.scard(distinct)).isEqualTo(1L);
        assertThat(redis.exists(KeyBuilder.decision(cid, "txn_w1"))).isEqualTo(1L);
        // warm (1d) tier also written (#10), and device-sharing set (§5)
        assertThat(redis.hget(KeyBuilder.customerAgg(cid, "amount", "1d", "2026-08-10"), "cnt")).isEqualTo("1");
        assertThat(redis.scard(KeyBuilder.deviceCustomers("device_x"))).isEqualTo(1L);

        // replay of the SAME transaction_id: idempotent no-op, nothing double-counted
        assertThat(writePath.writeOnce(req, result)).isFalse();
        assertThat(redis.hget(bucket, "cnt")).isEqualTo("1");
        assertThat(redis.hget(pair, "cnt_90d")).isEqualTo("1");
        assertThat(redis.llen(ring)).isEqualTo(1L);
        assertThat(redis.scard(distinct)).isEqualTo(1L);
    }

    @Test
    void distinctNewTransactionForSameCustomerCounts() {
        redis.flushdb();
        String cid = "cust_w2";
        String bucket = KeyBuilder.customerAgg(cid, "amount", "1h", "2026-08-10T12");

        assertThat(writePath.writeOnce(request("txn_a", cid, "bene_a", 10.0), new ScoreResult("approve", 0.0, null, "seed-v0"))).isTrue();
        assertThat(writePath.writeOnce(request("txn_b", cid, "bene_b", 20.0), new ScoreResult("approve", 0.0, null, "seed-v0"))).isTrue();

        assertThat(redis.hget(bucket, "cnt")).isEqualTo("2");
        assertThat(Double.parseDouble(redis.hget(bucket, "sum"))).isEqualTo(30.0);
    }

    @Test
    void cachedDecisionReturnsStoredResult() {
        redis.flushdb();
        ScoreRequest req = request("txn_w3", "cust_w3", "bene_w3", 50.0);
        assertThat(writePath.cachedDecision(req)).isEmpty();

        writePath.writeOnce(req, new ScoreResult("decline", 1.0, null, "seed-v0"));

        Optional<ScoreResult> cached = writePath.cachedDecision(req);
        assertThat(cached).isPresent();
        assertThat(cached.get().decision()).isEqualTo("decline");
        assertThat(cached.get().modelVersion()).isEqualTo("seed-v0");
    }
}
