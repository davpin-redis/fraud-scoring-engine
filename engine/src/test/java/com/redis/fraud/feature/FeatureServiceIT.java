package com.redis.fraud.feature;

import static org.assertj.core.api.Assertions.assertThat;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;
import com.redis.fraud.api.dto.ScoreRequest;
import com.redis.fraud.config.RedisConfigStore;
import com.redis.fraud.money.FxService;
import com.redis.fraud.testsupport.RedisFixtureLoader;
import com.redis.testcontainers.RedisContainer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Gate M1: the six feature readers, run against a Redis loaded exactly as
 * {@code load_redis.py} would load it, return the values the fixtures encode
 * (design doc §6.2, §7.4–7.5; README scenario table).
 */
@Testcontainers
class FeatureServiceIT {

    @Container
    static final RedisContainer REDIS = new RedisContainer(DockerImageName.parse("redis:8"));

    static final Path TEST_DATA = Path.of("..", "test_data").toAbsolutePath().normalize();

    static RedisClient client;
    static StatefulRedisConnection<String, String> connection;
    static RedisConfigStore configStore;
    static FeatureService featureService;

    @BeforeAll
    static void setUp() throws Exception {
        ObjectMapper mapper = JsonMapper.builder()
                .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .build();

        String uri = "redis://" + REDIS.getRedisHost() + ":" + REDIS.getRedisPort();
        client = RedisClient.create(uri);
        connection = client.connect();

        new RedisFixtureLoader(connection, mapper, TEST_DATA).load();

        configStore = new RedisConfigStore(connection, mapper);
        configStore.refresh();

        // reference/master data (profiles, IP-geo) is loaded into Redis by RedisFixtureLoader
        featureService = new FeatureService(connection, configStore, new FxService("GBP"), mapper);
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

    private static ScoreRequest req(String txnId, String cid, String country, String bene,
                                    String tpp, String device, double amount, String ts) {
        return new ScoreRequest(txnId, ts, cid, country, bene, tpp, device, amount, "GBP", null);
    }

    private static final String NOON = "2026-08-10T12:00:00+00:00";

    @Test
    void configLoadsAllWindowsMetricsRules() {
        assertThat(configStore.windows()).hasSize(2);
        assertThat(configStore.metrics()).hasSize(6);
        assertThat(configStore.rules()).hasSize(26); // R001–R011 + R012/R013 + R014–R018 + TimeSeries behavioural R019–R026
    }

    @Test
    void velocityBurstCustomerHasNineTxnsInLastHour() {
        FeatureVector fv = featureService.assemble(
                req("t1", "cust_012", "DE", "bene_005", "tpp_Alpha", "device_012", 45.0, NOON));

        assertThat(fv.asLong("customer_txn_count_1h")).isEqualTo(9L);
        // positional 3rd-previous payment exists and falls within the 1h sub-window
        assertThat(fv.get("c_snd_3rd_lst_pymt_dt_1h_rt")).isNotNull();
        // stddev is defined for a customer with activity in the last 24h
        assertThat(fv.asDouble("customerid_stddev_sendertransactionamo_1d")).isNotNull();
    }

    @Test
    void muleBeneficiaryHasFourteenDistinctSenders() {
        FeatureVector fv = featureService.assemble(
                req("t2", "cust_020", "US", "bene_018", "tpp_Beta", "device_020", 95.0, NOON));

        assertThat(fv.asLong("bene_distinct_senders_24h")).isEqualTo(14L);
    }

    @Test
    void establishedPairHasHistoryAndKnownBeneficiaryCountry() {
        FeatureVector fv = featureService.assemble(
                req("t3", "cust_030", "DE", "bene_017", "tpp_Delta", "device_030", 4800.0, NOON));

        assertThat(fv.asLong("pair_txn_count_90d")).isEqualTo(3L);
        assertThat(fv.get("beneficiary_country")).isNotNull();
    }

    @Test
    void newPayeePairHasNoHistory() {
        FeatureVector fv = featureService.assemble(
                req("t4", "cust_020", "US", "bene_015", "tpp_Gamma", "device_020", 4800.0, NOON));

        assertThat(fv.asLong("pair_txn_count_90d")).isEqualTo(0L);
    }

    @Test
    void deviceSharedAcrossAccountsIsDetected() {
        FeatureVector fv = featureService.assemble(
                req("t9", "cust_010", "US", "bene_003", "tpp_Alpha", "device_shared_01", 50.0, NOON));
        // cust_010, cust_027, cust_033 all transact on device_shared_01 in the backfill
        assertThat(fv.asLong("device_distinct_customers")).isEqualTo(3L);
    }

    @Test
    void membershipAndBlacklistFlagsResolve() {
        assertThat(featureService.assemble(
                req("t5", "cust_046", "GB", "bene_002", "tpp_Beta", "device_046", 40.0, NOON))
                .asBoolean("vip_customer")).isTrue();

        assertThat(featureService.assemble(
                req("t6", "cust_049", "BR", "bene_006", "tpp_Alpha", "device_049", 150.0, NOON))
                .asBoolean("watchlist")).isTrue();

        assertThat(featureService.assemble(
                req("t7", "cust_038", "SG", "bene_020", "tpp_Gamma", "device_038", 250.0, NOON))
                .asBoolean("bl_account")).isTrue();

        assertThat(featureService.assemble(
                req("t8", "cust_035", "US", "bene_004", "tpp_Delta", "device_099", 90.0, NOON))
                .asBoolean("bl_device")).isTrue();
    }
}
