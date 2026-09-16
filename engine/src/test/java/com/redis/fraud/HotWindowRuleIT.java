package com.redis.fraud;

import static org.assertj.core.api.Assertions.assertThat;

import com.redis.fraud.signal.SignalKeys;
import com.redis.fraud.testsupport.RedisFixtureLoader;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Proves the two hot-window rules (§8.1, revised — Flex removed): R012 (repeat
 * declines) and R013 (beneficiary fan-out), now computed from the streaming
 * signal store — a per-customer declines counter and a HyperLogLog of distinct
 * beneficiaries. This IT seeds the current rotating month bucket directly, then
 * scores a fresh transaction for each customer and asserts the hot-window rule fires.
 *
 * <p>Uses customer ids unique to this test so seeded signals can't perturb the
 * other ITs that share the singleton signal-store container.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HotWindowRuleIT extends AbstractTwoRedisIT {

    static final Path TEST_DATA = Path.of("..", "test_data").toAbsolutePath().normalize();
    static final Instant REF = OffsetDateTime.parse("2026-08-17T12:00:00+00:00").toInstant();
    static final String REF_TS = "2026-08-17T12:00:00+00:00";

    static final String CID_DECLINES = "cust_hw_declines";
    static final String CID_FANOUT = "cust_hw_fanout";
    static final String CID_CLEAN = "cust_hw_clean";
    static final String CID_DORMANT = "cust_hw_dormant";
    static final String CID_DEVICE = "cust_hw_device";
    static final String CID_BENE = "cust_hw_bene";
    static final String DEVICE_SURGE = "device_r025";     // dedicated (not the shared device_hw)
    static final String BENE_SURGE = "bene_surge_r026";

    @LocalServerPort
    int port;
    @Autowired
    StatefulRedisConnection<String, String> connection;
    @Autowired
    @Qualifier("signalWriteConnection")
    StatefulRedisConnection<String, String> signalConnection;
    @Autowired
    ObjectMapper mapper;

    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void setUp() throws Exception {
        new RedisFixtureLoader(connection, mapper, TEST_DATA).load();

        RedisCommands<String, String> sig = signalConnection.sync();
        // Reset this test's signal keys (the container is shared across test methods).
        for (String cid : List.of(CID_DECLINES, CID_FANOUT, CID_CLEAN)) {
            sig.del(SignalKeys.declinesMonth(cid, REF), SignalKeys.benesMonth(cid, REF));
        }
        // R012: four prior declines inside the 90-day window (>= 3).
        for (int i = 0; i < 4; i++) {
            sig.incr(SignalKeys.declinesMonth(CID_DECLINES, REF));
        }
        // R013: 16 distinct beneficiaries inside the window (> 15).
        for (int i = 1; i <= 16; i++) {
            sig.pfadd(SignalKeys.benesMonth(CID_FANOUT, REF), String.format("bene_%03d", i));
        }
        // Control: a clean history — one beneficiary, no declines.
        sig.pfadd(SignalKeys.benesMonth(CID_CLEAN, REF), "bene_002");

        // R024: last transaction ~90 days ago and nothing since → dormant reactivation.
        sig.del(SignalKeys.lastEventTs(CID_DORMANT));
        sig.set(SignalKeys.lastEventTs(CID_DORMANT),
                Long.toString(REF.minus(90, ChronoUnit.DAYS).toEpochMilli()));

        // R025 (device) / R026 (payee): a steady baseline (~1 event per 5-min bucket over the
        // last ~5.5h) plus a spike of many events in the current 5-min bucket → surge ratio > 5.
        sig.del(SignalKeys.deviceSurge5m(DEVICE_SURGE), SignalKeys.beneSurge5m(BENE_SURGE));
        seedSurge(sig, SignalKeys.deviceSurge5m(DEVICE_SURGE));
        seedSurge(sig, SignalKeys.beneSurge5m(BENE_SURGE));
    }

    /** 13 baseline 5-min buckets (1 event each, 10-min apart) + a 20-event spike in the current bucket. */
    private void seedSurge(RedisCommands<String, String> sig, String key) {
        long now = REF.toEpochMilli();
        for (int k = 1; k <= 13; k++) {
            long bucket = SignalKeys.bucketStart(now - k * 10L * 60 * 1000, SignalKeys.SURGE_BUCKET_MS);
            sig.hset(key, Long.toString(bucket), "1");
        }
        sig.hset(key, Long.toString(SignalKeys.bucketStart(now, SignalKeys.SURGE_BUCKET_MS)), "20");
    }

    @Test
    void repeatDeclinesTripsR012() throws Exception {
        JsonNode resp = score("hw_r012", CID_DECLINES, "bene_050", 75.0);
        assertThat(resp.get("decision").asString()).isEqualTo("review");
        assertThat(ruleIds(resp)).contains("R012_repeat_declines_90d");
    }

    @Test
    void beneficiaryFanOutTripsR013() throws Exception {
        JsonNode resp = score("hw_r013", CID_FANOUT, "bene_050", 75.0);
        assertThat(resp.get("decision").asString()).isEqualTo("review");
        assertThat(ruleIds(resp)).contains("R013_beneficiary_fanout_90d");
    }

    @Test
    void dormantReactivationTripsR024() throws Exception {
        JsonNode resp = score("hw_r024", CID_DORMANT, "bene_050", 75.0);
        assertThat(resp.get("feature_snapshot").get("dormancy_days").asDouble()).isGreaterThan(60.0);
        assertThat(ruleIds(resp)).contains("R024_dormant_reactivation");
    }

    @Test
    void deviceSurgeTripsR025() throws Exception {
        JsonNode resp = score("hw_r025", CID_DEVICE, "bene_050", 75.0, DEVICE_SURGE);
        assertThat(resp.get("feature_snapshot").get("device_surge").asDouble()).isGreaterThan(5.0);
        assertThat(ruleIds(resp)).contains("R025_device_surge");
    }

    @Test
    void beneInboundSurgeTripsR026() throws Exception {
        JsonNode resp = score("hw_r026", CID_BENE, BENE_SURGE, 75.0);
        assertThat(resp.get("feature_snapshot").get("bene_surge").asDouble()).isGreaterThan(5.0);
        assertThat(ruleIds(resp)).contains("R026_bene_inbound_surge");
    }

    @Test
    void cleanHistoryTripsNeitherHotWindowRule() throws Exception {
        JsonNode resp = score("hw_clean", CID_CLEAN, "bene_002", 75.0);
        assertThat(ruleIds(resp))
                .doesNotContain("R012_repeat_declines_90d", "R013_beneficiary_fanout_90d");
        assertThat(resp.get("decision").asString()).isEqualTo("approve");
        // The two hot-window features are surfaced on the response for observability.
        assertThat(resp.get("feature_snapshot").get("customer_declines_90d").asLong()).isEqualTo(0L);
        assertThat(resp.get("feature_snapshot").get("customer_distinct_bene_90d").asLong()).isEqualTo(1L);
    }

    @Test
    void featureSnapshotCarriesTheModelFeatureContract() throws Exception {
        // The 14 Track-A model features (pipeline/generator.py FEATURES) must all be present in
        // the persisted feature_snapshot so retrain_loop can rebuild input vectors from it (§8.4.3).
        // cust_001 is a seeded fixture customer (has a profile → account_age_days is known);
        // bene_zzz_new is a first-time payee → new_payee = 1.
        JsonNode snap = score("hw_feat", "cust_001", "bene_zzz_new", 1200.0)
                .get("feature_snapshot");
        for (String f : List.of(
                "velocity_ratio_1h", "customer_txn_rate_5m", "customer_declines_90d",
                "customer_distinct_bene_90d", "bene_distinct_senders_90d", "amount_zscore_90d",
                "device_distinct_customers", "dormancy_days", "device_surge", "bene_surge",
                "new_payee", "amount_base", "local_hour", "account_age_days")) {
            assertThat(snap.has(f)).as("feature_snapshot missing '%s'", f).isTrue();
        }
        assertThat(snap.get("new_payee").asDouble()).isEqualTo(1.0);   // first payment to this payee
    }

    private List<String> ruleIds(JsonNode resp) {
        List<String> ids = new ArrayList<>();
        resp.get("rules_fired").forEach(n -> ids.add(n.asString()));
        return ids;
    }

    private JsonNode score(String txnId, String cid, String bene, double amount) throws Exception {
        return score(txnId, cid, bene, amount, "device_hw");
    }

    private JsonNode score(String txnId, String cid, String bene, double amount, String device) throws Exception {
        String body = """
                {"transaction_id":"%s","timestamp":"%s","customer_id":"%s",
                 "customer_portfolio_country":"GB","receiver_account":"%s",
                 "tpp_name_ud":"tpp_Alpha","device_fingerprint":"%s","amount":%s,"currency":"GBP"}
                """.formatted(txnId, REF_TS, cid, bene, device, amount);
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/transactions/score"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
        return mapper.readTree(resp.body());
    }
}
