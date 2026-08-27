package com.redis.fraud.obs;

import static org.assertj.core.api.Assertions.assertThat;

import com.redis.fraud.AbstractTwoRedisIT;
import com.redis.fraud.testsupport.RedisFixtureLoader;
import io.lettuce.core.api.StatefulRedisConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import tools.jackson.databind.ObjectMapper;

/**
 * Gate M4 (metrics half): scored traffic is reflected in {@code /metrics/live}
 * — processed count, decision mix, and latency percentiles — read from the
 * engine's own Micrometer counters (§10, §13.5). Runs against the two-database
 * topology (§7.12): features read from the feature DB, records persist to the
 * transaction DB.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ObservabilityIT extends AbstractTwoRedisIT {

    static final Path TEST_DATA = Path.of("..", "test_data").toAbsolutePath().normalize();

    @LocalServerPort
    int port;

    @Autowired
    StatefulRedisConnection<String, String> connection;
    @Autowired
    ObjectMapper mapper;

    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void loadFixtures() throws Exception {
        new RedisFixtureLoader(connection, mapper, TEST_DATA).load();
    }

    private void postScore(String json) throws Exception {
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/transactions/score"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
    }

    @Test
    void liveMetricsReflectScoredTraffic() throws Exception {
        // one of each decision, distinct transaction ids
        postScore(request("m_approve", "cust_005", "US", "bene_003", "device_005", 65.0));   // approve
        postScore(request("m_review", "cust_012", "DE", "bene_005", "device_012", 45.0));    // review (velocity)
        postScore(request("m_decline", "cust_038", "SG", "bene_020", "device_038", 250.0));  // decline (blacklist)

        HttpResponse<String> live = http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/metrics/live")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(live.statusCode()).isEqualTo(200);

        LiveSnapshot snapshot = mapper.readValue(live.body(), LiveSnapshot.class);
        assertThat(snapshot.processed()).isGreaterThanOrEqualTo(3);
        assertThat(snapshot.pass()).isGreaterThanOrEqualTo(3);
        assertThat(snapshot.approve()).isGreaterThanOrEqualTo(1);
        assertThat(snapshot.review()).isGreaterThanOrEqualTo(1);
        assertThat(snapshot.decline()).isGreaterThanOrEqualTo(1);
        assertThat(snapshot.p50Ms()).isGreaterThanOrEqualTo(0.0);
        assertThat(snapshot.p99Ms()).isGreaterThanOrEqualTo(0.0);
        assertThat(snapshot.serverTimeMs()).isGreaterThan(0L);
    }

    private static String request(String txnId, String cid, String country, String bene, String device, double amount) {
        return """
                {"transaction_id":"%s","timestamp":"2026-08-10T12:00:00+00:00","customer_id":"%s",
                 "customer_portfolio_country":"%s","receiver_transaction_bank_account_number":"%s",
                 "tpp_name_ud":"tpp_Alpha","device_fingerprint":"%s","amount":%s,"currency":"GBP"}
                """.formatted(txnId, cid, country, bene, device, amount);
    }
}
