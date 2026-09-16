package com.redis.fraud;

import static org.assertj.core.api.Assertions.assertThat;

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
 * Proves the {@code fraud.model.enabled=true} toggle wires the embedded model
 * into the live scoring path: a scored response carries the model's version
 * ("logreg-v1") rather than "seed-v0" (design doc §8.2). With the default
 * (false), every other IT observes the seed-v0 bypass. Runs on the two-database
 * topology (§7.12).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "fraud.model.enabled=true")
class ModelToggleIT extends AbstractTwoRedisIT {

    static final Path TEST_DATA = Path.of("..", "test_data").toAbsolutePath().normalize();

    @LocalServerPort
    int port;
    @Autowired
    StatefulRedisConnection<String, String> connection;
    @Autowired
    ObjectMapper mapper;

    @BeforeEach
    void loadFixtures() throws Exception {
        new RedisFixtureLoader(connection, mapper, TEST_DATA).load();
    }

    @Test
    void enabledModelStampsItsVersionOnScoredResponse() throws Exception {
        String body = """
                {"transaction_id":"toggle_1","timestamp":"2026-08-10T12:00:00+00:00","customer_id":"cust_005",
                 "customer_portfolio_country":"US","receiver_account":"bene_003",
                 "tpp_name_ud":"tpp_Alpha","device_fingerprint":"device_005","amount":65.0,"currency":"GBP"}
                """;
        HttpResponse<String> resp = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/transactions/score"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(mapper.readTree(resp.body()).get("model_version").asString()).isEqualTo("logreg-v1");
    }
}
