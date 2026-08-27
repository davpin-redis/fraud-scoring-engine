package com.redis.fraud;

import static org.assertj.core.api.Assertions.assertThat;

import com.redis.fraud.api.dto.ScoreRequest;
import com.redis.fraud.config.RedisConfigStore;
import com.redis.fraud.feature.FeatureService;
import com.redis.fraud.feature.FeatureVector;
import com.redis.fraud.money.FxService;
import com.redis.fraud.rules.ConditionEvaluator;
import com.redis.fraud.rules.RuleEngine;
import com.redis.fraud.rules.RuleOutcome;
import com.redis.fraud.scoring.ScoreResult;
import com.redis.fraud.scoring.ScoringService;
import com.redis.fraud.testsupport.RedisFixtureLoader;
import com.redis.testcontainers.RedisContainer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

/**
 * Gate M2: every request in {@code sample_test_requests.json} produces its
 * expected decision through the full pipeline (features → rules → decision),
 * run against a Redis loaded exactly as {@code load_redis.py} loads it. This is
 * the JUnit equivalent of {@code run_sample_requests.py}.
 */
@Testcontainers
class ScoringPipelineIT {

    @Container
    static final RedisContainer REDIS = new RedisContainer(DockerImageName.parse("redis:8"));

    static final Path TEST_DATA = Path.of("..", "test_data").toAbsolutePath().normalize();

    static ObjectMapper mapper;
    static RedisClient client;
    static StatefulRedisConnection<String, String> connection;
    static FeatureService featureService;
    static RuleEngine ruleEngine;
    static ScoringService scoringService;

    @BeforeAll
    static void setUp() throws Exception {
        mapper = JsonMapper.builder().propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE).build();

        client = RedisClient.create("redis://" + REDIS.getRedisHost() + ":" + REDIS.getRedisPort());
        connection = client.connect();
        new RedisFixtureLoader(connection, mapper, TEST_DATA).load();

        RedisConfigStore configStore = new RedisConfigStore(connection, mapper);
        configStore.refresh();
        featureService = new FeatureService(connection, configStore, new FxService("GBP"), mapper);
        ruleEngine = new RuleEngine(configStore, new ConditionEvaluator());
        scoringService = new ScoringService();
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

    private String decide(ScoreRequest request) {
        FeatureVector features = featureService.assemble(request);
        RuleOutcome outcome = ruleEngine.evaluate(request, features);
        ScoreResult result = scoringService.decide(outcome);
        return result.decision();
    }

    @Test
    void allSampleRequestsReturnTheirExpectedDecision() throws Exception {
        JsonNode samples = mapper.readTree(
                Files.readString(TEST_DATA.resolve("transactions").resolve("sample_test_requests.json")));

        List<String> mismatches = new ArrayList<>();
        int total = 0;
        for (JsonNode sample : samples) {
            total++;
            String label = sample.get("label").asString();
            String expected = sample.get("expected_decision").asString();
            ScoreRequest request = mapper.treeToValue(sample.get("request"), ScoreRequest.class);

            String actual = decide(request);
            if (!expected.equals(actual)) {
                mismatches.add("%s: expected=%s actual=%s".formatted(label, expected, actual));
            }
        }

        assertThat(total).isEqualTo(11);
        assertThat(mismatches)
                .as("sample requests with wrong decision")
                .isEmpty();
    }
}
