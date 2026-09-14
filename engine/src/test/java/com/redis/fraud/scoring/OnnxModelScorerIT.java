package com.redis.fraud.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import com.redis.fraud.feature.FeatureVector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * Gate for §8.4.3 (Phase 4, B4): the engine scores the Track-A feedback model in-process
 * via ONNX Runtime — a fraud-shaped vector scores higher than a clean one, probabilities
 * are in [0,1], the version comes from the manifest, and a {@code reload} (the hot-swap
 * behind {@code model:invalidate}) picks up a different artifact.
 */
class OnnxModelScorerIT {

    static final ObjectMapper MAPPER = JsonMapper.builder().build();
    static final String STRONG = "src/test/resources/model/fraud-model.onnx";
    static final String WEAK = "src/test/resources/model/fraud-model-weak.onnx";

    private static FeatureVector fraudVector() {
        Map<String, Object> m = new HashMap<>();
        m.put("velocity_ratio_1h", 6.0);
        m.put("device_surge", 8.0);
        m.put("bene_surge", 8.0);
        m.put("bene_distinct_senders_90d", 40.0);
        m.put("new_payee", 1.0);
        m.put("device_distinct_customers", 20.0);
        m.put("amount_zscore_90d", 3.0);
        m.put("customer_txn_rate_5m", 6.0);
        m.put("amount_base", 800.0);
        return new FeatureVector(m);
    }

    private static FeatureVector cleanVector() {
        Map<String, Object> m = new HashMap<>();
        m.put("velocity_ratio_1h", 1.0);
        m.put("device_surge", 1.0);
        m.put("bene_surge", 1.0);
        m.put("bene_distinct_senders_90d", 3.0);
        m.put("new_payee", 0.0);
        m.put("device_distinct_customers", 1.0);
        m.put("amount_zscore_90d", 0.0);
        m.put("customer_txn_rate_5m", 0.0);
        m.put("amount_base", 100.0);
        m.put("account_age_days", 1500.0);
        m.put("local_hour", 12.0);
        return new FeatureVector(m);
    }

    @Test
    void scoresFraudHigherThanCleanAndReadsVersion() {
        OnnxModelScorer scorer = new OnnxModelScorer(STRONG, MAPPER);
        double fraud = scorer.fraudProbability(fraudVector());
        double clean = scorer.fraudProbability(cleanVector());
        assertThat(fraud).isBetween(0.0, 1.0);
        assertThat(clean).isBetween(0.0, 1.0);
        assertThat(fraud).isGreaterThan(clean);
        assertThat(scorer.version()).isEqualTo("fb-v1");
    }

    @Test
    void missingFeaturesDefaultToZeroAndStillScore() {
        OnnxModelScorer scorer = new OnnxModelScorer(STRONG, MAPPER);
        double p = scorer.fraudProbability(new FeatureVector(Map.of()));  // all features absent -> 0
        assertThat(p).isBetween(0.0, 1.0);
    }

    @Test
    void reloadSwapsArtifactAndVersion() {
        OnnxModelScorer scorer = new OnnxModelScorer(STRONG, MAPPER);
        assertThat(scorer.version()).isEqualTo("fb-v1");
        scorer.reload(WEAK);                                   // the hot-swap behind model:invalidate
        assertThat(scorer.version()).isEqualTo("fb-v0");
        assertThat(scorer.fraudProbability(fraudVector())).isBetween(0.0, 1.0);
    }

    @Test
    void manifestArtifactsExist() throws Exception {
        assertThat(Files.exists(Path.of(STRONG))).isTrue();
        assertThat(Files.readString(Path.of("src/test/resources/model/fraud-model.manifest.json")))
                .contains("fb-v1").contains("velocity_ratio_1h");
    }
}
