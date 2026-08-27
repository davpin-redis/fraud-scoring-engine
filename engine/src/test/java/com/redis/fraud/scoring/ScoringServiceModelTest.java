package com.redis.fraud.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import com.redis.fraud.feature.FeatureVector;
import com.redis.fraud.rules.RuleOutcome;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

/** The {@code fraud.model.enabled} toggle: model is called when present, bypassed when absent. */
class ScoringServiceModelTest {

    private static final RuleOutcome NO_RULES = new RuleOutcome(List.of(), false, false, 0.0);

    private static FeatureVector riskyFeatures() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("customer_txn_count_1h", 9L);
        m.put("bene_distinct_senders_24h", 14L);
        m.put("pair_txn_count_90d", 0L);
        m.put("amount_base", 45.0);
        m.put("local_hour", 3);
        m.put("customer_portfolio_country", "DE");
        m.put("beneficiary_country", "US");
        return new FeatureVector(m);
    }

    private static ModelScorer realModel() {
        return new LinearModelScorer(JsonMapper.builder()
                .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE).build());
    }

    @Test
    void modelDisabledBypassesModelAndUsesSeedV0() {
        ScoringService scoring = new ScoringService(); // no model
        assertThat(scoring.modelEnabled()).isFalse();

        ScoreResult result = scoring.decide(NO_RULES, false, riskyFeatures());
        assertThat(result.modelVersion()).isEqualTo("rules-v1");
        assertThat(result.decision()).isEqualTo("approve"); // no rules fired -> score 0 -> approve
    }

    @Test
    void modelEnabledCallsModelAndStampsItsVersion() {
        ScoringService scoring = new ScoringService(realModel());
        assertThat(scoring.modelEnabled()).isTrue();

        ScoreResult result = scoring.decide(NO_RULES, false, riskyFeatures());
        assertThat(result.modelVersion()).isEqualTo("logreg-v1");
        assertThat(result.finalScore()).isBetween(0.0, 1.0);
        assertThat(result.decision()).isIn("approve", "review", "decline");
    }

    @Test
    void hardBlockStillWinsRegardlessOfModel() {
        ScoringService scoring = new ScoringService(realModel());
        RuleOutcome blocked = new RuleOutcome(List.of(), true, false, 0.0);

        assertThat(scoring.decide(blocked, false, riskyFeatures()).decision()).isEqualTo("decline");
    }
}
