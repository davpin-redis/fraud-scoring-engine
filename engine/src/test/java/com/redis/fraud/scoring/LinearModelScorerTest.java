package com.redis.fraud.scoring;

import static org.assertj.core.api.Assertions.assertThat;

import com.redis.fraud.feature.FeatureVector;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

/** The embedded model loads its trained artifact and produces a probability that ranks risk sensibly. */
class LinearModelScorerTest {

    private final ObjectMapper mapper = JsonMapper.builder()
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE).build();
    private final LinearModelScorer scorer = new LinearModelScorer(mapper);

    private static FeatureVector fv(long count1h, long distinct24h, long pair90d,
                                    double amount, int hour, String custCountry, String beneCountry) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("customer_txn_count_1h", count1h);
        m.put("bene_distinct_senders_24h", distinct24h);
        m.put("pair_txn_count_90d", pair90d);
        m.put("amount_base", amount);
        m.put("local_hour", hour);
        m.put("customer_portfolio_country", custCountry);
        m.put("beneficiary_country", beneCountry);
        return new FeatureVector(m);
    }

    @Test
    void loadsArtifactAndReportsVersion() {
        assertThat(scorer.version()).isEqualTo("logreg-v1");
    }

    @Test
    void probabilitiesAreBoundedAndRankRisk() {
        double clean = scorer.fraudProbability(fv(0, 1, 5, 65.0, 12, "US", "US"));
        double risky = scorer.fraudProbability(fv(9, 14, 0, 45.0, 3, "DE", "US"));

        assertThat(clean).isBetween(0.0, 1.0);
        assertThat(risky).isBetween(0.0, 1.0);
        // velocity burst + high distinct-senders + cross-border should score riskier
        assertThat(risky).isGreaterThan(clean);
    }
}
