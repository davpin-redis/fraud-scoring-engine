package com.redis.fraud.fallback;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.redis.fraud.api.dto.ScoreRequest;
import com.redis.fraud.feature.FeatureService;
import com.redis.fraud.feature.FeatureVector;
import com.redis.fraud.rules.RuleOutcome;
import com.redis.fraud.scoring.ScoreResult;
import com.redis.fraud.scoring.ScoringService;
import com.redis.fraud.signal.SignalNames;
import com.redis.fraud.signal.SignalReader;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** §3.5 graceful degradation: a feature-store failure yields a degraded, conservative decision, not an error. */
class DegradedModeHandlerTest {

    private static ScoreRequest req() {
        return new ScoreRequest("t1", "2026-08-10T12:00:00+00:00", "cust_1", "US",
                "bene_1", "tpp_Alpha", "device_1", 100.0, "GBP", null);
    }

    @Test
    void featureFailureDegradesInsteadOfThrowing() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.assemble(any())).thenThrow(new RuntimeException("redis down"));

        DegradedModeHandler handler = new DegradedModeHandler(featureService, mock(SignalReader.class));
        DegradedModeHandler.Assembled assembled = handler.assembleFeatures(req());

        assertThat(assembled.degraded()).isTrue();
        assertThat(assembled.features().values()).isEmpty();
    }

    @Test
    void healthyAssemblyIsNotDegraded() {
        FeatureService featureService = mock(FeatureService.class);
        when(featureService.assemble(any())).thenReturn(new FeatureVector(Map.of("customer_txn_count_1h", 3L)));

        DegradedModeHandler handler = new DegradedModeHandler(featureService, mock(SignalReader.class));
        DegradedModeHandler.Assembled assembled = handler.assembleFeatures(req());

        assertThat(assembled.degraded()).isFalse();
        assertThat(assembled.features().asLong("customer_txn_count_1h")).isEqualTo(3L);
    }

    @Test
    void hotWindowFailureDegradesToEmptySignalsWithoutThrowing() {
        FeatureService featureService = mock(FeatureService.class);
        SignalReader reader = mock(SignalReader.class);
        when(reader.read(any())).thenThrow(new RuntimeException("signal store timeout"));

        DegradedModeHandler handler = new DegradedModeHandler(featureService, reader);
        DegradedModeHandler.HotWindow hotWindow = handler.assembleHotWindow(req());

        assertThat(hotWindow.degraded()).isTrue();
        assertThat(hotWindow.features().get(SignalNames.CUSTOMER_DECLINES_90D)).isEqualTo(0L);
        assertThat(hotWindow.features().get(SignalNames.CUSTOMER_DISTINCT_BENE_90D)).isEqualTo(0L);
    }

    @Test
    void mergeAddsHotWindowFeaturesToVector() {
        FeatureVector base = new FeatureVector(Map.of("customer_txn_count_1h", 3L));
        FeatureVector merged = DegradedModeHandler.merge(base, Map.of(
                SignalNames.CUSTOMER_DECLINES_90D, 2L, SignalNames.CUSTOMER_DISTINCT_BENE_90D, 17L));

        assertThat(merged.asLong("customer_txn_count_1h")).isEqualTo(3L);
        assertThat(merged.asLong("customer_declines_90d")).isEqualTo(2L);
        assertThat(merged.asLong("customer_distinct_bene_90d")).isEqualTo(17L);
    }

    @Test
    void degradedTransactionIsRoutedToReview() {
        ScoringService scoring = new ScoringService();
        RuleOutcome noRules = new RuleOutcome(List.of(), false, false, 0.0);

        ScoreResult healthy = scoring.decide(noRules, false);
        ScoreResult degraded = scoring.decide(noRules, true);

        assertThat(healthy.decision()).isEqualTo("approve");
        assertThat(degraded.decision()).isEqualTo("review");
        assertThat(degraded.modelVersion()).contains("degraded");
    }
}
