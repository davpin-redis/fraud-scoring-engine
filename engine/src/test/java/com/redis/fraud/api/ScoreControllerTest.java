package com.redis.fraud.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.redis.fraud.fallback.DegradedModeHandler;
import com.redis.fraud.feature.FeatureVector;
import com.redis.fraud.obs.ScoringMetrics;
import com.redis.fraud.rules.RuleEngine;
import com.redis.fraud.rules.RuleOutcome;
import com.redis.fraud.scoring.ScoreResult;
import com.redis.fraud.audit.TransactionWriter;
import com.redis.fraud.scoring.ScoringService;
import com.redis.fraud.signal.SignalWriter;
import com.redis.fraud.write.WritePath;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

// Import WriteConfig so the controller's hotWindowReadExecutor is a real executor.
@WebMvcTest(ScoreController.class)
@Import(com.redis.fraud.write.WriteConfig.class)
class ScoreControllerTest {

    @Autowired
    MockMvc mockMvc;

    @MockitoBean
    DegradedModeHandler degradedModeHandler;
    @MockitoBean
    RuleEngine ruleEngine;
    @MockitoBean
    ScoringService scoringService;
    @MockitoBean
    WritePath writePath;
    @MockitoBean
    SignalWriter signalWriter;
    @MockitoBean
    TransactionWriter transactionWriter;
    @MockitoBean
    ScoringMetrics metrics;

    private static final String REQUEST_JSON = """
            {
              "transaction_id": "req_001",
              "timestamp": "2026-08-10T12:00:00+00:00",
              "customer_id": "cust_005",
              "customer_portfolio_country": "US",
              "receiver_account": "bene_003",
              "tpp_name_ud": "tpp_Alpha",
              "device_fingerprint": "device_005",
              "amount": 65.0,
              "currency": "GBP"
            }
            """;

    @Test
    void mapsPipelineResultToResponseJson() throws Exception {
        when(writePath.cachedDecision(any())).thenReturn(Optional.empty());
        when(degradedModeHandler.assembleFeatures(any()))
                .thenReturn(new DegradedModeHandler.Assembled(
                        new FeatureVector(Map.of("customer_txn_count_1h", 9L)), false));
        when(degradedModeHandler.assembleHotWindow(any()))
                .thenReturn(new DegradedModeHandler.HotWindow(Map.of(), false));
        when(ruleEngine.evaluate(any(), any()))
                .thenReturn(new RuleOutcome(List.of(), false, false, 0.0));
        when(scoringService.decide(any(), anyBoolean(), any()))
                .thenReturn(new ScoreResult("approve", 0.0, null, "seed-v0"));

        mockMvc.perform(post("/v1/transactions/score")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(REQUEST_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transaction_id").value("req_001"))
                .andExpect(jsonPath("$.decision").value("approve"))
                .andExpect(jsonPath("$.model_version").value("seed-v0"))
                .andExpect(jsonPath("$.feature_snapshot.customer_txn_count_1h").value(9))
                .andExpect(jsonPath("$.degraded").value(false));
    }
}
