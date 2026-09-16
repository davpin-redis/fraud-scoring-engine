package com.redis.fraud.api;

import com.redis.fraud.api.dto.ScoreRequest;
import com.redis.fraud.api.dto.ScoreResponse;
import com.redis.fraud.audit.ScoredTransaction;
import com.redis.fraud.audit.TransactionWriter;
import com.redis.fraud.fallback.DegradedModeHandler;
import com.redis.fraud.feature.FeatureVector;
import com.redis.fraud.obs.ScoringMetrics;
import com.redis.fraud.rules.RuleEngine;
import com.redis.fraud.rules.RuleOutcome;
import com.redis.fraud.scoring.ScoreResult;
import com.redis.fraud.scoring.ScoringService;
import com.redis.fraud.signal.SignalWriter;
import com.redis.fraud.write.WritePath;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.ObjectMapper;

/**
 * Synchronous scoring endpoint (design doc §3.2): assemble features (§7.10) with
 * graceful degradation (§3.5), evaluate rules (§8.1), blend into a decision
 * (§8.3), persist off the response path (§3.2 step 8), and return the decision.
 * Instrumented for the §10/§13.5 metrics. A retried transaction short-circuits
 * to its cached decision (§7.8).
 */
@RestController
@RequestMapping("/v1/transactions")
public class ScoreController {

    private final DegradedModeHandler degradedModeHandler;
    private final RuleEngine ruleEngine;
    private final ScoringService scoringService;
    private final WritePath writePath;
    private final SignalWriter signalWriter;
    private final TransactionWriter transactionWriter;
    private final ScoringMetrics metrics;
    private final ObjectMapper objectMapper;
    private final ExecutorService hotWindowReadExecutor;

    public ScoreController(DegradedModeHandler degradedModeHandler, RuleEngine ruleEngine,
                           ScoringService scoringService, WritePath writePath, SignalWriter signalWriter,
                           TransactionWriter transactionWriter, ScoringMetrics metrics,
                           ObjectMapper objectMapper,
                           @Qualifier("hotWindowReadExecutor") ExecutorService hotWindowReadExecutor) {
        this.degradedModeHandler = degradedModeHandler;
        this.ruleEngine = ruleEngine;
        this.scoringService = scoringService;
        this.writePath = writePath;
        this.signalWriter = signalWriter;
        this.transactionWriter = transactionWriter;
        this.metrics = metrics;
        this.objectMapper = objectMapper;
        this.hotWindowReadExecutor = hotWindowReadExecutor;
    }

    @PostMapping("/score")
    public ScoreResponse score(@RequestBody ScoreRequest request) {
        long start = System.nanoTime();
        try {
            // Idempotent retry short-circuit (§7.8); tolerate a cache-read failure as a miss.
            Optional<ScoreResult> cached;
            try {
                cached = writePath.cachedDecision(request);
            } catch (RuntimeException e) {
                cached = Optional.empty();
            }
            if (cached.isPresent()) {
                ScoreResult c = cached.get();
                metrics.recordSuccess(c.decision(), false, List.of(), System.nanoTime() - start);
                return new ScoreResponse(request.transactionId(), c.decision(), c.finalScore(),
                        c.modelVersion(), List.of(), Map.of(), false);
            }

            // The 90-day hot-window read (transaction store, §3.3) runs concurrently
            // with the feature-store batch so its Query-Engine round trip overlaps.
            CompletableFuture<DegradedModeHandler.HotWindow> hotWindowFuture =
                    CompletableFuture.supplyAsync(() -> degradedModeHandler.assembleHotWindow(request),
                            hotWindowReadExecutor);

            DegradedModeHandler.Assembled assembled = degradedModeHandler.assembleFeatures(request);
            DegradedModeHandler.HotWindow hotWindow = hotWindowFuture.join();
            FeatureVector features = DegradedModeHandler.merge(assembled.features(), hotWindow.features());

            RuleOutcome outcome = ruleEngine.evaluate(request, features);
            ScoreResult result = scoringService.decide(outcome, assembled.degraded(), features);

            writePath.writeAsync(request, result);
            signalWriter.updateAsync(request, result);
            transactionWriter.persistAsync(toScoredTxn(request, result, outcome, features));

            metrics.recordSuccess(result.decision(), assembled.degraded(), outcome.firedRuleIds(),
                    System.nanoTime() - start);

            return new ScoreResponse(
                    request.transactionId(),
                    result.decision(),
                    result.finalScore(),
                    result.modelVersion(),
                    outcome.firedRuleIds(),
                    features.values(),
                    assembled.degraded());
        } catch (RuntimeException e) {
            metrics.recordFailure();
            throw e;
        }
    }

    private ScoredTransaction toScoredTxn(ScoreRequest request, ScoreResult result,
                                          RuleOutcome outcome, FeatureVector features) {
        Double amountBase = features.asDouble("amount_base");
        return new ScoredTransaction(
                request.transactionId(),
                request.customerId(),
                request.receiverAccount(),
                result.decision(),
                result.modelVersion(),
                result.finalScore(),
                result.modelScore(),
                request.amount() == null ? 0.0 : request.amount(),
                amountBase == null ? 0.0 : amountBase,
                request.currency(),
                request.customerPortfolioCountry(),
                OffsetDateTime.parse(request.timestamp()).toInstant().toEpochMilli(),
                request.deviceFingerprint(),
                request.tppNameUd(),
                outcome.firedRuleIds(),
                objectMapper.writeValueAsString(features.values()));
    }
}
