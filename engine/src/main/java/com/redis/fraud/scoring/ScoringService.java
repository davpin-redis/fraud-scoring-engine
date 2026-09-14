package com.redis.fraud.scoring;

import com.redis.fraud.config.RedisConfigStore;
import com.redis.fraud.feature.FeatureVector;
import com.redis.fraud.rules.RuleOutcome;
import java.util.function.Supplier;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Blends the rule outcome and (optionally) the ML model into a final decision
 * (design doc §8.3).
 *
 * <ul>
 *   <li>Hard block/allow rules win outright.</li>
 *   <li>Degraded transactions (§3.5) route to review.</li>
 *   <li>Otherwise the rules produce a deterministic risk score (summed rule
 *       contributions), mapped through the calibrated 0.30 / 0.70 bands. These
 *       bands apply <b>whether or not the model is enabled</b>.</li>
 *   <li>When the model is enabled ({@code fraud.model.enabled=true}) it acts as
 *       an <b>extra check</b>: the final score is {@code max(ruleScore, modelProbability)},
 *       so the model can only escalate risk, never override a rule to a softer
 *       outcome.</li>
 * </ul>
 */
@Service
public class ScoringService {

    static final String RULES_VERSION = "rules-v1";
    private static final DecisionBander DEFAULT_BANDER = new DecisionBander(0.30, 0.70);

    private final ModelScorer model; // null when fraud.model.enabled=false
    private final Supplier<DecisionBander> banderSupplier; // config-driven bands (§8.3, hot-reloadable)

    public ScoringService() {
        this(null, () -> DEFAULT_BANDER);
    }

    public ScoringService(ModelScorer model) {
        this(model, () -> DEFAULT_BANDER);
    }

    ScoringService(ModelScorer model, Supplier<DecisionBander> banderSupplier) {
        this.model = model;
        this.banderSupplier = banderSupplier;
    }

    @Autowired
    public ScoringService(ObjectProvider<ModelScorer> modelProvider, RedisConfigStore configStore) {
        this.model = modelProvider.getIfAvailable();
        this.banderSupplier = configStore::bands;   // thresholds from cfg:bands, reloaded on cfg:invalidate
    }

    public boolean modelEnabled() {
        return model != null;
    }

    public ScoreResult decide(RuleOutcome outcome) {
        return decide(outcome, false, null);
    }

    public ScoreResult decide(RuleOutcome outcome, boolean degraded) {
        return decide(outcome, degraded, null);
    }

    public ScoreResult decide(RuleOutcome outcome, boolean degraded, FeatureVector features) {
        if (outcome.hardBlock()) {
            return new ScoreResult(DecisionBander.DECLINE, 1.0, null, activeVersion());
        }
        if (outcome.hardAllow()) {
            return new ScoreResult(DecisionBander.APPROVE, 0.0, null, activeVersion());
        }
        if (degraded) {
            return new ScoreResult(DecisionBander.REVIEW, 0.5, null, activeVersion() + "-degraded");
        }

        double ruleScore = Math.min(1.0, outcome.softScore());
        Double modelScore = null;
        double finalScore = ruleScore;
        if (model != null && features != null) {
            double probability = model.fraudProbability(features); // model is an extra escalation check
            modelScore = probability;
            finalScore = Math.max(ruleScore, probability);
        }
        return new ScoreResult(banderSupplier.get().decide(finalScore), finalScore, modelScore, activeVersion());
    }

    private String activeVersion() {
        return model != null ? model.version() : RULES_VERSION;
    }
}
