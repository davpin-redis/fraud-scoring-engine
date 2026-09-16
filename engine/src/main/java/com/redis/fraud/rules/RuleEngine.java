package com.redis.fraud.rules;

import com.redis.fraud.api.dto.ScoreRequest;
import com.redis.fraud.config.RedisConfigStore;
import com.redis.fraud.config.RuleDef;
import com.redis.fraud.feature.FeatureVector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Evaluates the configurable rule set (design doc §8.1) against a transaction's
 * feature vector. Hard block/allow rules short-circuit an outcome; soft rules
 * contribute weighted signal. Rules read their inputs from the assembled
 * features using the same window/metric framework as everything else.
 *
 * <p>Set-membership conditions (blacklists, VIP, watchlist) are evaluated
 * against per-request singleton sets built from the precomputed {@code SISMEMBER}
 * flags (§7.7): the actual membership check already happened in Redis, so the
 * condition {@code X in bl_accounts} resolves without loading the whole list.
 */
@Component
public class RuleEngine {

    private static final Logger log = LoggerFactory.getLogger(RuleEngine.class);

    private final RedisConfigStore configStore;
    private final ConditionEvaluator evaluator;

    public RuleEngine(RedisConfigStore configStore, ConditionEvaluator evaluator) {
        this.configStore = configStore;
        this.evaluator = evaluator;
    }

    public RuleOutcome evaluate(ScoreRequest request, FeatureVector features) {
        Map<String, Object> ctx = buildContext(request, features);

        List<FiredRule> fired = new ArrayList<>();
        boolean hardBlock = false;
        boolean hardAllow = false;
        double softScore = 0.0;

        for (RuleDef rule : configStore.rules().values()) {
            boolean matched;
            try {
                matched = evaluator.evaluate(rule.condition(), ctx);
            } catch (RuntimeException e) {
                log.warn("Rule '{}' failed to evaluate ('{}'): {}", rule.ruleId(), rule.condition(), e.toString());
                matched = false;
            }
            if (!matched) {
                continue;
            }
            switch (rule.type()) {
                case RuleDef.HARD_BLOCK -> {
                    hardBlock = true;
                    fired.add(new FiredRule(rule.ruleId(), rule.type(), "block"));
                }
                case RuleDef.HARD_ALLOW -> {
                    hardAllow = true;
                    fired.add(new FiredRule(rule.ruleId(), rule.type(), "allow"));
                }
                case RuleDef.SOFT -> {
                    softScore += rule.weight() == null ? 0.0 : rule.weight();
                    fired.add(new FiredRule(rule.ruleId(), rule.type(), "signal"));
                }
                default -> log.warn("Unknown rule type '{}' for rule '{}'", rule.type(), rule.ruleId());
            }
        }
        return new RuleOutcome(fired, hardBlock, hardAllow, softScore);
    }

    private Map<String, Object> buildContext(ScoreRequest req, FeatureVector features) {
        Map<String, Object> ctx = new HashMap<>(features.values());

        // request-level identifiers the conditions may reference
        ctx.put("customer_id", req.customerId());
        ctx.put("receiver_account", req.receiverAccount());
        ctx.put("device_fingerprint", req.deviceFingerprint());
        ctx.put("tpp_name_ud", req.tppNameUd());
        ctx.put("customer_portfolio_country", req.customerPortfolioCountry());
        // amount_base is supplied by the feature vector (normalized at ingest, §3.2)

        // singleton membership sets derived from the precomputed SISMEMBER flags
        ctx.put("bl_accounts", membership(features.asBoolean("bl_account"), req.receiverAccount()));
        ctx.put("bl_devices", membership(features.asBoolean("bl_device"), req.deviceFingerprint()));
        ctx.put("vip_customers", membership(features.asBoolean("vip_customer"), req.customerId()));
        ctx.put("watchlist", membership(features.asBoolean("watchlist"), req.customerId()));

        return ctx;
    }

    private static Set<String> membership(Boolean isMember, String value) {
        return Boolean.TRUE.equals(isMember) ? Set.of(value) : Set.of();
    }
}
