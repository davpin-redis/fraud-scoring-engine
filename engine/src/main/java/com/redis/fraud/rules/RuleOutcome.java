package com.redis.fraud.rules;

import java.util.List;

/**
 * Aggregated result of evaluating the rule set against one transaction (§8.1):
 * whether a hard block/allow fired, the summed soft-signal weight, and the list
 * of rules that fired (for the audit trail and explainability, §2.2).
 */
public record RuleOutcome(List<FiredRule> fired, boolean hardBlock, boolean hardAllow, double softScore) {

    public List<String> firedRuleIds() {
        return fired.stream().map(FiredRule::ruleId).toList();
    }
}
