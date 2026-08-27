package com.redis.fraud.rules;

/** A rule that matched, with its type and the outcome it contributed (§8.1). */
public record FiredRule(String ruleId, String type, String outcome) {
}
