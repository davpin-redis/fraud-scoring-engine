package com.redis.fraud.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A rule definition (design doc §8.1), stored as JSON in {@code cfg:rules}.
 * {@code type} is {@code hard_block} | {@code hard_allow} | {@code soft};
 * {@code weight} is present only for soft rules. Evaluation arrives in Phase 2.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RuleDef(
        String ruleId,
        String type,
        String condition,
        Double weight
) {
    public static final String HARD_BLOCK = "hard_block";
    public static final String HARD_ALLOW = "hard_allow";
    public static final String SOFT = "soft";
}
