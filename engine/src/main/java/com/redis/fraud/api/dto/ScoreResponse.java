package com.redis.fraud.api.dto;

import java.util.List;
import java.util.Map;

/**
 * Scoring result returned synchronously to the caller. The functional checker
 * only reads {@code decision}; the remaining fields support audit and
 * explainability (design doc §4.2) and are populated as later phases land.
 */
public record ScoreResponse(
        String transactionId,
        String decision,
        double finalScore,
        String modelVersion,
        List<String> rulesFired,
        Map<String, Object> featureSnapshot,
        boolean degraded
) {
}
