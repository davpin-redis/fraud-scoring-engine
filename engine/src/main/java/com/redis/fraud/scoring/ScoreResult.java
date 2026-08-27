package com.redis.fraud.scoring;

/**
 * Final scoring outcome (design doc §4.2): the decision, the blended
 * {@code finalScore}, the raw {@code modelScore} (null when the model was not
 * consulted), and the scoring version.
 */
public record ScoreResult(String decision, double finalScore, Double modelScore, String modelVersion) {
}
