package com.redis.fraud.scoring;

/**
 * Maps a final score to a decision using configurable, half-open thresholds
 * (design doc §8.3): {@code [0, review)} approve, {@code [review, decline)}
 * review, {@code [decline, 1]} decline. Thresholds are configurable per §8.3,
 * not hardcoded constants.
 */
public record DecisionBander(double reviewThreshold, double declineThreshold) {

    public static final String APPROVE = "approve";
    public static final String REVIEW = "review";
    public static final String DECLINE = "decline";

    public String decide(double score) {
        if (score >= declineThreshold) {
            return DECLINE;
        }
        if (score >= reviewThreshold) {
            return REVIEW;
        }
        return APPROVE;
    }
}
