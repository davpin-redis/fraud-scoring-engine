package com.redis.fraud.api.dto;

/**
 * Incoming transaction to score. Field names map to the snake_case JSON in
 * {@code test_data/transactions/sample_test_requests.json} via the global
 * {@code SNAKE_CASE} Jackson strategy.
 *
 * <p>The transaction carries its raw {@code amount} and {@code currency}; the
 * engine normalizes to the base currency at ingest (design doc §3.2, {@code FxService})
 * and computes {@code amount_base}, which is what features and aggregates use (§7.4).
 */
public record ScoreRequest(
        String transactionId,
        String timestamp,
        String customerId,
        String customerPortfolioCountry,
        String receiverAccount,
        String tppNameUd,
        String deviceFingerprint,
        Double amount,
        String currency,
        String ipAddress
) {
}
