package com.redis.fraud.audit;

import java.util.List;

/**
 * Immutable audit payload for a scored transaction — the record handed to the
 * {@link TransactionSink} (design doc §4.3, the durable per-transaction audit /
 * "third store"). Decoupled from any storage entity so the sink can back onto a
 * Redis Stream, Kafka, or a warehouse without changing callers.
 */
public record ScoredTransaction(
        String transactionId,
        String customerId,
        String receiverAccount,
        String decision,
        String modelVersion,
        double finalScore,
        Double modelScore,
        double amount,
        double amountBase,
        String currency,
        String customerPortfolioCountry,
        long timestampEpochMs,
        String deviceFingerprint,
        String tppNameUd,
        List<String> rulesFired,
        String featureSnapshotJson
) {
}
