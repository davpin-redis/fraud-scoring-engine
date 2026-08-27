package com.redis.fraud.audit;

/**
 * Durable per-transaction audit store (design doc §4.3, the "third store"). Every
 * scored transaction is handed to the sink off the response path. The concrete
 * durable backing (capped Redis Stream, Kafka, GCS/BigQuery) is <b>not yet
 * implemented</b> — {@link NoOpTransactionSink} is the current placeholder; the
 * call site is wired so the real implementation drops in without touching callers.
 */
public interface TransactionSink {

    void accept(ScoredTransaction txn);
}
