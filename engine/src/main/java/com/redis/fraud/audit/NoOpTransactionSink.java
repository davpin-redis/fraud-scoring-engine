package com.redis.fraud.audit;

import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Placeholder {@link TransactionSink}: the durable per-transaction audit store
 * ("third store") is not yet implemented, so every scored transaction is counted
 * (and traced) but not persisted. Swap this bean for a real implementation
 * (capped Redis Stream, Kafka, warehouse) — callers are unaffected.
 */
@Component
public class NoOpTransactionSink implements TransactionSink {

    private static final Logger log = LoggerFactory.getLogger(NoOpTransactionSink.class);
    private final AtomicLong seen = new AtomicLong();

    @Override
    public void accept(ScoredTransaction txn) {
        seen.incrementAndGet();
        if (log.isTraceEnabled()) {
            log.trace("audit sink (not implemented) — dropping txn {}", txn.transactionId());
        }
    }

    /** Number of transactions handed to the sink since start (observability). */
    public long seen() {
        return seen.get();
    }
}
