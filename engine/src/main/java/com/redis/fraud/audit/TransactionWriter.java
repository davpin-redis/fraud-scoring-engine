package com.redis.fraud.audit;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Hands every scored transaction to the {@link TransactionSink} (the durable audit
 * "third store") off the response path (design doc §3.2 step 8), never letting a
 * sink failure affect the caller. A failed hand-off is buffered and retried by a
 * background drainer (§3.5). The buffer is in-memory and bounded; the sink itself
 * is currently a no-op placeholder ({@link NoOpTransactionSink}).
 */
@Component
public class TransactionWriter {

    private static final Logger log = LoggerFactory.getLogger(TransactionWriter.class);
    static final int RETRY_BUFFER_CAP = 10_000;
    private static final long DRAIN_INTERVAL_SEC = 1;

    private final TransactionSink sink;
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    private final BlockingQueue<ScoredTransaction> retryBuffer = new LinkedBlockingQueue<>(RETRY_BUFFER_CAP);

    @Autowired
    public TransactionWriter(TransactionSink sink, @Qualifier("featureWriteExecutor") ExecutorService executor) {
        this(sink, executor, true);
    }

    TransactionWriter(TransactionSink sink, ExecutorService executor, boolean autoDrain) {
        this.sink = sink;
        this.executor = executor;
        if (autoDrain) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "txn-audit-retry");
                t.setDaemon(true);
                return t;
            });
            this.scheduler.scheduleWithFixedDelay(this::drainOnce, DRAIN_INTERVAL_SEC, DRAIN_INTERVAL_SEC, TimeUnit.SECONDS);
        } else {
            this.scheduler = null;
        }
    }

    /** Persist off the response path; never throws to the caller. */
    public void persistAsync(ScoredTransaction txn) {
        executor.submit(() -> persist(txn));
    }

    void persist(ScoredTransaction txn) {
        try {
            sink.accept(txn);
        } catch (RuntimeException e) {
            log.warn("Audit sink failed for {}: {} — buffering for retry", txn.transactionId(), e.toString());
            if (!retryBuffer.offer(txn)) {
                log.error("Retry buffer full ({}); dropping txn {}", RETRY_BUFFER_CAP, txn.transactionId());
            }
        }
    }

    /** Retry each buffered record once; failures are re-buffered. Returns remaining count. */
    int drainOnce() {
        int n = retryBuffer.size();
        for (int i = 0; i < n; i++) {
            ScoredTransaction txn = retryBuffer.poll();
            if (txn == null) {
                break;
            }
            try {
                sink.accept(txn);
            } catch (RuntimeException e) {
                retryBuffer.offer(txn);
            }
        }
        return retryBuffer.size();
    }

    int bufferedCount() {
        return retryBuffer.size();
    }

    @PreDestroy
    void shutdown() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
