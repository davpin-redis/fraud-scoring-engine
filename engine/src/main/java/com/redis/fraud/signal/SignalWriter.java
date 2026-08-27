package com.redis.fraud.signal;

import com.redis.fraud.api.dto.ScoreRequest;
import com.redis.fraud.money.FxService;
import com.redis.fraud.scoring.DecisionBander;
import com.redis.fraud.scoring.ScoreResult;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Updates the 90-day approximate signals in the signal store as each transaction is
 * scored (design doc §8.1, revised — replaces the Flex transaction-store write).
 * Runs off the response path and never fails the caller.
 *
 * <p>Idempotent via a per-transaction {@code SET … NX} guard on the signal store, so
 * a retried transaction is a no-op (mirrors the feature-store write's guard, §7.8).
 * All updates target the current rotating monthly bucket / velocity bucket
 * ({@link SignalKeys}); reads span the window. Uses core Redis only — HyperLogLog
 * ({@code PFADD}) for distinct fan-out/fan-in, counters for declines and velocity,
 * and count/sum/sumsq for the amount distribution.
 */
@Component
public class SignalWriter {

    private static final Logger log = LoggerFactory.getLogger(SignalWriter.class);
    private static final long AWAIT_MS = 2000;

    private final StatefulRedisConnection<String, String> connection;
    private final FxService fx;
    private final ExecutorService writeExecutor;

    public SignalWriter(@Qualifier("signalWriteConnection") StatefulRedisConnection<String, String> connection,
                        FxService fx,
                        @Qualifier("featureWriteExecutor") ExecutorService writeExecutor) {
        this.connection = connection;
        this.fx = fx;
        this.writeExecutor = writeExecutor;
    }

    /** Fire-and-forget: never blocks or fails the caller (§3.2 step 8). */
    public void updateAsync(ScoreRequest request, ScoreResult result) {
        writeExecutor.submit(() -> {
            try {
                updateOnce(request, result);
            } catch (RuntimeException e) {
                log.error("Signal-store update failed for txn {}: {}", request.transactionId(), e.toString());
            }
        });
    }

    /** Returns {@code true} if this call updated the signals, {@code false} on a duplicate. */
    public boolean updateOnce(ScoreRequest request, ScoreResult result) {
        String cid = request.customerId();
        String bene = request.receiverTransactionBankAccountNumber();
        Instant now = OffsetDateTime.parse(request.timestamp()).toInstant();

        String won = connection.sync().set(SignalKeys.guard(cid, request.transactionId()), "1",
                SetArgs.Builder.nx().ex(SignalKeys.GUARD_TTL_SEC));
        if (won == null) {
            return false; // duplicate transaction — nothing updated
        }

        double amount = fx.toBase(request.amount() == null ? 0.0 : request.amount(), request.currency());
        boolean decline = DecisionBander.DECLINE.equals(result.decision());

        RedisAsyncCommands<String, String> a = connection.async();
        List<RedisFuture<?>> f = new ArrayList<>();

        if (decline) {
            String k = SignalKeys.declinesMonth(cid, now);
            f.add(a.incr(k));
            f.add(a.expire(k, SignalKeys.MONTH_BUCKET_TTL_SEC));
        }

        String benes = SignalKeys.benesMonth(cid, now);          // R013 fan-out (HLL)
        f.add(a.pfadd(benes, bene));
        f.add(a.expire(benes, SignalKeys.MONTH_BUCKET_TTL_SEC));

        String senders = SignalKeys.sendersMonth(bene, now);     // R005 fan-in (HLL)
        f.add(a.pfadd(senders, cid));
        f.add(a.expire(senders, SignalKeys.MONTH_BUCKET_TTL_SEC));

        // Velocity + amount as TimeSeries (R017–R026: rate, baseline, cadence, dormancy,
        // circadian, bust-out trend, device/payee surge). Series auto-create with 90d retention.
        long nowMs = now.toEpochMilli();
        f.add(SignalTimeSeries.add(a, SignalKeys.velocityTs(cid), nowMs, 1, SignalKeys.TS_RETENTION_MS, "SUM"));
        f.add(SignalTimeSeries.add(a, SignalKeys.amountTs(cid), nowMs, amount, SignalKeys.TS_RETENTION_MS, "LAST"));
        String device = request.deviceFingerprint();
        if (device != null && !device.isBlank()) {                // R025 device velocity surge (bot/ATO ring)
            f.add(SignalTimeSeries.add(a, SignalKeys.deviceVelocityTs(device), nowMs, 1, SignalKeys.SURGE_RETENTION_MS, "SUM"));
        }
        if (bene != null && !bene.isBlank()) {                    // R026 payee inbound velocity surge (mule)
            f.add(SignalTimeSeries.add(a, SignalKeys.beneVelocityTs(bene), nowMs, 1, SignalKeys.SURGE_RETENTION_MS, "SUM"));
        }

        String amt = SignalKeys.amountStatsMonth(cid, now);      // amount distribution (z-score)
        f.add(a.hincrby(amt, "cnt", 1));
        f.add(a.hincrbyfloat(amt, "sum", amount));
        f.add(a.hincrbyfloat(amt, "sumsq", amount * amount));
        f.add(a.expire(amt, SignalKeys.MONTH_BUCKET_TTL_SEC));

        a.flushCommands();
        LettuceFutures.awaitAll(AWAIT_MS, TimeUnit.MILLISECONDS, f.toArray(new RedisFuture<?>[0]));
        return true;
    }
}
