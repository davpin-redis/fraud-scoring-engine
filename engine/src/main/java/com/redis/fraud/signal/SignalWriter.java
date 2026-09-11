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

        // Velocity / cadence / trend / surge as fixed-width bucket hashes with per-field TTL
        // (R017–R026). Each HINCRBY targets the current time-bucket field; HEXPIRE bounds the
        // hash to its window so the read reply stays constant-size regardless of history/rate.
        long nowMs = now.toEpochMilli();
        bumpBucket(a, f, SignalKeys.velocityHour(cid), nowMs, SignalKeys.VELH_BUCKET_MS, SignalKeys.VELH_TTL_SEC);
        bumpBucket(a, f, SignalKeys.velocity5m(cid), nowMs, SignalKeys.VEL5_BUCKET_MS, SignalKeys.VEL5_TTL_SEC);

        // Recent event timestamps (capped list, newest-first) — inter-arrival cadence (R022).
        String ets = SignalKeys.recentEvents(cid);
        f.add(a.lpush(ets, Long.toString(nowMs)));
        f.add(a.ltrim(ets, 0, SignalKeys.EVENTS_CAP - 1));
        f.add(a.expire(ets, SignalKeys.EVENTS_TTL_SEC));
        // Last-event timestamp (long-lived) — dormant reactivation (R024).
        f.add(a.set(SignalKeys.lastEventTs(cid), Long.toString(nowMs),
                SetArgs.Builder.ex(SignalKeys.LAST_EVENT_TTL_SEC)));

        // Weekly amount sum/count buckets — amount bust-out trend (R023).
        String wk = Long.toString(SignalKeys.bucketStart(nowMs, SignalKeys.AMTW_BUCKET_MS));
        f.add(a.hincrbyfloat(SignalKeys.amountWeekSum(cid), wk, amount));
        f.add(a.hincrby(SignalKeys.amountWeekCount(cid), wk, 1));
        f.add(a.hexpire(SignalKeys.amountWeekSum(cid), SignalKeys.AMTW_TTL_SEC, wk));
        f.add(a.hexpire(SignalKeys.amountWeekCount(cid), SignalKeys.AMTW_TTL_SEC, wk));

        String device = request.deviceFingerprint();
        if (device != null && !device.isBlank()) {                // R025 device velocity surge (bot/ATO ring)
            bumpBucket(a, f, SignalKeys.deviceSurge5m(device), nowMs, SignalKeys.SURGE_BUCKET_MS, SignalKeys.SURGE_TTL_SEC);
        }
        if (bene != null && !bene.isBlank()) {                    // R026 payee inbound velocity surge (mule)
            bumpBucket(a, f, SignalKeys.beneSurge5m(bene), nowMs, SignalKeys.SURGE_BUCKET_MS, SignalKeys.SURGE_TTL_SEC);
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

    /**
     * Increments the current time-bucket field of a rollup hash and (re)sets that field's TTL
     * to the window, so the hash self-trims to a bounded field count (Redis 8 {@code HEXPIRE}).
     */
    private static void bumpBucket(RedisAsyncCommands<String, String> a, List<RedisFuture<?>> f,
                                   String key, long nowMs, long widthMs, long ttlSec) {
        String field = Long.toString(SignalKeys.bucketStart(nowMs, widthMs));
        f.add(a.hincrby(key, field, 1));
        f.add(a.hexpire(key, ttlSec, field));
    }
}
