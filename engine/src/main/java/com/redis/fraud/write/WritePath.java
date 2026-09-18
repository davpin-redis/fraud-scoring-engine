package com.redis.fraud.write;

import com.redis.fraud.api.dto.ScoreRequest;
import com.redis.fraud.feature.BucketTimes;
import com.redis.fraud.money.FxService;
import com.redis.fraud.redis.KeyBuilder;
import com.redis.fraud.scoring.DecisionBander;
import com.redis.fraud.scoring.ScoreResult;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Post-decision feature-store write (design doc §7.9), run off the response path
 * (§3.2 step 8).
 *
 * <p><b>Customer-side write</b> — one atomic Lua script over keys that all share
 * the {@code {customer_id}} hash tag (ring buffer, the current customer bucket,
 * the pair state, the last-event hash) fronted by the {@code SET … NX} guard
 * (§7.8). Because the guard and the writes are in the same script and slot, a
 * retried transaction is a true no-op: the guard loses the {@code NX}, the script
 * returns 0, and nothing is written twice.
 *
 * <p><b>Cross-entity write</b> — beneficiary / geography / TPP aggregates live on
 * their own shards, so they can't join the customer script; they run as a
 * separate pipelined batch, executed only when the guard was won (return 1).
 * They are independent counters, eventually consistent with the customer write.
 */
@Component
public class WritePath {

    private static final Logger log = LoggerFactory.getLogger(WritePath.class);

    static final int RING_CAP = 50;
    static final long DECISION_TTL_SEC = 24 * 3600;
    static final long HOT_BUCKET_TTL_SEC = 26 * 3600;
    static final long WARM_BUCKET_TTL_SEC = 95L * 24 * 3600; // ~95d for the aged_24h_90d tier
    private static final long AWAIT_MS = 2000;

    /**
     * KEYS: 1 decision(NX guard), 2 ring, 3 customer 1h bucket, 4 pair state, 5 last-event,
     *       6 customer 1d bucket, 7 declines-24h counter, 8 distinct-benes-24h set.
     * ARGV: 1 payload, 2 decisionTtl, 3 ringEntry, 4 ringCap, 5 amount, 6 amountSq, 7 ts,
     *       8 hotTtl, 9 warmTtl, 10 isDecline('1'/'0'), 11 ttl24h, 12 beneficiary.
     * The 24h exact hot-window signals (R012 declines, R013 fan-out) live in this same
     * customer-hash-tagged script, so they are written atomically and idempotently with
     * the rest of the customer state (§7.8/§7.9). Returns 1 if this call wrote, 0 on a duplicate.
     */
    static final String CUSTOMER_SCRIPT = """
            if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) == false then
              return 0
            end
            redis.call('LPUSH', KEYS[2], ARGV[3])
            redis.call('LTRIM', KEYS[2], 0, tonumber(ARGV[4]) - 1)
            redis.call('HINCRBY', KEYS[3], 'cnt', 1)
            redis.call('HINCRBYFLOAT', KEYS[3], 'sum', ARGV[5])
            redis.call('HINCRBYFLOAT', KEYS[3], 'sumsq', ARGV[6])
            redis.call('EXPIRE', KEYS[3], ARGV[8])
            redis.call('HINCRBY', KEYS[6], 'cnt', 1)
            redis.call('HINCRBYFLOAT', KEYS[6], 'sum', ARGV[5])
            redis.call('HINCRBYFLOAT', KEYS[6], 'sumsq', ARGV[6])
            redis.call('EXPIRE', KEYS[6], ARGV[9])
            redis.call('HSETNX', KEYS[4], 'first_seen_ts', ARGV[7])
            redis.call('HINCRBY', KEYS[4], 'cnt_90d', 1)
            redis.call('HINCRBYFLOAT', KEYS[4], 'sum_90d', ARGV[5])
            redis.call('HSET', KEYS[5], 'payment', ARGV[7])
            redis.call('SADD', KEYS[8], ARGV[12])
            redis.call('EXPIRE', KEYS[8], ARGV[11])
            if ARGV[10] == '1' then
              redis.call('INCR', KEYS[7])
              redis.call('EXPIRE', KEYS[7], ARGV[11])
            end
            return 1
            """;

    private final StatefulRedisConnection<String, String> readConnection;
    private final StatefulRedisConnection<String, String> writeConnection;
    private final ObjectMapper mapper;
    private final FxService fx;
    private final ExecutorService writeExecutor;

    public WritePath(@Qualifier("featureReadConnection") StatefulRedisConnection<String, String> readConnection,
                     @Qualifier("featureWriteConnection") StatefulRedisConnection<String, String> writeConnection,
                     ObjectMapper mapper,
                     FxService fx,
                     @Qualifier("featureWriteExecutor") ExecutorService writeExecutor) {
        this.readConnection = readConnection;
        this.writeConnection = writeConnection;
        this.mapper = mapper;
        this.fx = fx;
        this.writeExecutor = writeExecutor;
    }

    /** Idempotent short-circuit (§7.8): return the cached decision if this txn was already scored. */
    public Optional<ScoreResult> cachedDecision(ScoreRequest request) {
        String cached = readConnection.sync().get(KeyBuilder.decision(request.customerId(), request.transactionId()));
        if (cached == null) {
            return Optional.empty();
        }
        return Optional.of(mapper.readValue(cached, ScoreResult.class));
    }

    /** Fire-and-forget: never blocks or fails the caller (§3.2 step 8). */
    public void writeAsync(ScoreRequest request, ScoreResult result) {
        writeExecutor.submit(() -> {
            try {
                writeOnce(request, result);
            } catch (RuntimeException e) {
                log.error("Feature-store write failed for txn {}: {}", request.transactionId(), e.toString());
            }
        });
    }

    /**
     * Performs the write synchronously. Returns {@code true} if this call wrote,
     * {@code false} if the transaction had already been written (idempotent no-op).
     */
    public boolean writeOnce(ScoreRequest request, ScoreResult result) {
        Instant now = OffsetDateTime.parse(request.timestamp()).toInstant();
        String cid = request.customerId();
        String bene = request.receiverAccount();
        double amount = fx.toBase(request.amount() == null ? 0.0 : request.amount(), request.currency());
        String hourBucket = BucketTimes.hourBucket(now);
        String dayBucket = BucketTimes.dayBucket(now);
        String ts = request.timestamp();

        String[] keys = {
                KeyBuilder.decision(cid, request.transactionId()),
                KeyBuilder.customerRing(cid),
                KeyBuilder.customerAgg(cid, "amount", "1h", hourBucket),
                KeyBuilder.pairState(cid, bene),
                KeyBuilder.customerLast(cid),
                KeyBuilder.customerAgg(cid, "amount", "1d", dayBucket),
                KeyBuilder.customerDeclines24h(cid),
                KeyBuilder.customerBenes24h(cid)
        };
        boolean isDecline = DecisionBander.DECLINE.equals(result.decision());
        Long written = writeConnection.sync().eval(
                CUSTOMER_SCRIPT, ScriptOutputType.INTEGER, keys,
                mapper.writeValueAsString(result),
                Long.toString(DECISION_TTL_SEC),
                ringEntry(ts, amount, bene),
                Integer.toString(RING_CAP),
                Double.toString(amount),
                Double.toString(amount * amount),
                ts,
                Long.toString(HOT_BUCKET_TTL_SEC),
                Long.toString(WARM_BUCKET_TTL_SEC),
                isDecline ? "1" : "0",
                Long.toString(DECISION_TTL_SEC),
                bene);

        if (written == null || written != 1L) {
            return false; // duplicate transaction — nothing written
        }

        writeCrossEntity(cid, bene, request.customerPortfolioCountry(),
                request.tppNameUd(), request.deviceFingerprint(), hourBucket, dayBucket, amount);
        return true;
    }

    private void writeCrossEntity(String cid, String bene, String country, String tpp, String deviceFp,
                                  String hourBucket, String dayBucket, double amount) {
        RedisAsyncCommands<String, String> async = writeConnection.async();
        List<RedisFuture<?>> futures = new ArrayList<>();

        // distinct senders to the beneficiary (mule fan-in)
        String distinct = KeyBuilder.beneDistinctSenders(bene);
        futures.add(async.sadd(distinct, cid)); // idempotent for the same sender
        futures.add(async.expire(distinct, HOT_BUCKET_TTL_SEC));

        // device-sharing set (§5): distinct customers seen on this device
        String deviceKey = KeyBuilder.deviceCustomers(deviceFp);
        futures.add(async.sadd(deviceKey, cid));
        futures.add(async.expire(deviceKey, WARM_BUCKET_TTL_SEC));

        // per-entity aggregates at both tiers (§7.4, §7.6): hot (1h) + warm (1d)
        bumpAmount(async, futures, KeyBuilder.beneAgg(bene, "amount", "1h", hourBucket), amount, HOT_BUCKET_TTL_SEC);
        bumpAmount(async, futures, KeyBuilder.beneAgg(bene, "amount", "1d", dayBucket), amount, WARM_BUCKET_TTL_SEC);
        bumpAmount(async, futures, KeyBuilder.geoAgg(country, "amount", "1h", hourBucket), amount, HOT_BUCKET_TTL_SEC);
        bumpAmount(async, futures, KeyBuilder.geoAgg(country, "amount", "1d", dayBucket), amount, WARM_BUCKET_TTL_SEC);
        bumpAmount(async, futures, KeyBuilder.tppAgg(tpp, "amount", "1h", hourBucket), amount, HOT_BUCKET_TTL_SEC);
        bumpAmount(async, futures, KeyBuilder.tppAgg(tpp, "amount", "1d", dayBucket), amount, WARM_BUCKET_TTL_SEC);

        // composite aggregates (§6.3): beneficiary x country, TPP x country
        bumpAmount(async, futures, KeyBuilder.beneCountryAgg(bene, country, "amount", "1h", hourBucket), amount, HOT_BUCKET_TTL_SEC);
        bumpAmount(async, futures, KeyBuilder.beneCountryAgg(bene, country, "amount", "1d", dayBucket), amount, WARM_BUCKET_TTL_SEC);
        bumpAmount(async, futures, KeyBuilder.tppCountryAgg(tpp, country, "amount", "1h", hourBucket), amount, HOT_BUCKET_TTL_SEC);
        bumpAmount(async, futures, KeyBuilder.tppCountryAgg(tpp, country, "amount", "1d", dayBucket), amount, WARM_BUCKET_TTL_SEC);

        async.flushCommands();
        io.lettuce.core.LettuceFutures.awaitAll(AWAIT_MS, TimeUnit.MILLISECONDS,
                futures.toArray(new RedisFuture<?>[0]));
    }

    private void bumpAmount(RedisAsyncCommands<String, String> async, List<RedisFuture<?>> futures,
                            String key, double amount, long ttlSec) {
        futures.add(async.hincrby(key, "cnt", 1));
        futures.add(async.hincrbyfloat(key, "sum", amount));
        futures.add(async.hincrbyfloat(key, "sumsq", amount * amount));
        futures.add(async.expire(key, ttlSec));
    }

    private String ringEntry(String ts, double amount, String bene) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("ts", ts);
        entry.put("amount", amount);
        entry.put("bene", bene);
        return mapper.writeValueAsString(entry);
    }
}
