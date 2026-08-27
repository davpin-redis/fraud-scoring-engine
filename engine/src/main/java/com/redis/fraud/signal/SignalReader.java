package com.redis.fraud.signal;

import com.redis.fraud.api.dto.ScoreRequest;
import com.redis.fraud.money.FxService;
import com.redis.fraud.redis.KeyBuilder;
import com.redis.fraud.signal.SignalTimeSeries.Sample;
import io.lettuce.core.KeyValue;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Assembles the hot-window signals for a transaction (design doc §8.1). Reads the
 * <b>24h exact</b> signals from the feature store and the <b>90d approximate</b>
 * signals from the signal store — distinct/count signals from HyperLogLog/counters,
 * and time-shaped behavioural signals (velocity vs baseline, sustained elevation,
 * circadian, machine cadence, amount bust-out, dormancy, device/payee surge) from
 * RedisTimeSeries — each pipelined and merged into the scoring vector.
 *
 * <p>Degrades gracefully: on failure the caller substitutes {@link #empty()}
 * (all-safe defaults). TimeSeries reads for a not-yet-existing series are tolerated
 * individually (treated as no history) so new customers never fail or false-trip.
 */
@Component
public class SignalReader {

    private static final long AWAIT_MS = 2000;
    private static final long MIN_AMOUNT_SAMPLES = 5;
    private static final long HOUR_MS = 3_600_000L;
    private static final long DAY_MS = 86_400_000L;
    private static final long MIN5_MS = 300_000L;
    private static final long WEEK_MS = 604_800_000L;
    private static final int MIN_BASELINE_HOURS = 24;   // active hours needed before rate/circadian signals engage
    private static final int MIN_GAP_SAMPLES = 5;
    private static final int MIN_TREND_WEEKS = 4;
    private static final int MIN_SURGE_BUCKETS = 12;

    private final StatefulRedisConnection<String, String> featureConnection;
    private final StatefulRedisConnection<String, String> signalConnection;
    private final FxService fx;

    public SignalReader(StatefulRedisConnection<String, String> featureConnection,
                        @Qualifier("signalReadConnection") StatefulRedisConnection<String, String> signalConnection,
                        FxService fx) {
        this.featureConnection = featureConnection;
        this.signalConnection = signalConnection;
        this.fx = fx;
    }

    public Map<String, Object> read(ScoreRequest request) {
        String cid = request.customerId();
        String bene = request.receiverTransactionBankAccountNumber();
        String device = request.deviceFingerprint();
        Instant now = OffsetDateTime.parse(request.timestamp()).toInstant();
        long nowMs = now.toEpochMilli();
        double amount = fx.toBase(request.amount() == null ? 0.0 : request.amount(), request.currency());

        // ---- 24h exact (feature store) ----
        RedisAsyncCommands<String, String> fa = featureConnection.async();
        RedisFuture<String> declines24hF = fa.get(KeyBuilder.customerDeclines24h(cid));
        RedisFuture<Long> benes24hF = fa.scard(KeyBuilder.customerBenes24h(cid));

        // ---- 90d approximate: distinct/count (signal store) ----
        RedisAsyncCommands<String, String> sa = signalConnection.async();
        RedisFuture<List<KeyValue<String, String>>> declines90dF =
                sa.mget(SignalKeys.declinesWindow(cid, now).toArray(new String[0]));
        RedisFuture<Long> benes90dF = sa.pfcount(SignalKeys.benesWindow(cid, now).toArray(new String[0]));
        RedisFuture<Long> senders90dF = sa.pfcount(SignalKeys.sendersWindow(bene, now).toArray(new String[0]));
        List<RedisFuture<List<KeyValue<String, String>>>> amtF = new ArrayList<>();
        for (String k : SignalKeys.amountStatsWindow(cid, now)) {
            amtF.add(sa.hmget(k, "cnt", "sum", "sumsq"));
        }

        // ---- 90d time-shaped (RedisTimeSeries, tolerated individually) ----
        RedisFuture<List<Object>> velHourlyF =
                SignalTimeSeries.rangeAgg(sa, SignalKeys.velocityTs(cid), nowMs - 7 * DAY_MS, nowMs, "count", HOUR_MS);
        RedisFuture<List<Object>> velRecentF =
                SignalTimeSeries.range(sa, SignalKeys.velocityTs(cid), nowMs - 2 * HOUR_MS, nowMs);
        RedisFuture<List<Object>> velLastF = SignalTimeSeries.get(sa, SignalKeys.velocityTs(cid));
        RedisFuture<List<Object>> amtWeeklyF =
                SignalTimeSeries.rangeAgg(sa, SignalKeys.amountTs(cid), nowMs - 56 * DAY_MS, nowMs, "avg", WEEK_MS);
        RedisFuture<List<Object>> deviceRecentF = (device == null || device.isBlank()) ? null
                : SignalTimeSeries.rangeAgg(sa, SignalKeys.deviceVelocityTs(device), nowMs - 6 * HOUR_MS, nowMs, "count", MIN5_MS);
        RedisFuture<List<Object>> beneRecentF = (bene == null || bene.isBlank()) ? null
                : SignalTimeSeries.rangeAgg(sa, SignalKeys.beneVelocityTs(bene), nowMs - 6 * HOUR_MS, nowMs, "count", MIN5_MS);

        fa.flushCommands();
        sa.flushCommands();
        // Strict await for the non-TS reads (a failure here means the store is down → degrade).
        List<RedisFuture<?>> strict = new ArrayList<>(List.of(
                declines24hF, benes24hF, declines90dF, benes90dF, senders90dF));
        strict.addAll(amtF);
        LettuceFutures.awaitAll(AWAIT_MS, TimeUnit.MILLISECONDS, strict.toArray(new RedisFuture<?>[0]));

        Map<String, Object> out = empty();
        out.put(SignalNames.CUSTOMER_DECLINES_24H, parseLong(get(declines24hF)));
        out.put(SignalNames.CUSTOMER_DISTINCT_BENE_24H, get(benes24hF));
        out.put(SignalNames.CUSTOMER_DECLINES_90D, sumLongs(get(declines90dF)));
        out.put(SignalNames.CUSTOMER_DISTINCT_BENE_90D, get(benes90dF));
        out.put(SignalNames.BENE_DISTINCT_SENDERS_90D, get(senders90dF));
        out.put(SignalNames.AMOUNT_ZSCORE_90D, zscore(amtF, amount));

        // ---- TimeSeries-derived signals ----
        List<Sample> hourly = SignalTimeSeries.parse(tolerant(velHourlyF));
        List<Sample> recent = SignalTimeSeries.parse(tolerant(velRecentF));
        List<Sample> last = SignalTimeSeries.parse(tolerant(velLastF));
        List<Sample> weekly = SignalTimeSeries.parse(tolerant(amtWeeklyF));
        List<Sample> deviceRecent = deviceRecentF == null ? List.of() : SignalTimeSeries.parse(tolerant(deviceRecentF));
        List<Sample> beneRecent = beneRecentF == null ? List.of() : SignalTimeSeries.parse(tolerant(beneRecentF));

        double baseline = mean(hourly);
        double current1h = sumSince(hourly, nowMs - HOUR_MS);
        out.put(SignalNames.CUSTOMER_TXN_RATE_1H, (long) current1h);
        out.put(SignalNames.CUSTOMER_TXN_RATE_5M, (long) countSince(recent, nowMs - MIN5_MS));
        if (hourly.size() >= MIN_BASELINE_HOURS && baseline > 0) {
            out.put(SignalNames.VELOCITY_RATIO_1H, current1h / baseline);
            long elevated = hourly.stream()
                    .filter(s -> s.timestampMs() >= nowMs - 6 * HOUR_MS && s.value() > 2 * baseline)
                    .count();
            out.put(SignalNames.VELOCITY_ELEVATED_HOURS, elevated);
            out.put(SignalNames.HOD_SHARE_NOW, hourOfDayShare(hourly, nowMs));
        }
        out.put(SignalNames.INTERARRIVAL_CV, interarrivalCv(recent));
        out.put(SignalNames.DORMANCY_DAYS, last.isEmpty() ? 0.0 : (nowMs - last.get(last.size() - 1).timestampMs()) / (double) DAY_MS);
        out.put(SignalNames.AMOUNT_TREND, amountTrend(weekly));
        out.put(SignalNames.DEVICE_SURGE, surge(deviceRecent, nowMs));
        out.put(SignalNames.BENE_SURGE, surge(beneRecent, nowMs));
        return out;
    }

    /** Safe defaults for the degraded path / insufficient history — never trips a rule. */
    public static Map<String, Object> empty() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(SignalNames.CUSTOMER_DECLINES_24H, 0L);
        m.put(SignalNames.CUSTOMER_DISTINCT_BENE_24H, 0L);
        m.put(SignalNames.CUSTOMER_DECLINES_90D, 0L);
        m.put(SignalNames.CUSTOMER_DISTINCT_BENE_90D, 0L);
        m.put(SignalNames.BENE_DISTINCT_SENDERS_90D, 0L);
        m.put(SignalNames.AMOUNT_ZSCORE_90D, 0.0);
        m.put(SignalNames.CUSTOMER_TXN_RATE_5M, 0L);
        m.put(SignalNames.CUSTOMER_TXN_RATE_1H, 0L);
        m.put(SignalNames.VELOCITY_RATIO_1H, 0.0);
        m.put(SignalNames.VELOCITY_ELEVATED_HOURS, 0L);
        m.put(SignalNames.HOD_SHARE_NOW, 1.0);       // 1.0 = "typical hour" → won't trip R021
        m.put(SignalNames.INTERARRIVAL_CV, 999.0);   // large = irregular → won't trip R022
        m.put(SignalNames.AMOUNT_TREND, 0.0);
        m.put(SignalNames.DORMANCY_DAYS, 0.0);
        m.put(SignalNames.DEVICE_SURGE, 0.0);
        m.put(SignalNames.BENE_SURGE, 0.0);
        return m;
    }

    // ---- signal computations ----

    private static double mean(List<Sample> s) {
        if (s.isEmpty()) {
            return 0;
        }
        double sum = 0;
        for (Sample x : s) {
            sum += x.value();
        }
        return sum / s.size();
    }

    private static double sumSince(List<Sample> s, long fromMs) {
        double sum = 0;
        for (Sample x : s) {
            if (x.timestampMs() >= fromMs) {
                sum += x.value();
            }
        }
        return sum;
    }

    private static long countSince(List<Sample> s, long fromMs) {
        return s.stream().filter(x -> x.timestampMs() >= fromMs).count();
    }

    /** Share of the customer's activity that falls in the current UTC hour-of-day (low = off-hour). */
    private static double hourOfDayShare(List<Sample> hourly, long nowMs) {
        double total = 0;
        double inNowHod = 0;
        long nowHod = (nowMs / HOUR_MS) % 24;
        for (Sample s : hourly) {
            total += s.value();
            if ((s.timestampMs() / HOUR_MS) % 24 == nowHod) {
                inNowHod += s.value();
            }
        }
        return total <= 0 ? 1.0 : inNowHod / total;
    }

    /** Coefficient of variation of inter-arrival gaps (low = machine-like cadence). */
    private static double interarrivalCv(List<Sample> recent) {
        if (recent.size() < MIN_GAP_SAMPLES) {
            return 999.0;
        }
        List<Long> gaps = new ArrayList<>();
        for (int i = 1; i < recent.size(); i++) {
            gaps.add(recent.get(i).timestampMs() - recent.get(i - 1).timestampMs());
        }
        double m = gaps.stream().mapToLong(Long::longValue).average().orElse(0);
        if (m <= 0) {
            return 999.0;
        }
        double var = gaps.stream().mapToDouble(g -> (g - m) * (g - m)).average().orElse(0);
        return Math.sqrt(var) / m;
    }

    /** Least-squares slope of weekly-average amount, normalised by the mean (fractional growth per week). */
    private static double amountTrend(List<Sample> weekly) {
        int n = weekly.size();
        if (n < MIN_TREND_WEEKS) {
            return 0.0;
        }
        double sx = 0, sy = 0, sxx = 0, sxy = 0;
        for (int i = 0; i < n; i++) {
            double y = weekly.get(i).value();
            sx += i;
            sy += y;
            sxx += (double) i * i;
            sxy += (double) i * y;
        }
        double denom = n * sxx - sx * sx;
        double meanY = sy / n;
        if (denom == 0 || meanY <= 0) {
            return 0.0;
        }
        double slope = (n * sxy - sx * sy) / denom;
        return slope / meanY;
    }

    /** Current 5-min country rate vs the country's trailing average (high = systemic surge). */
    /**
     * Ratio of the current 5-minute event count to the mean 5-minute bucket over the
     * recent window. Used for both R025 (per-device) and R026 (per-beneficiary) surge —
     * the entity's own rate spiking well above its recent baseline. Needs a minimum
     * number of populated buckets before it engages (a brand-new entity never trips).
     */
    private static double surge(List<Sample> buckets, long nowMs) {
        if (buckets.size() < MIN_SURGE_BUCKETS) {
            return 0.0;
        }
        double avg = mean(buckets);
        double current = sumSince(buckets, nowMs - MIN5_MS);
        return avg <= 0 ? 0.0 : current / avg;
    }

    private double zscore(List<RedisFuture<List<KeyValue<String, String>>>> amtF, double amount) {
        double cnt = 0, sum = 0, sumsq = 0;
        for (RedisFuture<List<KeyValue<String, String>>> f : amtF) {
            List<KeyValue<String, String>> kvs = get(f);
            cnt += statValue(kvs, 0);
            sum += statValue(kvs, 1);
            sumsq += statValue(kvs, 2);
        }
        if (cnt < MIN_AMOUNT_SAMPLES) {
            return 0.0;
        }
        double m = sum / cnt;
        double std = Math.sqrt(Math.max(0.0, sumsq / cnt - m * m));
        return std <= 0 ? 0.0 : (amount - m) / std;
    }

    // ---- reply helpers ----

    private static double statValue(List<KeyValue<String, String>> kvs, int idx) {
        KeyValue<String, String> kv = kvs.get(idx);
        return kv.hasValue() ? Double.parseDouble(kv.getValue()) : 0.0;
    }

    private static long sumLongs(List<KeyValue<String, String>> kvs) {
        long total = 0;
        for (KeyValue<String, String> kv : kvs) {
            if (kv.hasValue()) {
                total += Long.parseLong(kv.getValue());
            }
        }
        return total;
    }

    private static long parseLong(String v) {
        return v == null ? 0L : Long.parseLong(v);
    }

    private static <T> T get(RedisFuture<T> f) {
        try {
            return f.get(AWAIT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Signal read failed", e);
        }
    }

    /** Tolerant getter for TimeSeries reads: a missing series (new customer) → no history. */
    private static List<Object> tolerant(RedisFuture<List<Object>> f) {
        try {
            List<Object> v = f.get(AWAIT_MS, TimeUnit.MILLISECONDS);
            return v == null ? List.of() : v;
        } catch (Exception e) {
            return List.of();
        }
    }
}
