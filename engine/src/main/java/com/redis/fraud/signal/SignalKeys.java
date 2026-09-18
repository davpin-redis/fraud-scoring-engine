package com.redis.fraud.signal;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Key layout for the signal store (90-day approximate signals). Entity keys are
 * Redis Cluster hash-tagged by their entity so all of an entity's signal buckets
 * co-locate on one shard.
 *
 * <p><b>Distinct/count signals</b> use rotating monthly buckets
 * ({@link #WINDOW_MONTHS} ≈ 90–120 days): HyperLogLog for fan-out/fan-in, counters
 * for declines, a hash for the amount z-score. <b>Time-shaped signals</b> (velocity,
 * amount trend, cadence, dormancy, circadian, device/payee surge) use <b>fixed-width
 * bucket hashes with per-field TTL</b> (Redis 8 {@code HEXPIRE}) plus a capped list of
 * recent event timestamps — so each read returns a bounded, constant number of fields
 * regardless of run length or key hotness (design doc §8.1). RedisTimeSeries is no longer
 * on the scoring path.
 */
public final class SignalKeys {

    private SignalKeys() {
    }

    /** Rolling 90-day window = the current month bucket plus the prior 3 (~90–120d). */
    public static final int WINDOW_MONTHS = 4;
    public static final long MONTH_BUCKET_TTL_SEC = (WINDOW_MONTHS + 1) * 31L * 24 * 3600;
    public static final long GUARD_TTL_SEC = 24 * 3600;
    // ---- Behavioural rollups: fixed-width bucket hashes with per-field TTL (Redis 8 HEXPIRE) ----
    // Each rollup is a Hash whose field = the bucket-start epoch-ms and value = a counter/sum.
    // Per-field TTL evicts buckets older than the window, so the field count (and therefore the
    // HGETALL reply size / egress) is bounded by (window / bucket-width) — constant regardless of
    // how long the engine runs or how hot the key is. Replaces the unbounded RedisTimeSeries reads.

    /** Hourly velocity bucket width and field TTL (7-day window + margin) — R019/R020/R021 + rate_1h. */
    public static final long VELH_BUCKET_MS = 3_600_000L;
    public static final long VELH_TTL_SEC = 7L * 24 * 3600 + 2 * 3600;
    /** 5-minute velocity bucket width and field TTL (~2h) — R017 rate_5m. */
    public static final long VEL5_BUCKET_MS = 300_000L;
    public static final long VEL5_TTL_SEC = 2L * 3600 + 600;
    /** Weekly amount bucket width and field TTL (56-day window + margin) — R023 amount trend. */
    public static final long AMTW_BUCKET_MS = 604_800_000L;
    public static final long AMTW_TTL_SEC = 56L * 24 * 3600 + 24 * 3600;
    /** 5-minute surge bucket width and field TTL (~6h) — R025/R026 device/payee surge. */
    public static final long SURGE_BUCKET_MS = 300_000L;
    public static final long SURGE_TTL_SEC = 6L * 3600 + 600;
    /** Capped list of recent event timestamps — inter-arrival cadence (R022). Short-lived. */
    public static final int EVENTS_CAP = 64;
    public static final long EVENTS_TTL_SEC = 7L * 24 * 3600;
    /**
     * Last-event timestamp for dormant-reactivation (R024). Kept long enough to outlive the
     * dormancy threshold (must exceed the ~60-day trigger), unlike the short-lived cadence list.
     */
    public static final long LAST_EVENT_TTL_SEC = 95L * 24 * 3600;

    /** Bucket-start epoch-ms for a timestamp at the given bucket width (used as the hash field). */
    public static long bucketStart(long epochMs, long widthMs) {
        return (epochMs / widthMs) * widthMs;
    }

    private static final DateTimeFormatter MONTH = DateTimeFormatter.ofPattern("yyyyMM").withZone(ZoneOffset.UTC);

    private static String cust(String cid) {
        return "sig:c:{" + cid + "}:";
    }

    private static String bene(String b) {
        return "sig:b:{" + b + "}:";
    }

    private static String dev(String device) {
        return "sig:dev:{" + device + "}:";
    }

    /** Per-transaction idempotency guard (co-located with the customer). */
    public static String guard(String cid, String txnId) {
        return "sig:seen:{" + cid + "}:" + txnId;
    }

    // ---- monthly-bucket keys (HLL / counter / hash) ----

    public static String declinesMonth(String cid, Instant now) {
        return cust(cid) + "declines:" + MONTH.format(now);
    }

    public static String benesMonth(String cid, Instant now) {
        return cust(cid) + "benes:" + MONTH.format(now);
    }

    public static String sendersMonth(String b, Instant now) {
        return bene(b) + "senders:" + MONTH.format(now);
    }

    public static String amountStatsMonth(String cid, Instant now) {
        return cust(cid) + "amt:" + MONTH.format(now);
    }

    public static List<String> declinesWindow(String cid, Instant now) {
        return months(cust(cid) + "declines:", now);
    }

    public static List<String> benesWindow(String cid, Instant now) {
        return months(cust(cid) + "benes:", now);
    }

    public static List<String> sendersWindow(String b, Instant now) {
        return months(bene(b) + "senders:", now);
    }

    public static List<String> amountStatsWindow(String cid, Instant now) {
        return months(cust(cid) + "amt:", now);
    }

    private static List<String> months(String prefix, Instant now) {
        List<String> keys = new ArrayList<>(WINDOW_MONTHS);
        for (int i = 0; i < WINDOW_MONTHS; i++) {
            keys.add(prefix + MONTH.format(now.minus(31L * i, ChronoUnit.DAYS)));
        }
        return keys;
    }

    // ---- Behavioural rollup keys (bounded bucket hashes + capped list) ----

    /** Per-customer hourly velocity counts (Hash: field = hour-bucket ms → count). */
    public static String velocityHour(String cid) {
        return cust(cid) + "vh";
    }

    /** Per-customer 5-minute velocity counts (Hash: field = 5m-bucket ms → count). */
    public static String velocity5m(String cid) {
        return cust(cid) + "v5";
    }

    /** Per-customer capped list of recent event timestamps (List, newest-first) — cadence (R022). */
    public static String recentEvents(String cid) {
        return cust(cid) + "ets";
    }

    /** Per-customer last-event timestamp (String, epoch-ms) — dormant reactivation (R024). */
    public static String lastEventTs(String cid) {
        return cust(cid) + "lts";
    }

    /** Per-customer weekly amount sum (Hash: field = week-bucket ms → sum_base). */
    public static String amountWeekSum(String cid) {
        return cust(cid) + "aws";
    }

    /** Per-customer weekly amount count (Hash: field = week-bucket ms → count). */
    public static String amountWeekCount(String cid) {
        return cust(cid) + "awc";
    }

    /**
     * Per-device 5-minute velocity counts (Hash) — R025 device velocity surge
     * (emulator/bot farm, account-takeover ring). High-cardinality key → spreads evenly
     * across shards (replaces the low-cardinality per-country geo series).
     */
    public static String deviceSurge5m(String device) {
        return dev(device) + "s5";
    }

    /**
     * Per-beneficiary inbound 5-minute velocity counts (Hash) — R026 payee inbound
     * velocity surge (mule / scam payout account). Co-located with the payee's
     * distinct-senders HLL under the same {bene} hash tag.
     */
    public static String beneSurge5m(String b) {
        return bene(b) + "s5";
    }
}
