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
 * amount trend, cadence, dormancy, circadian, geo waves) use RedisTimeSeries series
 * with 90-day retention, aggregated at query time (design doc §8.1).
 */
public final class SignalKeys {

    private SignalKeys() {
    }

    /** Rolling 90-day window = the current month bucket plus the prior 3 (~90–120d). */
    public static final int WINDOW_MONTHS = 4;
    public static final long MONTH_BUCKET_TTL_SEC = (WINDOW_MONTHS + 1) * 31L * 24 * 3600;
    public static final long GUARD_TTL_SEC = 24 * 3600;
    /** TimeSeries retention (~95 days, a little over the 90-day window). */
    public static final long TS_RETENTION_MS = 95L * 24 * 3600 * 1000;
    /**
     * Retention for the device/payee surge series (R025/R026). The surge read only looks
     * back 6h, so a short retention keeps these series tiny <b>and</b> bounds the range-scan
     * permanently — no matter how long the run, the series can never grow without limit
     * (the failure mode the old 95-day per-country geo series suffered).
     */
    public static final long SURGE_RETENTION_MS = 2L * 24 * 3600 * 1000;

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

    // ---- TimeSeries keys (velocity / amount trend / entity surge) ----

    /** Per-customer transaction event series (value 1/event) — velocity, cadence, dormancy, circadian. */
    public static String velocityTs(String cid) {
        return cust(cid) + "vel";
    }

    /** Per-customer amount series (value = amount_base) — amount trend / bust-out. */
    public static String amountTs(String cid) {
        return cust(cid) + "amt:ts";
    }

    /**
     * Per-device transaction event series (value 1/event) — R025 device velocity surge
     * (emulator/bot farm, account-takeover ring). High-cardinality key → spreads evenly
     * across shards (replaces the low-cardinality per-country geo series).
     */
    public static String deviceVelocityTs(String device) {
        return dev(device) + "vel";
    }

    /**
     * Per-beneficiary inbound transaction event series (value 1/event) — R026 payee
     * inbound velocity surge (mule / scam payout account). Co-located with the payee's
     * distinct-senders HLL under the same {bene} hash tag.
     */
    public static String beneVelocityTs(String b) {
        return bene(b) + "vel";
    }
}
