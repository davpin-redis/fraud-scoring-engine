package com.redis.fraud.signal;

/**
 * Feature-vector keys produced by the streaming signal path and referenced by the
 * rule definitions in {@code cfg:rules}. The hot-window signals are computed over
 * both a 24-hour window (exact — feature store) and a 90-day window (approximate —
 * signal store) where it makes sense (design doc §8.1, revised — Flex removed).
 *
 * <p>All signals are built on <b>core</b> Redis types (no modules): counters and
 * Sets for the exact 24h values, HyperLogLog for high-cardinality 90d distincts,
 * rotating monthly counters for 90d counts, per-minute/hour counters for velocity,
 * and rolling count/sum/sumsq stats for the amount z-score. New-payee is served by
 * the existing pair-state (R006), so no separate structure is needed.
 */
public final class SignalNames {

    private SignalNames() {
    }

    // R012 — repeat declines (count)
    public static final String CUSTOMER_DECLINES_24H = "customer_declines_24h";
    public static final String CUSTOMER_DECLINES_90D = "customer_declines_90d";

    // R013 — distinct beneficiary fan-out (distinct count)
    public static final String CUSTOMER_DISTINCT_BENE_24H = "customer_distinct_bene_24h";
    public static final String CUSTOMER_DISTINCT_BENE_90D = "customer_distinct_bene_90d";

    // R005 — mule fan-in: distinct senders per beneficiary (24h is the existing
    // bene_distinct_senders_24h feature-store metric; 90d is the new HLL signal)
    public static final String BENE_DISTINCT_SENDERS_90D = "bene_distinct_senders_90d";

    // Velocity — transaction rate over rolling windows (from the velocity TimeSeries)
    public static final String CUSTOMER_TXN_RATE_5M = "customer_txn_rate_5m";
    public static final String CUSTOMER_TXN_RATE_1H = "customer_txn_rate_1h";

    // Amount anomaly — current amount vs the customer's own 90d distribution (z-score)
    public static final String AMOUNT_ZSCORE_90D = "amount_zscore_90d";

    // ---- TimeSeries-derived behavioural signals (design doc §8.1, R019–R026) ----
    public static final String VELOCITY_RATIO_1H = "velocity_ratio_1h";       // R019 rate vs own baseline
    public static final String VELOCITY_ELEVATED_HOURS = "velocity_elevated_hours"; // R020 sustained elevation
    public static final String HOD_SHARE_NOW = "hod_share_now";               // R021 circadian (off-hour) — default 1.0
    public static final String INTERARRIVAL_CV = "interarrival_cv";           // R022 machine cadence — default large
    public static final String AMOUNT_TREND = "amount_trend";                 // R023 bust-out trajectory
    public static final String DORMANCY_DAYS = "dormancy_days";               // R024 dormant reactivation
    public static final String DEVICE_SURGE = "device_surge";                 // R025 device velocity surge (bot/ATO ring)
    public static final String BENE_SURGE = "bene_surge";                     // R026 payee inbound velocity surge (mule)
}
