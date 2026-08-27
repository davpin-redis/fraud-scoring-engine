package com.redis.fraud.redis;

/**
 * Builds Redis keys for the feature store, matching the schema written by
 * {@code test_data/load_redis.py} (design doc §7.3).
 *
 * <p>Per-customer keys carry a Redis Cluster hash tag ({@code {customer_id}})
 * so every read/write for one transaction lands on a single shard and can be
 * pipelined or scripted without cross-slot fan-out (§7.1). Cross-entity keys
 * (beneficiary, geography, TPP) are tagged by their own value instead.
 */
public final class KeyBuilder {

    private KeyBuilder() {
    }

    private static String tag(String value) {
        return "{" + value + "}";
    }

    // ---- per-customer (hash-tagged by customer_id) ----

    public static String customerLast(String customerId) {
        return "c:" + tag(customerId) + ":last";
    }

    public static String customerRing(String customerId) {
        return "c:" + tag(customerId) + ":ring:payment";
    }

    public static String customerAgg(String customerId, String metric, String resolution, String bucket) {
        return "c:" + tag(customerId) + ":agg:" + metric + ":" + resolution + ":" + bucket;
    }

    public static String pairState(String customerId, String beneficiary) {
        return "c:" + tag(customerId) + ":pair:" + beneficiary + ":state";
    }

    /** 24h exact repeat-declines counter (R012, §8.1). */
    public static String customerDeclines24h(String customerId) {
        return "c:" + tag(customerId) + ":declines:24h";
    }

    /** 24h exact distinct-beneficiary set (R013, §8.1); count is its SCARD. */
    public static String customerBenes24h(String customerId) {
        return "c:" + tag(customerId) + ":benes:24h";
    }

    // ---- cross-entity (hash-tagged by their own value) ----

    public static String beneDistinctSenders(String beneficiary) {
        return "bene:" + tag(beneficiary) + ":distinct_senders:last_24h";
    }

    public static String beneAgg(String beneficiary, String metric, String resolution, String bucket) {
        return "bene:" + tag(beneficiary) + ":agg:" + metric + ":" + resolution + ":" + bucket;
    }

    public static String geoAgg(String country, String metric, String resolution, String bucket) {
        return "geo:" + tag(country) + ":agg:" + metric + ":" + resolution + ":" + bucket;
    }

    public static String tppAgg(String tpp, String metric, String resolution, String bucket) {
        return "tpp:" + tag(tpp) + ":agg:" + metric + ":" + resolution + ":" + bucket;
    }

    /** Set of distinct customers seen on a device — device-sharing signal (§5). */
    public static String deviceCustomers(String deviceFingerprint) {
        return "device:" + tag(deviceFingerprint) + ":customers";
    }

    // ---- composite cross-entity aggregates (§6.3, §7.6) ----

    public static String beneCountryAgg(String beneficiary, String country, String metric, String resolution, String bucket) {
        return "bene:" + tag(beneficiary) + ":geo:" + country + ":agg:" + metric + ":" + resolution + ":" + bucket;
    }

    public static String tppCountryAgg(String tpp, String country, String metric, String resolution, String bucket) {
        return "tpp:" + tag(tpp) + ":geo:" + country + ":agg:" + metric + ":" + resolution + ":" + bucket;
    }

    // ---- reference / master data (slowly-changing, §7.2) ----

    /** Per-customer profile hash (country, account_open_date, risk_segment), co-located with the customer. */
    public static String customerProfile(String customerId) {
        return "c:" + tag(customerId) + ":profile";
    }

    /** Per-beneficiary profile hash (country). */
    public static String beneProfile(String beneficiary) {
        return "bene:" + tag(beneficiary) + ":profile";
    }

    /** Global IP-geo reference (design doc §7.2): HASH of first-two-octet prefix -> country, and a proxy-IP SET. */
    public static final String GEO_IP_PREFIXES = "geo:ip_prefixes";
    public static final String GEO_PROXY_IPS = "geo:proxy_ips";

    // ---- idempotency guard ----

    /**
     * Cached-decision / idempotency key (§7.8), hash-tagged by customer so it
     * shares a slot with that customer's feature keys — letting the NX guard and
     * the customer-side write run in one atomic Lua script (§7.9).
     */
    public static String decision(String customerId, String transactionId) {
        return "decision:{" + customerId + "}:" + transactionId;
    }
}
