package com.redis.fraud.testsupport;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Faithful Java port of {@code test_data/load_redis.py} for integration tests:
 * replays the backfill and reference data into a (Testcontainers) Redis using
 * the exact §7.3 key schema, so the feature readers are tested against the same
 * state the Python loader produces. Anchored to the fixed reference clock.
 */
public final class RedisFixtureLoader {

    static final Instant REFERENCE_NOW = OffsetDateTime.parse("2026-08-10T12:00:00+00:00").toInstant();
    static final Instant HOT_CUTOFF = REFERENCE_NOW.minus(24, ChronoUnit.HOURS);
    static final int RING_CAP = 50;

    private static final DateTimeFormatter HOUR = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    private final RedisCommands<String, String> r;
    private final ObjectMapper mapper;
    private final Path testDataDir;

    public RedisFixtureLoader(StatefulRedisConnection<String, String> connection, ObjectMapper mapper, Path testDataDir) {
        this.r = connection.sync();
        this.mapper = mapper;
        this.testDataDir = testDataDir;
    }

    public void load() throws Exception {
        r.flushdb();

        JsonNode bl = readTree("reference", "blacklists.json");
        for (JsonNode a : bl.get("bl_accounts")) {
            r.sadd("bl:accounts", a.asString());
        }
        for (JsonNode d : bl.get("bl_devices")) {
            r.sadd("bl:devices", d.asString());
        }

        JsonNode ml = readTree("reference", "membership_lists.json");
        for (JsonNode c : ml.get("vip_customers")) {
            r.sadd("list:vip_customers", c.asString());
        }
        for (JsonNode c : ml.get("watchlist")) {
            r.sadd("list:watchlist", c.asString());
        }

        loadConfig("cfg:windows", "window_id", "config", "windows.json");
        loadConfig("cfg:metrics", "metric_id", "config", "metrics.json");
        loadConfig("cfg:rules", "rule_id", "config", "rules.json");

        // reference / master data (§7.2): customer & beneficiary profiles, IP-geo
        for (JsonNode c : readTree("reference", "customers.json")) {
            Map<String, String> profile = new LinkedHashMap<>();
            profile.put("country", c.get("customer_portfolio_country").asString());
            profile.put("account_open_date", c.get("account_open_date").asString());
            profile.put("risk_segment", c.get("risk_segment").asString());
            r.hset("c:{" + c.get("customer_id").asString() + "}:profile", profile);
        }
        for (JsonNode b : readTree("reference", "beneficiaries.json")) {
            r.hset("bene:{" + b.get("receiver_transaction_bank_account_number").asString() + "}:profile",
                    "country", b.get("country").asString());
        }
        JsonNode ipgeo = readTree("reference", "ip_geo.json");
        ipgeo.get("prefixes").properties().forEach(e -> r.hset("geo:ip_prefixes", e.getKey(), e.getValue().asString()));
        for (JsonNode ip : ipgeo.get("proxy_ips")) {
            r.sadd("geo:proxy_ips", ip.asString());
        }

        for (String line : Files.readAllLines(testDataDir.resolve("transactions").resolve("historical_backfill.jsonl"))) {
            if (!line.isBlank()) {
                applyTransaction(mapper.readTree(line));
            }
        }
    }

    private void loadConfig(String key, String idField, String... parts) throws Exception {
        for (JsonNode node : readTree(parts)) {
            r.hset(key, node.get(idField).asString(), mapper.writeValueAsString(node));
        }
    }

    private void applyTransaction(JsonNode txn) {
        String cid = txn.get("customer_id").asString();
        String bene = txn.get("receiver_transaction_bank_account_number").asString();
        String country = txn.get("customer_portfolio_country").asString();
        String tpp = txn.get("tpp_name_ud").asString();
        double amount = txn.get("amount_base").doubleValue();  // base-currency amount (§7.4)
        Instant ts = OffsetDateTime.parse(txn.get("timestamp").asString()).toInstant();

        boolean hot = !ts.isBefore(HOT_CUTOFF);
        String hb = HOUR.format(ts);
        String db = DAY.format(ts);

        String ringKey = "c:{" + cid + "}:ring:payment";
        r.lpush(ringKey, ringEntry(txn.get("timestamp").asString(), amount, bene));
        r.ltrim(ringKey, 0, RING_CAP - 1);

        bumpAmountStats(hot ? "c:{" + cid + "}:agg:amount:1h:" + hb : "c:{" + cid + "}:agg:amount:1d:" + db, amount);

        bumpAmountStats(hot ? "bene:{" + bene + "}:agg:amount:1h:" + hb : "bene:{" + bene + "}:agg:amount:1d:" + db, amount);
        if (hot) {
            String dkey = "bene:{" + bene + "}:distinct_senders:last_24h";
            r.sadd(dkey, cid);
            r.expire(dkey, 26 * 3600);
        }

        String pkey = "c:{" + cid + "}:pair:" + bene + ":state";
        r.hsetnx(pkey, "first_seen_ts", txn.get("timestamp").asString());
        r.hincrby(pkey, "cnt_90d", 1);
        r.hincrbyfloat(pkey, "sum_90d", amount);

        bumpAmountStats(hot ? "geo:{" + country + "}:agg:amount:1h:" + hb : "geo:{" + country + "}:agg:amount:1d:" + db, amount);
        bumpAmountStats(hot ? "tpp:{" + tpp + "}:agg:amount:1h:" + hb : "tpp:{" + tpp + "}:agg:amount:1d:" + db, amount);

        // device-sharing set (§5)
        r.sadd("device:{" + txn.get("device_fingerprint").asString() + "}:customers", cid);
    }

    private void bumpAmountStats(String key, double amount) {
        r.hincrby(key, "cnt", 1);
        r.hincrbyfloat(key, "sum", amount);
        r.hincrbyfloat(key, "sumsq", amount * amount);
        String curMin = r.hget(key, "min");
        if (curMin == null || amount < Double.parseDouble(curMin)) {
            r.hset(key, "min", Double.toString(amount));
        }
        String curMax = r.hget(key, "max");
        if (curMax == null || amount > Double.parseDouble(curMax)) {
            r.hset(key, "max", Double.toString(amount));
        }
    }

    private String ringEntry(String ts, double amount, String bene) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("ts", ts);
        e.put("amount", amount);
        e.put("bene", bene);
        try {
            return mapper.writeValueAsString(e);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private JsonNode readTree(String... parts) throws Exception {
        Path p = testDataDir;
        for (String part : parts) {
            p = p.resolve(part);
        }
        return mapper.readTree(Files.readString(p));
    }
}
