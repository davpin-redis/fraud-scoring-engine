package com.redis.fraud.feature;

import com.redis.fraud.api.dto.ScoreRequest;
import com.redis.fraud.config.MetricDef;
import com.redis.fraud.config.RedisConfigStore;
import com.redis.fraud.config.WindowDef;
import com.redis.fraud.money.FxService;
import com.redis.fraud.redis.KeyBuilder;
import tools.jackson.databind.ObjectMapper;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Assembles the feature vector for a transaction from the Redis feature store
 * (design doc §7.10). Every read for one transaction is issued on the shared
 * Lettuce connection and awaited together, so the whole assembly is effectively
 * one pipelined round trip rather than a read-per-metric.
 *
 * <p>Metric semantics follow §6.2 / §7.4–7.5 and match the keys written by
 * {@code test_data/load_redis.py} (§7.3).
 */
@Service
public class FeatureService {

    private static final Logger log = LoggerFactory.getLogger(FeatureService.class);
    private static final String[] STAT_FIELDS = {"cnt", "sum", "sumsq"};
    private static final long AWAIT_MS = 2000;

    private final StatefulRedisConnection<String, String> connection;
    private final RedisConfigStore configStore;
    private final FxService fx;
    private final ObjectMapper mapper;

    public FeatureService(StatefulRedisConnection<String, String> connection,
                          RedisConfigStore configStore,
                          FxService fx,
                          ObjectMapper mapper) {
        this.connection = connection;
        this.configStore = configStore;
        this.fx = fx;
        this.mapper = mapper;
    }

    public FeatureVector assemble(ScoreRequest request) {
        Instant now = OffsetDateTime.parse(request.timestamp()).toInstant();
        String cid = request.customerId();
        String bene = request.receiverTransactionBankAccountNumber();

        RedisAsyncCommands<String, String> async = connection.async();
        List<RedisFuture<?>> futures = new ArrayList<>();
        Map<String, Supplier<Object>> combiners = new LinkedHashMap<>();

        // ---- configured metrics (§6.2) ----
        for (MetricDef metric : configStore.metrics().values()) {
            switch (metric.type()) {
                case MetricDef.TYPE_TIME_SINCE ->
                        timeSinceLastEvent(async, futures, combiners, metric, cid, now);
                case MetricDef.TYPE_POSITIONAL ->
                        positional(async, futures, combiners, metric, cid, now);
                case MetricDef.TYPE_AGGREGATE ->
                        aggregate(async, futures, combiners, metric, cid, bene, now);
                default -> log.warn("Unknown metric type '{}' for metric '{}'", metric.type(), metric.metricId());
            }
        }

        // ---- membership / blacklist flags (part of the same batch, §7.7/§7.10) ----
        RedisFuture<Boolean> vip = async.sismember("list:vip_customers", cid);
        RedisFuture<Boolean> watch = async.sismember("list:watchlist", cid);
        RedisFuture<Boolean> blAcct = async.sismember("bl:accounts", bene);
        RedisFuture<Boolean> blDev = async.sismember("bl:devices", request.deviceFingerprint());
        RedisFuture<Long> deviceCustomers = async.scard(KeyBuilder.deviceCustomers(request.deviceFingerprint()));
        futures.add(vip);
        futures.add(watch);
        futures.add(blAcct);
        futures.add(blDev);
        futures.add(deviceCustomers);

        // ---- reference / master data, read from Redis in the same batch (§7.2) ----
        RedisFuture<String> beneCountryF = async.hget(KeyBuilder.beneProfile(bene), "country");
        RedisFuture<String> acctOpenF = async.hget(KeyBuilder.customerProfile(cid), "account_open_date");
        futures.add(beneCountryF);
        futures.add(acctOpenF);
        String ip = request.ipAddress();
        String ipPrefix = ipPrefix(ip);
        RedisFuture<String> ipCountryF = ipPrefix == null ? null : async.hget(KeyBuilder.GEO_IP_PREFIXES, ipPrefix);
        RedisFuture<Boolean> ipProxyF = ip == null ? null : async.sismember(KeyBuilder.GEO_PROXY_IPS, ip);
        if (ipCountryF != null) {
            futures.add(ipCountryF);
        }
        if (ipProxyF != null) {
            futures.add(ipProxyF);
        }

        // one flush, then await the whole batch
        async.flushCommands();
        awaitAll(futures);

        Map<String, Object> out = new LinkedHashMap<>();
        combiners.forEach((id, c) -> out.put(id, c.get()));

        out.put("vip_customer", get(vip));
        out.put("watchlist", get(watch));
        out.put("bl_account", get(blAcct));
        out.put("bl_device", get(blDev));
        out.put("device_distinct_customers", get(deviceCustomers));

        // ---- request-derived + reference-data fields ----
        double amountBase = fx.toBase(request.amount() == null ? 0.0 : request.amount(), request.currency());
        out.put("amount_base", amountBase);
        out.put("customer_portfolio_country", request.customerPortfolioCountry());
        out.put("beneficiary_country", get(beneCountryF));
        out.put("local_hour", now.atZone(ZoneOffset.UTC).getHour());

        // §5 account & network signals (reference data from Redis, §7.2)
        String accountOpen = get(acctOpenF);
        out.put("account_age_days", accountOpen == null ? null
                : ChronoUnit.DAYS.between(LocalDate.parse(accountOpen), now.atZone(ZoneOffset.UTC).toLocalDate()));
        String ipCountry = ipCountryF == null ? null : get(ipCountryF);
        // unknown/absent IP is treated as domestic so it doesn't false-trigger the geo rule
        out.put("ip_country", ipCountry != null ? ipCountry : request.customerPortfolioCountry());
        out.put("ip_proxy", ipProxyF != null && Boolean.TRUE.equals(get(ipProxyF)));

        return new FeatureVector(out);
    }

    private static String ipPrefix(String ip) {
        if (ip == null) {
            return null;
        }
        String[] parts = ip.split("\\.");
        return parts.length >= 2 ? parts[0] + "." + parts[1] : null;
    }

    // ---- metric handlers ----

    private void timeSinceLastEvent(RedisAsyncCommands<String, String> async, List<RedisFuture<?>> futures,
                                    Map<String, Supplier<Object>> combiners, MetricDef m, String cid, Instant now) {
        RedisFuture<String> f = async.hget(KeyBuilder.customerLast(cid), m.eventType());
        futures.add(f);
        combiners.put(m.metricId(), () -> {
            String ts = get(f);
            if (ts == null) {
                return null; // undefined: no such event in the window (§6.2)
            }
            long seconds = OffsetDateTime.parse(ts).toInstant().until(now, java.time.temporal.ChronoUnit.SECONDS);
            return seconds / 3600.0;
        });
    }

    private void positional(RedisAsyncCommands<String, String> async, List<RedisFuture<?>> futures,
                            Map<String, Supplier<Object>> combiners, MetricDef m, String cid, Instant now) {
        int index = m.position() == null ? 0 : m.position();
        RedisFuture<String> f = async.lindex(KeyBuilder.customerRing(cid), index);
        futures.add(f);
        combiners.put(m.metricId(), () -> {
            String json = get(f);
            if (json == null) {
                return null;
            }
            try {
                String ts = mapper.readTree(json).get("ts").asString();
                Instant eventTs = OffsetDateTime.parse(ts).toInstant();
                if (m.subWindowSec() != null && eventTs.isBefore(now.minusSeconds(m.subWindowSec()))) {
                    return null; // outside the sub-window bound (§6.2)
                }
                return ts;
            } catch (Exception e) {
                throw new IllegalStateException("Bad ring-buffer entry for " + cid + ": " + json, e);
            }
        });
    }

    private void aggregate(RedisAsyncCommands<String, String> async, List<RedisFuture<?>> futures,
                           Map<String, Supplier<Object>> combiners, MetricDef m,
                           String cid, String bene, Instant now) {
        if ("distinct_count".equals(m.agg()) && MetricDef.ENTITY_BENEFICIARY.equals(m.entity())) {
            RedisFuture<Long> f = async.scard(KeyBuilder.beneDistinctSenders(bene));
            futures.add(f);
            combiners.put(m.metricId(), () -> get(f));
            return;
        }
        if (MetricDef.ENTITY_PAIR.equals(m.entity())) {
            // lightweight pair state hash holds the running count (§7.12)
            RedisFuture<String> f = async.hget(KeyBuilder.pairState(cid, bene), "cnt_90d");
            futures.add(f);
            combiners.put(m.metricId(), () -> {
                String v = get(f);
                return v == null ? 0L : Long.parseLong(v);
            });
            return;
        }

        // customer-level bucketed sufficient statistics (§7.4)
        List<RedisFuture<List<KeyValue<String, String>>>> bucketFutures = new ArrayList<>();
        for (String bucketKey : customerBucketKeys(m, cid, now)) {
            RedisFuture<List<KeyValue<String, String>>> f = async.hmget(bucketKey, STAT_FIELDS);
            futures.add(f);
            bucketFutures.add(f);
        }
        String agg = m.agg();
        combiners.put(m.metricId(), () -> combineAggregate(agg, bucketFutures));
    }

    private List<String> customerBucketKeys(MetricDef m, String cid, Instant now) {
        WindowDef w = configStore.window(m.windowId());
        long startOffset = m.subWindowSec() != null ? -m.subWindowSec() : w.lookbackStartOffsetSec();
        long endOffset = w.lookbackEndOffsetSec();
        Instant start = now.plusSeconds(startOffset);
        Instant end = now.plusSeconds(endOffset);

        String res = w.bucketResolution();
        List<String> buckets = "1d".equals(res)
                ? BucketTimes.dayBuckets(start, end)
                : BucketTimes.hourBuckets(start, end);

        List<String> keys = new ArrayList<>(buckets.size());
        for (String b : buckets) {
            keys.add(KeyBuilder.customerAgg(cid, "amount", res, b));
        }
        return keys;
    }

    private Object combineAggregate(String agg, List<RedisFuture<List<KeyValue<String, String>>>> bucketFutures) {
        double cnt = 0, sum = 0, sumsq = 0;
        for (RedisFuture<List<KeyValue<String, String>>> f : bucketFutures) {
            List<KeyValue<String, String>> kvs = get(f);
            cnt += statValue(kvs, 0);
            sum += statValue(kvs, 1);
            sumsq += statValue(kvs, 2);
        }
        return switch (agg) {
            case "count" -> (long) cnt;
            case "sum" -> sum;
            case "mean" -> cnt == 0 ? null : sum / cnt;
            case "stddev" -> {
                if (cnt == 0) {
                    yield null;
                }
                double mean = sum / cnt;
                double variance = Math.max(0.0, sumsq / cnt - mean * mean);
                yield Math.sqrt(variance);
            }
            default -> throw new IllegalArgumentException("Unsupported aggregate: " + agg);
        };
    }

    private static double statValue(List<KeyValue<String, String>> kvs, int idx) {
        KeyValue<String, String> kv = kvs.get(idx);
        return kv.hasValue() ? Double.parseDouble(kv.getValue()) : 0.0;
    }

    private void awaitAll(List<RedisFuture<?>> futures) {
        try {
            io.lettuce.core.LettuceFutures.awaitAll(AWAIT_MS, TimeUnit.MILLISECONDS,
                    futures.toArray(new RedisFuture<?>[0]));
        } catch (RuntimeException e) {
            throw new IllegalStateException("Feature-store read batch failed", e);
        }
    }

    private static <T> T get(RedisFuture<T> f) {
        try {
            return f.get(AWAIT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Feature read failed", e);
        }
    }
}
