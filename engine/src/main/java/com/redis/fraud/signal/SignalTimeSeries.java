package com.redis.fraud.signal;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.NestedMultiOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Thin RedisTimeSeries access over Lettuce. Lettuce core has no typed {@code TS.*}
 * commands, so these ride the raw-command {@code dispatch} escape hatch — still
 * fully async ({@link RedisFuture}) and pipelined on the shared multiplexed
 * connection, exactly like every other signal command (design doc §8.1, velocity /
 * amount-trend / behavioural rules).
 *
 * <p>RedisTimeSeries is bundled in Redis 8 and Redis Enterprise, so no separate
 * module install is needed. Series are created lazily by {@code TS.ADD} with a
 * {@code RETENTION} + {@code ON_DUPLICATE} policy; reads aggregate at query time
 * from the raw series (no compaction series to manage).
 */
public final class SignalTimeSeries {

    private SignalTimeSeries() {
    }

    enum Ts implements ProtocolKeyword {
        ADD("TS.ADD"), RANGE("TS.RANGE"), GET("TS.GET");

        private final byte[] bytes;

        Ts(String cmd) {
            this.bytes = cmd.getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }
    }

    /** A single time-series sample. */
    public record Sample(long timestampMs, double value) {
    }

    /**
     * {@code TS.ADD key ts value RETENTION <ms> ON_DUPLICATE <policy>} — appends a
     * sample, creating the series on first use. {@code SUM} for event counters (so
     * same-ms events add up), {@code LAST} for gauges like amount.
     */
    public static RedisFuture<Long> add(RedisAsyncCommands<String, String> async, String key,
                                        long timestampMs, double value, long retentionMs, String dupPolicy) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                .addKey(key).add(timestampMs).add(value)
                .add("RETENTION").add(retentionMs)
                .add("ON_DUPLICATE").add(dupPolicy);
        return async.dispatch(Ts.ADD, new IntegerOutput<>(StringCodec.UTF8), args);
    }

    /** {@code TS.RANGE key from to} — raw samples in the window (no aggregation). */
    public static RedisFuture<List<Object>> range(RedisAsyncCommands<String, String> async, String key,
                                                  long fromMs, long toMs) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                .addKey(key).add(fromMs).add(toMs);
        return async.dispatch(Ts.RANGE, new NestedMultiOutput<>(StringCodec.UTF8), args);
    }

    /** {@code TS.GET key} — the latest sample (or empty). */
    public static RedisFuture<List<Object>> get(RedisAsyncCommands<String, String> async, String key) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).addKey(key);
        return async.dispatch(Ts.GET, new NestedMultiOutput<>(StringCodec.UTF8), args);
    }

    /** {@code TS.RANGE key from to AGGREGATION <type> <bucketMs>} — bucketed aggregation. */
    public static RedisFuture<List<Object>> rangeAgg(RedisAsyncCommands<String, String> async, String key,
                                                    long fromMs, long toMs, String aggType, long bucketMs) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                .addKey(key).add(fromMs).add(toMs)
                .add("AGGREGATION").add(aggType).add(bucketMs);
        return async.dispatch(Ts.RANGE, new NestedMultiOutput<>(StringCodec.UTF8), args);
    }

    /**
     * Parse a {@code TS.RANGE} reply — a flat list alternating {@code ts, value} in
     * RESP2 ({@code NestedMultiOutput} flattens the [[ts,val],…] pairs). Missing or
     * malformed entries are skipped. Returns samples in ascending time order.
     */
    public static List<Sample> parse(List<Object> reply) {
        List<Sample> out = new ArrayList<>();
        if (reply == null) {
            return out;
        }
        // NestedMultiOutput yields either a flat [ts,val,ts,val,...] or nested [[ts,val],...]
        // depending on RESP version; handle both.
        if (!reply.isEmpty() && reply.get(0) instanceof List) {
            for (Object o : reply) {
                if (o instanceof List<?> pair && pair.size() >= 2) {
                    Sample s = toSample(pair.get(0), pair.get(1));
                    if (s != null) {
                        out.add(s);
                    }
                }
            }
        } else {
            for (int i = 0; i + 1 < reply.size(); i += 2) {
                Sample s = toSample(reply.get(i), reply.get(i + 1));
                if (s != null) {
                    out.add(s);
                }
            }
        }
        return out;
    }

    private static Sample toSample(Object tsObj, Object valObj) {
        try {
            long ts = tsObj instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(tsObj));
            double val = valObj instanceof Number n ? n.doubleValue() : Double.parseDouble(String.valueOf(valObj));
            return new Sample(ts, val);
        } catch (RuntimeException e) {
            return null;
        }
    }
}
