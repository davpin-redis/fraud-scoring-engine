package com.redis.fraud.feature;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Enumerates rollup-bucket labels for a time range, matching the formats the
 * loader writes (design doc §7.4): hourly buckets {@code yyyy-MM-dd'T'HH} and
 * daily buckets {@code yyyy-MM-dd}, both in UTC.
 */
public final class BucketTimes {

    private static final DateTimeFormatter HOUR_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DAY_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    private BucketTimes() {
    }

    public static String hourBucket(Instant ts) {
        return HOUR_FMT.format(ts);
    }

    public static String dayBucket(Instant ts) {
        return DAY_FMT.format(ts);
    }

    /** Hourly bucket labels covering [start, end] inclusive, floored to the hour. */
    public static List<String> hourBuckets(Instant start, Instant end) {
        List<String> out = new ArrayList<>();
        Instant cursor = start.truncatedTo(ChronoUnit.HOURS);
        Instant last = end.truncatedTo(ChronoUnit.HOURS);
        while (!cursor.isAfter(last)) {
            out.add(HOUR_FMT.format(cursor));
            cursor = cursor.plus(1, ChronoUnit.HOURS);
        }
        return out;
    }

    /** Daily bucket labels covering [start, end] inclusive, floored to the day. */
    public static List<String> dayBuckets(Instant start, Instant end) {
        List<String> out = new ArrayList<>();
        Instant cursor = start.truncatedTo(ChronoUnit.DAYS);
        Instant last = end.truncatedTo(ChronoUnit.DAYS);
        while (!cursor.isAfter(last)) {
            out.add(DAY_FMT.format(cursor));
            cursor = cursor.plus(1, ChronoUnit.DAYS);
        }
        return out;
    }
}
