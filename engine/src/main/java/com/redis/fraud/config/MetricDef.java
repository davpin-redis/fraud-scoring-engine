package com.redis.fraud.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A metric definition (design doc §6.2), stored as JSON in {@code cfg:metrics}.
 * Optional fields are null when absent for a given metric type.
 *
 * <ul>
 *   <li>{@code type} — {@code time_since_last_event} | {@code streaming_aggregate} | {@code positional}</li>
 *   <li>{@code agg} — for streaming aggregates: count | stddev | sum | mean | distinct_count</li>
 *   <li>{@code entity} — group-by key (§6.3): customer_id, receiver_account,
 *       or the composite "[customer_id, receiver_account]"</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MetricDef(
        String metricId,
        String entity,
        String type,
        String agg,
        String field,
        String eventType,
        String windowId,
        Long subWindowSec,
        Integer position
) {
    public static final String TYPE_TIME_SINCE = "time_since_last_event";
    public static final String TYPE_AGGREGATE = "streaming_aggregate";
    public static final String TYPE_POSITIONAL = "positional";

    public static final String ENTITY_CUSTOMER = "customer_id";
    public static final String ENTITY_BENEFICIARY = "receiver_account";
    public static final String ENTITY_PAIR = "[customer_id, receiver_account]";
}
