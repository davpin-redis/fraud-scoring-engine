package com.redis.fraud.audit;

import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Durable audit sink (design doc §4.3, §8.4.1): appends every scored transaction to a
 * <b>bounded Redis Stream</b> ({@code txn:events}). The stream is a buffered transport,
 * not the archive — it is trimmed with an approximate {@code MAXLEN} so its RAM stays
 * flat regardless of volume; the Parquet writer (a stream consumer) is the system of
 * record. Consumers (Parquet writer, metrics aggregator, embedding service) read via
 * consumer groups.
 *
 * <p>Runs off the response path via {@link TransactionWriter}; a failed {@code XADD}
 * throws and is buffered/retried by that writer, never affecting the caller.
 */
@Component
@ConditionalOnProperty(name = "fraud.audit-sink.type", havingValue = "redis-stream", matchIfMissing = true)
public class RedisStreamTransactionSink implements TransactionSink {

    private static final Logger log = LoggerFactory.getLogger(RedisStreamTransactionSink.class);

    private final StatefulRedisConnection<String, String> connection;
    private final ObjectMapper mapper;
    private final String stream;
    private final XAddArgs xaddArgs;

    public RedisStreamTransactionSink(
            @Qualifier("featureWriteConnection") StatefulRedisConnection<String, String> connection,
            ObjectMapper mapper,
            @Value("${fraud.audit-sink.stream:txn:events}") String stream,
            @Value("${fraud.audit-sink.maxlen:1000000}") long maxlen) {
        this.connection = connection;
        this.mapper = mapper;
        this.stream = stream;
        this.xaddArgs = XAddArgs.Builder.maxlen(maxlen).approximateTrimming();
        log.info("Audit sink: Redis Stream '{}' (approx MAXLEN {})", stream, maxlen);
    }

    @Override
    public void accept(ScoredTransaction txn) {
        String json = mapper.writeValueAsString(txn);
        connection.sync().xadd(stream, xaddArgs, Map.of("v", json));
    }
}
