package com.redis.fraud.redis;

import io.lettuce.core.event.command.CommandFailedEvent;
import io.lettuce.core.event.command.CommandListener;
import io.lettuce.core.event.command.CommandStartedEvent;
import io.lettuce.core.event.command.CommandSucceededEvent;
import io.lettuce.core.protocol.RedisCommand;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lettuce command listener that logs every feature-store command and its latency
 * through {@link RedisCallLog}. Lettuce fires a start/success/failure event per
 * command — including each command in a pipelined batch — so this captures the
 * true per-command type ({@code HGET}, {@code MGET}, {@code SISMEMBER},
 * {@code EVAL}, …) and duration, not just the batch as a whole.
 *
 * <p>Lettuce's own event duration is only millisecond-resolution, which is too
 * coarse for sub-millisecond in-memory reads, so latency is measured here with
 * {@link System#nanoTime()}: the start time is stashed on {@code commandStarted}
 * (keyed by the command instance) and subtracted on completion.
 */
public class LoggingCommandListener implements CommandListener {

    private static final String STORE = "feature";

    private final RedisCallLog callLog;
    // Command instances have identity equals/hashCode; the same instance is passed
    // to the start and completion events, so it correlates the two.
    private final ConcurrentHashMap<RedisCommand<?, ?, ?>, Long> startNanos = new ConcurrentHashMap<>();

    public LoggingCommandListener(RedisCallLog callLog) {
        this.callLog = callLog;
    }

    @Override
    public void commandStarted(CommandStartedEvent event) {
        startNanos.put(event.getCommand(), System.nanoTime());
    }

    @Override
    public void commandSucceeded(CommandSucceededEvent event) {
        callLog.record(STORE, type(event.getCommand()), elapsedNanos(event.getCommand()));
    }

    @Override
    public void commandFailed(CommandFailedEvent event) {
        startNanos.remove(event.getCommand());
        Throwable cause = event.getCause();
        callLog.recordFailure(STORE, type(event.getCommand()), cause == null ? "unknown" : cause.toString());
    }

    private long elapsedNanos(RedisCommand<?, ?, ?> command) {
        Long start = startNanos.remove(command);
        return start == null ? 0L : System.nanoTime() - start;
    }

    private static String type(RedisCommand<?, ?, ?> command) {
        return command.getType().toString();
    }
}
