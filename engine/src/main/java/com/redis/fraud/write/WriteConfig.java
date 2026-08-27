package com.redis.fraud.write;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Virtual-thread executors for work that runs off, or alongside, the response
 * path.
 *
 * <ul>
 *   <li>{@code featureWriteExecutor} — the post-decision feature-store write
 *       (design doc §3.2 step 8): writes run off the response path so persistence
 *       never adds caller latency.</li>
 *   <li>{@code hotWindowReadExecutor} — the 90-day transaction hot-window read
 *       (§3.3): runs concurrently with the feature-store batch so the extra
 *       Query-Engine round trip overlaps rather than adds to scoring latency.</li>
 * </ul>
 *
 * A virtual-thread-per-task executor keeps these blocking Redis calls cheap.
 */
@Configuration
public class WriteConfig {

    @Bean(destroyMethod = "close")
    public ExecutorService featureWriteExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean(destroyMethod = "close")
    public ExecutorService hotWindowReadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
