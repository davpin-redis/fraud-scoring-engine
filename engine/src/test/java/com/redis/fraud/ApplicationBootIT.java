package com.redis.fraud;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Boots the full application context against the two-database topology
 * (§7.12), so wiring problems (missing/ambiguous constructors, bad bean graph,
 * wrong DB routing) fail here rather than only at runtime. Complements the
 * slice and pipeline tests.
 */
@SpringBootTest
class ApplicationBootIT extends AbstractTwoRedisIT {

    @Test
    void contextLoads() {
        // Success = the full bean graph instantiated and both the feature-store
        // (Lettuce) and transaction-store (OM Spring) connections came up.
    }
}
