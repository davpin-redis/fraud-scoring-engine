package com.redis.fraud.redis;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Locks the key strings to exactly what {@code test_data/load_redis.py} writes,
 * so the engine reads the same keys the loader populated (design doc §7.3).
 */
class KeyBuilderTest {

    @Test
    void customerKeysAreHashTaggedByCustomerId() {
        assertThat(KeyBuilder.customerLast("cust_012")).isEqualTo("c:{cust_012}:last");
        assertThat(KeyBuilder.customerRing("cust_012")).isEqualTo("c:{cust_012}:ring:payment");
        assertThat(KeyBuilder.customerAgg("cust_012", "amount", "1h", "2026-08-10T11"))
                .isEqualTo("c:{cust_012}:agg:amount:1h:2026-08-10T11");
        assertThat(KeyBuilder.customerAgg("cust_012", "amount", "1d", "2026-08-10"))
                .isEqualTo("c:{cust_012}:agg:amount:1d:2026-08-10");
        assertThat(KeyBuilder.pairState("cust_030", "bene_017"))
                .isEqualTo("c:{cust_030}:pair:bene_017:state");
    }

    @Test
    void crossEntityKeysAreHashTaggedByTheirOwnValue() {
        assertThat(KeyBuilder.beneDistinctSenders("bene_018"))
                .isEqualTo("bene:{bene_018}:distinct_senders:last_24h");
        assertThat(KeyBuilder.beneAgg("bene_018", "amount", "1h", "2026-08-10T11"))
                .isEqualTo("bene:{bene_018}:agg:amount:1h:2026-08-10T11");
        assertThat(KeyBuilder.geoAgg("US", "amount", "1d", "2026-08-10"))
                .isEqualTo("geo:{US}:agg:amount:1d:2026-08-10");
        assertThat(KeyBuilder.tppAgg("tpp_Alpha", "amount", "1h", "2026-08-10T11"))
                .isEqualTo("tpp:{tpp_Alpha}:agg:amount:1h:2026-08-10T11");
    }

    @Test
    void decisionKeyIsHashTaggedByCustomerForColocatedAtomicWrite() {
        assertThat(KeyBuilder.decision("cust_012", "req_001")).isEqualTo("decision:{cust_012}:req_001");
    }
}
