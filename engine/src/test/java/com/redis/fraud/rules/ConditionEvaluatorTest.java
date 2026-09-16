package com.redis.fraud.rules;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Unit tests for the rule-condition DSL, covering the operators the 8 fixture rules use. */
class ConditionEvaluatorTest {

    private final ConditionEvaluator evaluator = new ConditionEvaluator();

    private boolean eval(String condition, Map<String, Object> ctx) {
        return evaluator.evaluate(condition, ctx);
    }

    @Test
    void numericComparisons() {
        assertThat(eval("customer_txn_count_1h > 5", Map.of("customer_txn_count_1h", 9L))).isTrue();
        assertThat(eval("customer_txn_count_1h > 5", Map.of("customer_txn_count_1h", 5L))).isFalse();
        assertThat(eval("bene_distinct_senders_24h > 10", Map.of("bene_distinct_senders_24h", 14L))).isTrue();
        assertThat(eval("amount_usd < 500", Map.of("amount_usd", 40.0))).isTrue();
        assertThat(eval("pair_txn_count_90d == 0", Map.of("pair_txn_count_90d", 0L))).isTrue();
        assertThat(eval("pair_txn_count_90d == 0", Map.of("pair_txn_count_90d", 3L))).isFalse();
    }

    @Test
    void setMembership() {
        assertThat(eval("receiver_account in bl_accounts",
                Map.of("receiver_account", "bene_020", "bl_accounts", Set.of("bene_020")))).isTrue();
        assertThat(eval("customer_id in vip_customers",
                Map.of("customer_id", "cust_046", "vip_customers", Set.of("cust_046")))).isTrue();
        assertThat(eval("customer_id in watchlist",
                Map.of("customer_id", "cust_005", "watchlist", Set.of()))).isFalse();
    }

    @Test
    void listLiteralMembershipIsNumericAware() {
        assertThat(eval("local_hour in [0,1,2,3,4]", Map.of("local_hour", 3))).isTrue();
        assertThat(eval("local_hour in [0,1,2,3,4]", Map.of("local_hour", 12))).isFalse();
    }

    @Test
    void inequalityAndConjunction() {
        Map<String, Object> ctx = Map.of(
                "customer_portfolio_country", "DE",
                "beneficiary_country", "US",
                "local_hour", 3);
        assertThat(eval("customer_portfolio_country != beneficiary_country and local_hour in [0,1,2,3,4]", ctx)).isTrue();
    }

    @Test
    void hardAllowCombinesMembershipAndAmount() {
        assertThat(eval("customer_id in vip_customers and amount_usd < 500",
                Map.of("customer_id", "cust_046", "vip_customers", Set.of("cust_046"), "amount_usd", 40.0))).isTrue();
        assertThat(eval("customer_id in vip_customers and amount_usd < 500",
                Map.of("customer_id", "cust_046", "vip_customers", Set.of("cust_046"), "amount_usd", 900.0))).isFalse();
    }

    @Test
    void missingNumericVariableIsFalseNotError() {
        assertThat(eval("customer_txn_count_1h > 5", Map.of())).isFalse();
    }

    @Test
    void listLiteralParsesAsCollection() {
        // sanity: the list literal itself is usable on the right of `in`
        assertThat(eval("amount_usd in [40, 65, 120]", Map.of("amount_usd", 65.0))).isTrue();
    }
}
