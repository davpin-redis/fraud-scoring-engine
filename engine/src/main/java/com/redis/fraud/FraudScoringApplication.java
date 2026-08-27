package com.redis.fraud;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the fraud scoring engine.
 *
 * <p>Phase 0 is a walking skeleton: the scoring endpoint is reachable and
 * returns a stubbed decision so the {@code test_data/run_sample_requests.py}
 * checker runs end to end. Feature reads, rules, scoring, and the write path
 * arrive in later phases (see {@code IMPLEMENTATION_PLAN.md}).
 */
@SpringBootApplication
public class FraudScoringApplication {

    public static void main(String[] args) {
        SpringApplication.run(FraudScoringApplication.class, args);
    }
}
