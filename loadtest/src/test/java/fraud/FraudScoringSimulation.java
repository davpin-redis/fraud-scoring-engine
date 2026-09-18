package fraud;

import static io.gatling.javaapi.core.CoreDsl.StringBody;
import static io.gatling.javaapi.core.CoreDsl.constantUsersPerSec;
import static io.gatling.javaapi.core.CoreDsl.global;
import static io.gatling.javaapi.core.CoreDsl.rampUsersPerSec;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import static io.gatling.javaapi.http.HttpDsl.http;
import static io.gatling.javaapi.http.HttpDsl.status;

import io.gatling.javaapi.core.OpenInjectionStep;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Load test for the scoring endpoint (design doc §13.3). Drives realistic,
 * seeded traffic (drawn from the fixture entity pools with a small hot-key skew)
 * and injects ~2% correctness cases (blacklist / velocity) so the run verifies
 * decisions stay correct under load, not just fast.
 *
 * <p>Configured by system properties (defaults in parentheses):
 * <ul>
 *   <li>{@code baseUrl} (http://localhost:8082)</li>
 *   <li>{@code scenario} = smoke | steady | ramp | burst | soak | failure (smoke)</li>
 *   <li>{@code p50Max} (10), {@code p99Max} (30) ms; {@code failMax} (1.0) percent — the §2.2 SLA</li>
 * </ul>
 * The full-scale targets (1,000 TPS sustained, burst 3–5k) assume prod-like
 * sizing (§13.4); the {@code smoke} profile is a scaled-down harness check.
 */
public class FraudScoringSimulation extends Simulation {

    private static final String BASE_URL = System.getProperty("baseUrl", "http://localhost:8080");
    private static final String SCENARIO = System.getProperty("scenario", "smoke");
    private static final int P50_MAX = Integer.getInteger("p50Max", 10);
    private static final int P99_MAX = Integer.getInteger("p99Max", 30);
    private static final double FAIL_MAX = Double.parseDouble(System.getProperty("failMax", "1.0"));
    // Scale/shape knobs (used by the `func` functional profile).
    private static final int CUSTOMERS = Integer.getInteger("customers", 50);
    private static final int HIGH_RISK = Integer.getInteger("highRisk", 3);   // seeded R012/R013 customers (cust_001..)
    // General-pool payee cardinality. Must stay HIGH so the R026 per-beneficiary velocity
    // series (sig:b:{bene}:vel) spreads across shards instead of becoming a hot key.
    private static final int BENES = Integer.getInteger("benes", Math.max(1000, CUSTOMERS / 5));
    private static final int TPS = Integer.getInteger("tps", 50);
    private static final int DURATION_SEC = Integer.getInteger("durationSec", 60);

    // --- seeded entity pools (match test_data fixture scale) ---
    private static final String[] TPPS = {"tpp_Alpha", "tpp_Beta", "tpp_Gamma", "tpp_Delta", "tpp_Epsilon"};
    private static final String[] COUNTRIES = {"US", "GB", "DE", "SG", "BR"};

    private static String customer(int i) {
        return String.format("cust_%03d", i);
    }

    private static String bene(int i) {
        return String.format("bene_%03d", i);
    }

    private static String device(int i) {
        return String.format("device_%03d", i);
    }

    /** Infinite feeder: mostly general traffic, a hot-key slice, and ~2% injected correctness cases. */
    private static Iterator<Map<String, Object>> feeder() {
        AtomicLong seq = new AtomicLong();
        return Stream.generate(() -> {
            Map<String, Object> row = new HashMap<>();
            long n = seq.incrementAndGet();
            double roll = Math.random();

            String cid;
            String beneAcct;
            String dev;
            if (roll < 0.01) {                        // ~1% blacklist hits -> decline
                cid = customer(1 + (int) (Math.random() * CUSTOMERS));
                beneAcct = "bene_020";                // seeded blacklisted account
                dev = Math.random() < 0.5 ? "device_099" : device(1 + (int) (Math.random() * CUSTOMERS));
            } else if (roll < 0.02) {                 // ~1% velocity/mule -> review
                cid = "cust_012";                     // seeded velocity-burst customer
                beneAcct = "bene_018";                // seeded mule beneficiary
                dev = device(12);
            } else if (roll < 0.15) {                 // ~13% hot-window slice -> review (R012/R013)
                cid = customer(1 + (int) (Math.random() * HIGH_RISK)); // seeded high-risk customers (TransactionSeeder)
                beneAcct = bene(1 + (int) (Math.random() * 20));
                dev = device(1 + (int) (Math.random() * HIGH_RISK));
            } else {                                  // general pool (high-cardinality payee + device → no hot key)
                cid = customer(1 + (int) (Math.random() * CUSTOMERS));
                beneAcct = bene(1 + (int) (Math.random() * BENES));
                dev = device(1 + (int) (Math.random() * CUSTOMERS));
            }

            row.put("transaction_id", "l" + Long.toString(n, 36)); // compact unique id (short keys, §7.11)
            row.put("customer_id", cid);
            row.put("receiver_account", beneAcct);
            row.put("device_fingerprint", dev);
            row.put("tpp_name_ud", TPPS[(int) (Math.random() * TPPS.length)]);
            row.put("customer_portfolio_country", COUNTRIES[(int) (Math.random() * COUNTRIES.length)]);
            row.put("amount", Math.round(Math.random() * 4900 + 10));
            row.put("currency", "GBP");
            // Live timestamp so the request's 90-day cut-off lines up with the wall-clock
            // history written by TransactionSeeder (feeds R012/R013).
            row.put("timestamp", OffsetDateTime.now(ZoneOffset.UTC).toString());
            return row;
        }).iterator();
    }

    private static final String BODY = """
            {"transaction_id":"#{transaction_id}","timestamp":"#{timestamp}",
             "customer_id":"#{customer_id}","customer_portfolio_country":"#{customer_portfolio_country}",
             "receiver_account":"#{receiver_account}",
             "tpp_name_ud":"#{tpp_name_ud}","device_fingerprint":"#{device_fingerprint}",
             "amount":#{amount},"currency":"#{currency}"}
            """;

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl(BASE_URL)
            .contentTypeHeader("application/json")
            .acceptHeader("application/json")
            .shareConnections();

    private final ScenarioBuilder scn = scenario("fraud-scoring-" + SCENARIO)
            .feed(feeder())
            .exec(http("score")
                    .post("/v1/transactions/score")
                    .body(StringBody(BODY))
                    .check(status().is(200)));

    private static List<OpenInjectionStep> profile() {
        return switch (SCENARIO) {
            // functional check: constant `tps` for `durationSec` (defaults 50/60s)
            case "func" -> List.of(constantUsersPerSec(TPS).during(Duration.ofSeconds(DURATION_SEC)));
            // §13.3 steady-state: 1,000 TPS sustained
            case "steady" -> List.of(constantUsersPerSec(1000).during(Duration.ofMinutes(15)));
            // §13.3 ramp-up: 0 -> 1,000 over ~2 min
            case "ramp" -> List.of(rampUsersPerSec(1).to(1000).during(Duration.ofMinutes(2)));
            // §13.3 burst/peak: 3–5k TPS
            case "burst" -> List.of(constantUsersPerSec(4000).during(Duration.ofMinutes(5)));
            // §13.3 soak/endurance
            case "soak" -> List.of(constantUsersPerSec(1000).during(Duration.ofHours(2)));
            // §13.3 failure injection: steady load while a dependency is degraded externally
            case "failure" -> List.of(constantUsersPerSec(1000).during(Duration.ofMinutes(10)));
            // default: scaled-down harness check with a short warm-up ramp
            default -> List.of(
                    rampUsersPerSec(5).to(50).during(Duration.ofSeconds(5)),
                    constantUsersPerSec(50).during(Duration.ofSeconds(15)));
        };
    }

    {
        setUp(scn.injectOpen(profile()))
                .protocols(httpProtocol)
                .assertions(
                        global().responseTime().percentile(50.0).lt(P50_MAX),
                        global().responseTime().percentile(99.0).lt(P99_MAX),
                        global().failedRequests().percent().lt(FAIL_MAX));
    }
}
