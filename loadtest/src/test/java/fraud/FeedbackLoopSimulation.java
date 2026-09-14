package fraud;

import static io.gatling.javaapi.core.CoreDsl.StringBody;
import static io.gatling.javaapi.core.CoreDsl.constantUsersPerSec;
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
 * Large-scale feedback-loop traffic (design doc §8.4 / §13.6, LOAD_TEST_RUNBOOK "Feedback-loop
 * run"). Emits the same three-process mixture the offline generator uses so the loop can learn:
 *
 * <ul>
 *   <li><b>normal legit</b> — general customer/payee/device pools (high cardinality);</li>
 *   <li><b>look-alike legit</b> (~8%) — a shared family device (small {@code shared_*} pool) so
 *       {@code device_distinct_customers} rises and the blunt rule false-positives on them;</li>
 *   <li><b>fraud rings</b> (~1%) — reusing a bounded set of {@code mule_*} payees and
 *       {@code farm_*} devices, higher amounts. The chargeback feed (pipeline/chargeback_feed.py)
 *       recognises these entities as fraud and submits labels; the retrain loop learns to
 *       separate them from the look-alikes, so recall rises and false positives fall.</li>
 * </ul>
 *
 * Ground truth is carried only by the entity scheme (mule_/farm_), never through the scoring API.
 * Config via system properties: {@code baseUrl}, {@code scenario}, {@code customers}, {@code benes},
 * {@code mules}, {@code farm}, {@code shared}, {@code tps}, {@code durationSec}.
 */
public class FeedbackLoopSimulation extends Simulation {

    private static final String BASE_URL = System.getProperty("baseUrl", "http://localhost:8080");
    private static final String SCENARIO = System.getProperty("scenario", "func");
    private static final int CUSTOMERS = Integer.getInteger("customers", 5_000_000);
    private static final int BENES = Integer.getInteger("benes", 1_000_000);
    private static final int MULES = Integer.getInteger("mules", 20_000);
    private static final int FARM = Integer.getInteger("farm", 3_000);
    private static final int SHARED = Integer.getInteger("shared", 50_000);   // look-alike shared devices
    private static final int TPS = Integer.getInteger("tps", 1_000);
    private static final int DURATION_SEC = Integer.getInteger("durationSec", 1_800);
    private static final double FRAUD_RATE = Double.parseDouble(System.getProperty("fraudRate", "0.01"));
    private static final double LOOKALIKE_RATE = Double.parseDouble(System.getProperty("lookalikeRate", "0.08"));

    private static final String[] TPPS = {"tpp_Alpha", "tpp_Beta", "tpp_Gamma", "tpp_Delta", "tpp_Epsilon"};
    private static final String[] COUNTRIES = {"US", "GB", "DE", "SG", "BR"};

    private static int rnd(int n) {
        return (int) (Math.random() * n);
    }

    private static Iterator<Map<String, Object>> feeder() {
        AtomicLong seq = new AtomicLong();
        return Stream.generate(() -> {
            Map<String, Object> row = new HashMap<>();
            long n = seq.incrementAndGet();
            double roll = Math.random();
            String bene, device;
            double amount;
            if (roll < FRAUD_RATE) {                                   // fraud ring — reused mule/farm entities
                bene = String.format("mule_%05d", rnd(MULES));
                device = String.format("farm_%05d", rnd(FARM));
                amount = 500 + Math.random() * 4500;
            } else if (roll < FRAUD_RATE + LOOKALIKE_RATE) {           // look-alike legit — shared device
                bene = String.format("bene_%d", rnd(BENES));
                device = String.format("shared_%05d", rnd(SHARED));
                amount = 10 + Math.random() * 490;
            } else {                                                    // general legit
                bene = String.format("bene_%d", rnd(BENES));
                device = String.format("device_%d", rnd(CUSTOMERS));
                amount = 10 + Math.random() * 490;
            }
            row.put("transaction_id", "fl" + Long.toString(n, 36));
            row.put("customer_id", String.format("cust_%03d", 1 + rnd(CUSTOMERS)));
            row.put("receiver_transaction_bank_account_number", bene);
            row.put("device_fingerprint", device);
            row.put("tpp_name_ud", TPPS[rnd(TPPS.length)]);
            row.put("customer_portfolio_country", COUNTRIES[rnd(COUNTRIES.length)]);
            row.put("amount", Math.round(amount));
            row.put("currency", "GBP");
            row.put("timestamp", OffsetDateTime.now(ZoneOffset.UTC).toString());
            return row;
        }).iterator();
    }

    private static final String BODY = """
            {"transaction_id":"#{transaction_id}","timestamp":"#{timestamp}",
             "customer_id":"#{customer_id}","customer_portfolio_country":"#{customer_portfolio_country}",
             "receiver_transaction_bank_account_number":"#{receiver_transaction_bank_account_number}",
             "tpp_name_ud":"#{tpp_name_ud}","device_fingerprint":"#{device_fingerprint}",
             "amount":#{amount},"currency":"#{currency}"}
            """;

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl(BASE_URL)
            .contentTypeHeader("application/json")
            .acceptHeader("application/json")
            .shareConnections();

    private final ScenarioBuilder scn = scenario("fraud-feedback-" + SCENARIO)
            .feed(feeder())
            .exec(http("score").post("/v1/transactions/score").body(StringBody(BODY)).check(status().is(200)));

    private static List<OpenInjectionStep> profile() {
        return switch (SCENARIO) {
            case "steady" -> List.of(constantUsersPerSec(TPS).during(Duration.ofSeconds(DURATION_SEC)));
            case "ramp" -> List.of(rampUsersPerSec(1).to(TPS).during(Duration.ofMinutes(2)),
                    constantUsersPerSec(TPS).during(Duration.ofSeconds(DURATION_SEC)));
            default -> List.of(constantUsersPerSec(TPS).during(Duration.ofSeconds(DURATION_SEC)));
        };
    }

    {
        setUp(scn.injectOpen(profile())).protocols(httpProtocol);
    }
}
