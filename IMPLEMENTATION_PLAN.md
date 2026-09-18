# Implementation Plan — Real-Time Transaction Fraud Scoring System

Companion to `Fraud_Scoring_System_Design_Doc.md`. This plan turns that design
into a buildable reference implementation, sequenced so a functional slice
exists early and each later phase is independently verifiable.

---

## 1. Scope

**In scope — a working reference implementation:**

- A Java/Spring Boot **Scoring Engine** (§3.6) exposing `POST /v1/transactions/score`.
- Redis-backed **Feature Service** implementing the §7 data model against the
  exact key schema the existing `test_data/load_redis.py` writes.
- **Rules engine + decision blending** (§8.1, §8.3) driven by `cfg:rules`.
- **Embedded ML scorer** (§3.6): a `seed-v0` heuristic first, a real
  gradient-boosted model later — both behind one in-process interface.
- **Write path** with idempotency (§7.8–7.9) and an async **Transaction Store**.
- **Observability** (Micrometer, §10) and wiring the existing
  `load_test_dashboard_mockup.html` to a live metrics feed (§13.5).
- **Gatling load test** (§13.3) and the `test_data` functional gate as CI checks.

**Out of scope (infra/ops, not app code):**

- Production 10M-account / multi-region provisioning and the Redis Enterprise
  cluster topology (§7.12) — the app is written to run against it, but standing
  up regional stacks is an ops workstream.
- Cold-tier data lake / CDC pipeline (§4.3) — behind a `TransactionStore`
  interface; only the hot tier is implemented now.
- KYC, settlement, and the case-management review UI (§1.3 non-goals).

**Primary acceptance target:** all 11 requests in
`test_data/transactions/sample_test_requests.json` return their
`expected_decision` against a Redis loaded by `load_redis.py`, and the Gatling
steady-state scenario meets p50 ≤ 10 ms / p99 ≤ 30 ms on a test deployment.

---

## 2. The contract (already fixed by `test_data/`)

**Request** (`POST /v1/transactions/score`):

```json
{
  "transaction_id": "req_001",
  "timestamp": "2026-08-10T12:00:00+00:00",
  "customer_id": "cust_005",
  "customer_portfolio_country": "US",
  "receiver_account": "bene_003",
  "tpp_name_ud": "tpp_Alpha",
  "device_fingerprint": "device_005",
  "amount_usd": 65.0
}
```

> Note: fixtures pre-supply `amount_usd`. In production the gateway normalizes
> `amount`+`currency` → base currency at ingest (§3.2/§7.4); the engine consumes
> the normalized value. Keep an ingest normalization hook even though the
> fixtures skip it.

**Response** (superset of what the checker needs — it reads `decision`):

```json
{
  "transaction_id": "req_001",
  "decision": "approve",
  "final_score": 0.06,
  "model_version": "seed-v0",
  "rules_fired": [],
  "feature_snapshot": { "...": "..." },
  "degraded": false
}
```

**Redis keys the engine reads/writes** (hash-tagged per §7.3, from `load_redis.py`):

| Key | Type | Use |
|---|---|---|
| `c:{cid}:ring:payment` | LIST | positional metrics (`LINDEX`), ring buffer |
| `c:{cid}:agg:amount:1h:{YYYY-MM-DDTHH}` | HASH `cnt/sum/sumsq/min/max` | hot streaming aggregates |
| `c:{cid}:agg:amount:1d:{YYYY-MM-DD}` | HASH | warm streaming aggregates |
| `c:{cid}:pair:{bene}:state` | HASH `first_seen_ts/cnt_90d/sum_90d` | pair velocity / new-payee |
| `bene:{acct}:distinct_senders:last_24h` | SET (TTL 26h) | mule fan-in (`SCARD`) |
| `bene:{acct}:agg:amount:{1h,1d}:{b}`, `geo:{c}:…`, `tpp:{t}:…` | HASH | cross-entity aggregates |
| `bl:accounts`, `bl:devices` | SET | blacklists (`SISMEMBER`) |
| `list:vip_customers`, `list:watchlist` | SET | membership |
| `cfg:windows`, `cfg:metrics`, `cfg:rules` | HASH field→JSON | config-as-data |
| `decision:{transaction_id}` | STRING (TTL, NX) | **engine-written** idempotency guard (§7.8) |

**Config to honor:** `cfg:windows` (2), `cfg:metrics` (6 — the 3 §6.2 examples
plus `customer_txn_count_1h`, `bene_distinct_senders_24h`, `pair_txn_count_90d`),
`cfg:rules` (8 — R001/R002 hard-block, R003 hard-allow, R004–R008 soft).

**Master data** (`test_data/reference/*.json`) simulates a customer-profile
store the engine looks up (e.g. `beneficiary_country` for R007), *not* the Redis
feature store — implement as a `ReferenceDataService` behind an interface.

---

## 3. Tech choices (defaults — adjustable)

| Area | Choice | Rationale |
|---|---|---|
| Language / framework | Java 25 (LTS), Spring Boot 4.0 | §3.6 specifies Java/Spring Boot; Java 25 LTS on Spring Boot 4 / Spring Framework 7, virtual threads for the sync path. |
| Build | Maven | Ubiquitous; matches the Spring Boot skills. |
| Redis client (feature store, hot path) | Lettuce | Netty-multiplexed: many concurrent pipelined batches share one connection, no per-command pool checkout — the right profile for 1,000+ TPS (burst 5,000) against the 10 ms budget, plus mature Redis Cluster topology handling (§7.3). |
| Redis client (transaction store) | Jedis, via Redis OM Spring | Object mapping + Query Engine need Jedis; the store is a separate logical DB from the feature store (§7.12), so a second client to a second endpoint is natural, not a smell. |
| Hot-path Redis access (feature store) | Lettuce: pipelined/async batch read (§7.10) + single Lua `eval` write (§7.9), hash-tag-aware keys | One round trip each; fits the 10 ms budget. Low-level commands (`HMGET`/`HINCRBY`/`LINDEX`/`SCARD`), not object mapping. |
| Rules evaluation | Restricted SpEL over a typed feature map | Keeps rules data-driven (matches `cfg:rules` conditions) without hand-rolling a parser; lock down the context. |
| Embedded model | `seed-v0` heuristic → XGBoost/LightGBM exported to ONNX, run via ONNX Runtime (Java) in-process | §3.6/§8.2; no network hop; artifact shipped with the app. |
| Transaction Store (hot) | Redis JSON documents via **Redis OM Spring** (`@Document` + `@Indexed`/`@Searchable`, `EntityStream` for search/aggregation), async writer, behind a `TransactionStore` interface | §4.3 wants a low-latency *indexed document* store, not relational; Redis OM Spring gives Spring-idiomatic JSON mapping + the Query Engine (RediSearch) for review/dispute lookups and aggregations, on the same Jedis client. Separate DB from the feature store, so Redis Enterprise Auto Tiering (flash) is available for its 7–14 day window (§7.12 flash exclusion applies only to the feature working set). Cold/warehouse tier stays pluggable behind the interface. |
| Metrics | Micrometer → Prometheus + a `/metrics/live` SSE endpoint | Feeds §10 monitoring and the §13.5 live dashboard. |
| Tests | JUnit 5, Mockito, MockMvc, Testcontainers (`redis:8`) | Per `springboot-tdd`. |
| Load test | Gatling (Java DSL) | §13.3. |
| Local infra | `docker-compose` (`redis:8`) | One-command local bring-up; Redis 8 bundles the Query Engine (RediSearch), JSON, and probabilistic types into core, so no separate Stack image. |

---

## 4. Module layout

```
engine/                         # Spring Boot service (the synchronous scoring engine)
  src/main/java/com/redis/fraud/
    api/          ScoreController, ScoreRequest/ScoreResponse DTOs, AuthFilter
    config/       RedisConfigStore (windows/metrics/rules), read-through cache + Pub/Sub invalidation (§7.2)
    redis/        LettuceConfig (feature store), KeyBuilder (hash-tag aware), PipelinedReader, LuaWriter
    feature/      FeatureService + metric readers:
                    AggregateReader (cnt/sum/sumsq → mean/stddev, §7.4)
                    PositionalReader (ring buffer, §7.5)
                    DistinctCountReader (SCARD), PairStateReader, LastEventReader
                  ReferenceDataService (master-data lookups)
    rules/        RuleEngine, ConditionEvaluator, RuleResult (block/allow/signal)
    scoring/      ModelScorer (iface) → SeedV0Heuristic, OnnxGbtScorer
                  OverrideLayer (§8.3), DecisionBander (half-open bands)
    store/        TransactionStore (iface) → OmSpringTransactionStore (@Document JSON + Query Engine index/aggregation), AsyncWriter, IdempotencyGuard
    fallback/     DegradedModeHandler (§3.5)
    obs/          Metrics (Micrometer), LiveMetricsSseController (§13.5)
ml/                             # Python offline training → ONNX artifact + model registry
loadtest/                       # Gatling simulation + feeders drawn from test_data (§13.3)
test_data/                      # EXISTS — fixtures + run_sample_requests.py (functional gate)
load_test_dashboard_mockup.html # EXISTS — repoint from simulation to /metrics/live
docker-compose.yml              # redis:8 (Query Engine + JSON built in) for local dev
```

---

## 5. Phased delivery

Each phase ends at a **verifiable gate**. TDD throughout (`springboot-tdd`);
run the `springboot-verification` loop before each gate.

### Phase 0 — Scaffold & contract (walking skeleton)
- Spring Boot app, Java 25 + Spring Boot 4.0, Maven; `docker-compose` (`redis:8`); health endpoint.
- Lettuce client for the feature store + Redis OM Spring (Jedis) for the transaction store, virtual threads enabled; `KeyBuilder` with hash-tag support.
- `ScoreRequest`/`ScoreResponse` DTOs matching §2; `ScoreController` returns a
  hardcoded `approve`.
- **Gate M0:** `run_sample_requests.py --base-url http://localhost:8080` reaches
  the endpoint and prints a PASS/FAIL table (most will FAIL — that's expected).

### Phase 1 — Config + feature reads
- `RedisConfigStore`: load `cfg:windows/metrics/rules` into typed objects,
  read-through cache, Pub/Sub invalidation (§7.2, §3.6).
- Metric readers for all 6 fixture metrics; bucketed aggregate combine (§7.4:
  `count`, `stddev` from `cnt/sum/sumsq`), positional `LINDEX` (§7.5),
  `distinct_count` via `SCARD`, pair `cnt_90d`, last-event.
- `FeatureService`: assemble the feature vector in **one pipelined batch** (§7.10).
- `ReferenceDataService` loads `reference/*.json` (stand-in for master data).
- **Gate M1:** unit + Testcontainers tests assert each metric matches the values
  a `load_redis.py`-loaded Redis holds at the fixed reference time
  (e.g. `cust_012` 1h count = 9, `bene_018` distinct senders = 14).

### Phase 2 — Rules + decision → **functional gate**
- `RuleEngine`: evaluate `cfg:rules` conditions via restricted SpEL over the
  feature map; hard-block/allow short-circuit; soft rules emit weighted signals.
- `scoring/`: `SeedV0Heuristic` (final_score from soft-rule signals for now),
  `OverrideLayer` (no-op default), `DecisionBander` with half-open bands
  `[0,0.30) / [0.30,0.70) / [0.70,1.0]`.
- Populate `rules_fired`, `final_score`, `model_version=seed-v0`, `feature_snapshot`.
- **Gate M2 (primary):** all 11 `sample_test_requests.json` return their
  `expected_decision`. This is the headline functional milestone.

### Phase 3 — Write path, idempotency, persistence
- `LuaWriter`: post-decision update (§7.9) — ring push+trim, bucket increments
  across the applicable entities (customer, bene, pair, geo, tpp), distinct-sender
  `SADD`, last-event — as one atomic script per shard.
- `IdempotencyGuard`: `SET decision:{txn_id} … NX EX`; only the NX winner runs
  the write; retries return the cached decision (§7.8).
- `OmSpringTransactionStore` + `AsyncWriter`: persist the full record off the
  response path (§3.2 step 8) as a Redis OM Spring `@Document` (JSON), indexed by
  the Query Engine for review/dispute lookups and aggregations (§4.3 hot tier),
  non-blocking.
- **Gate M3:** replaying the same `transaction_id` twice does not double-count
  aggregates; a killed store write is buffered and retried, response unaffected.

### Phase 4 — Observability + live dashboard + fallbacks
- Micrometer: per-request pass/fail counters, latency histograms (p50/p99),
  per-stage timers (§3.3), rule-fire rates, decision-mix, degraded-mode counter (§10).
- `/actuator/prometheus` + `/metrics/live` SSE at ~1s (§13.5).
- Repoint `load_test_dashboard_mockup.html` from its simulation to `/metrics/live`
  (keep a `?sim=1` fallback).
- `DegradedModeHandler` (§3.5): model timeout → rules-only bias-to-review;
  partial feature failure → score on subset + `degraded:true`.
- **Gate M4:** dashboard shows real traffic from a local load run; killing the
  model path flips the engine to rules-only without failing requests.

### Phase 5 — Real ML model
- `ml/`: train XGBoost/LightGBM on the labeled backfill (`outcome_label`),
  handle class imbalance, evaluate PR-AUC, calibrate probabilities, export to ONNX.
- `OnnxGbtScorer` embedded via ONNX Runtime; feature-vector schema pinned to the
  `cfg:metrics` version and stamped in `model_version` (§8.2 coupling).
- Soft-rule signals fed in as model features; A/B via canary deploy, rollback by
  shipping the prior artifact; `seed-v0` remains the fallback.
- **Gate M5:** the trained model beats the `seed-v0` heuristic on held-out
  PR-AUC without regressing false-decline rate; `sample_test_requests` still pass.

### Phase 6 — Load test & validation
- `loadtest/`: Gatling sim per §13.3 — feeder from the seed corpus, signed auth
  token, 1–2% correctness-injection requests, scenarios steady/ramp/burst/soak/
  failure, assertions p50 < 10 ms / p99 < 30 ms / fail ≈ 0.
- Run against a dedicated test deployment sized toward §7.12 as practical (§13.4).
- Wire `run_sample_requests.py` and a short Gatling smoke into CI.
- **Gate M6:** steady-state and burst scenarios pass their assertions; soak
  surfaces no memory/GC regression.

---

## 6. Testing strategy

- **Unit:** metric combine math (mean/stddev/count), band edges, blending, rule
  conditions, idempotency logic.
- **Integration (Testcontainers):** `redis:8` — raw-Jedis feature reads/writes
  against a `load_redis.py`-equivalent fixture load, and Redis OM Spring JSON/Search
  store writes.
- **Contract/functional:** `run_sample_requests.py` as the golden gate (M2).
- **Load:** Gatling (M6).
- **Coverage:** JaCoCo gate per `springboot-verification`.

---

## 7. Key risks & open items (carried from design review)

- **Currency normalization** (resolved): normalize to base currency at ingest,
  before bucket increment; raw native amount stays on the record. Fixtures skip
  this (`amount_usd` pre-supplied) — keep the ingest hook regardless.
- **`min`/`max` in §7.4:** confirm any in-scope metric needs them before wiring
  the compare-and-set Lua (none of the 6 fixture metrics do — can defer).
- **Region routing owner** (§12): the engine assumes it receives only its
  home-region traffic; routing is an upstream concern to confirm.
- **`confirmed_legit` maturation window** (§12): needed before Phase 5 training
  labels are trustworthy.
- **Override-layer bound/expiry** (§8.3/§12): implement as no-op in Phase 2;
  design the bounded, expiring version before it carries real adjustments.
- **Redis OM Spring on Spring Boot 4:** SB4 / Spring Framework 7 is new — pin a
  Redis OM Spring version that supports it in Phase 0. If none is released yet,
  fall back to Jedis' native JSON/Search API for the Transaction Store (same
  client, less sugar) until OM Spring catches up.

---

## 8. Suggested immediate next step

Start **Phase 0** (scaffold + `docker-compose` + endpoint returning a stub
decision) so `run_sample_requests.py` runs end-to-end, then drive Phases 1–2 with
tests until Gate M2 (all 11 sample requests pass) is green.
