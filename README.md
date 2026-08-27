# Real-Time Transaction Fraud Scoring System

A reference implementation of the design in
[`Fraud_Scoring_System_Design_Doc.md`](Fraud_Scoring_System_Design_Doc.md): a
Java 25 / Spring Boot 4 engine that scores payment transactions synchronously
against a Redis feature store, evaluates configurable rules, optionally calls an
embedded ML model, persists every decision durably, and reports live metrics.

Build plan and phase-by-phase status: [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md).

## Architecture

Synchronous scoring path (§3.2); persistence happens off the response path so it
never adds caller latency.

```
   POST /v1/transactions/score
              │
              ▼
     ScoreController ──────────── idempotent retry short-circuit (§7.8)
              │
              ▼
   Feature assembly  ◄──────────  Redis feature store  (Lettuce, one pipelined
   (§7.10; degrades §3.5)         aggregates/ring/sets/cfg   batch per request)
              │
              ▼
     Rules engine (cfg:rules) ──▶ Scoring: seed-v0 heuristic, or embedded ML
              │                   model when fraud.model.enabled=true (§8.3)
              ▼
        decision returned
              │
   ── async, off the response path ──────────────────────────────
        │                                        │
        ▼                                         ▼
   Feature-store write +                  Signal-store update +
   signal 24h (atomic Lua, §7.9)          audit sink (§4.3, §3.5)
   idempotent on transaction_id           HLL/counters (§8.1)
```

Engine packages (`engine/src/main/java/com/redis/fraud/`):

| Package | Responsibility |
|---|---|
| `api` | REST endpoint + request/response DTOs |
| `feature` | metric readers + `FeatureService` (single pipelined batch); reference/master data (profiles, IP-geo) read from Redis |
| `rules` | data-driven condition DSL evaluator + `RuleEngine` (`cfg:rules`) |
| `scoring` | `ScoringService` (blend + decision bands), `ModelScorer` (toggleable embedded model) |
| `write` | atomic Lua feature-store write + idempotency guard |
| `signal` | streaming hot-window signals: `SignalWriter` (updates the 90d signals per txn) + `SignalReader` (reads 24h exact + 90d approximate into the feature vector) on the RAM signal store — HyperLogLog / counters / hashes + **RedisTimeSeries** (`SignalTimeSeries`, async `dispatch`) for velocity + behavioural signals (R019–R026) |
| `audit` | `TransactionSink` — every scored txn flows to the durable per-transaction audit "third store" (`NoOpTransactionSink` placeholder) via a resilient async writer |
| `fallback` | graceful degradation (§3.5) |
| `obs` | Micrometer metrics + `/metrics/live` |
| `redis`, `config` | Lettuce client / key builder; Redis-backed window/metric/rule config |

**Two RAM Redis databases by design** (§7.12): a **feature store** (`fraud.feature-store.uri`, default `:6379`) for 24h exact signals + aggregates + reference data, and a separate **signal store** (`fraud.signal-store.uri`, default `:6381`) for the 90-day approximate hot-window signals. **Both use Lettuce** (one multiplexed, pipelined connection model — no blocking pool). Signals use **core** Redis types (HyperLogLog for distinct fan-out/fan-in, counters for repeat declines, count/sum/sumsq hashes for the amount z-score) **plus RedisTimeSeries** for the velocity + behavioural signals (rate-vs-baseline, sustained elevation, circadian, machine cadence, amount bust-out trend, dormancy, device velocity surge, payee inbound velocity surge — R019–R026). All TimeSeries keys are hash-tagged by a **high-cardinality** entity (`{cid}`, `{device}`, `{bene}`) so load spreads evenly across shards — no low-cardinality hot key. TimeSeries is **bundled in Redis 8 and Redis Enterprise**, so no separate module install is needed; TS is driven from Lettuce via async `dispatch`. (The earlier Flex/OM-Spring transaction store was removed — see §8.1.) A separate database per region is a configuration change.

## Repository layout

```
engine/                 Spring Boot scoring engine (the service)
loadtest/               Gatling load test (§13.3)
ml/                     Offline model training (train.py) -> engine/.../model/model.json
test_data/              Laptop-scale fixtures + loader + functional checker
docker-compose.yml      two Redis DBs (feature :6379, signal :6381) for local dev
load_test_dashboard_mockup.html   Live load-test dashboard (§13.5)
```

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| JDK | **25** | Spring Boot 4 requires a modern JDK. `java -version` must report 25, or set `JAVA_HOME`. |
| Docker | any recent | Runs Redis, and the Testcontainers integration tests. |
| Python | 3.10+ | For the fixture loader and functional checker. |
| Maven | not needed | Use the bundled wrapper `engine/mvnw`. |

> **No Redis modules required.** Both stores use only core Redis types
> (String/Hash/Set/HyperLogLog), so any recent Redis works. The provided
> `docker-compose.yml` uses `redis:8`.

Set `JAVA_HOME` if 25 isn't your default JDK, e.g. on macOS + Homebrew:
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 25 2>/dev/null || echo /opt/homebrew/opt/openjdk)
```

---

## Quick start (laptop)

All commands are run from the repository root unless noted.

### 1. Start Redis
```bash
docker compose up -d
```
This starts **two** Redis databases: the feature store on `localhost:6379` and
the signal store on `localhost:6381`. If those ports are in use, edit the `ports`
mappings in `docker-compose.yml` and pass the new locations to the engine
(`fraud.feature-store.uri` / `fraud.signal-store.uri`) and to the loader
(`--redis-url`, feature store) shown below.

### 2. Load the seed fixtures
The loader needs the `redis` Python package:
```bash
python3 -m venv .venv && . .venv/bin/activate && pip install redis
python3 test_data/load_redis.py --flush
```
This replays the 482-transaction backfill through the write-path logic so the
feature store is warm (velocity aggregates, ring buffers, blacklists, reference
profiles/IP-geo, and the `cfg:*` config), not empty. It targets the **feature
store** DB (`--redis-url`, default `redis://localhost:6379`); the transaction
store fills at runtime as the engine scores. See
[`test_data/README.md`](test_data/README.md) for the encoded scenarios.

> The hot-window rules **R012** (repeat declines) and **R013** (beneficiary fan-out)
> read the **signal store** (§8.1). They fire once it holds representative signals —
> which build up live as the engine scores, or can be pre-warmed with the job below
> (optional for the 11 sample requests, which don't exercise R012/R013; recommended
> for the load test).

### 3. Build and test the engine
```bash
cd engine
./mvnw test        # unit tests only (no Docker needed)
./mvnw verify      # full suite incl. Testcontainers integration tests (needs Docker)
```

### 4. Run the engine
With Redis up (step 1) and fixtures loaded (step 2):
```bash
cd engine
./mvnw spring-boot:run
```
Or run the packaged jar (build it with `./mvnw package -DskipTests`):
```bash
java -jar engine/target/fraud-scoring-engine-0.0.1-SNAPSHOT.jar
```
The service listens on `http://localhost:8080` by default and connects to the
feature store (`redis://localhost:6379`) and the signal store
(`redis://localhost:6381`). Both must be reachable at startup.

> Reference/master data (customer & beneficiary profiles, IP-geo) is read from
> Redis, populated by the fixture loader in step 2 — the engine reads no local
> reference files, so it can run from any directory.

#### (Optional) Pre-warm the signal store — rules R012/R013/R005
The signal store warms itself as the engine scores, but for a load test you can
pre-warm the 90-day signals so R012/R013/R005/amount fire from the first request.
`test_data/seed_signal_store.py` writes the engine's `SignalKeys` layout
(HyperLogLog fan-out/fan-in, declines counters, amount stats) in parallel:
```bash
python3 test_data/seed_signal_store.py \
  --host localhost --port 6381 --customers 5000 --flagged 20 --id-width 3 --procs 8
```
`--flagged N` makes `cust_1..cust_N` high-risk (≥3 declines, >15 distinct
beneficiaries) so R012/R013 fire for them. **`--id-width 3` and `--customers` must
match the Gatling feeder and the feature seeder** (`cust_%03d`). Shard across VMs
with `--customer-start` + `--total-customers`. There is no index to build and no
document store to size — the signals are bounded core-type structures (§8.1).

### 5. Functional check
In another shell (stdlib only, no extra installs):
```bash
python3 test_data/run_sample_requests.py --base-url http://localhost:8080
```
Expect **11/11 passed** — each sample request returns its expected
approve / review / decline decision.

### 6. (Optional) enable the ML model
By default the model is bypassed (`fraud.model.enabled=false`, seed-v0
rules-only). To train and enable it:
```bash
python3 ml/train.py          # writes engine/src/main/resources/model/model.json
cd engine && ./mvnw spring-boot:run -Dspring-boot.run.arguments=--fraud.model.enabled=true
```
Scored responses then carry `model_version: logreg-v1`. (This laptop model is
illustrative — see [Known limitations](#known-limitations).)

### 7. Live dashboard
The dashboard simulates data by default. To point it at the running engine,
serve it over HTTP (a `file://` page can't read the query string or do the
cross-origin fetch reliably):
```bash
python3 -m http.server 8090        # from the repo root
```
Open **http://localhost:8090/load_test_dashboard_mockup.html?live=http://localhost:8080**.
The `/metrics/live` endpoint is CORS-open so the dashboard can read it.

---

## Configuration

Pass as `--flag=value` (jar) or `-Dspring-boot.run.arguments=--flag=value`
(`spring-boot:run`), or set in `application.yml`.

| Property | Default | Purpose |
|---|---|---|
| `server.port` | `8080` | HTTP port. |
| `fraud.feature-store.uri` | `redis://localhost:6379` | RAM feature store (Lettuce). |
| `fraud.signal-store.uri` | `redis://localhost:6381` | RAM signal store (Lettuce): 90-day hot-window signals — HyperLogLog / counters / hashes (R005/R012/R013/R018) + RedisTimeSeries for velocity + behavioural rules (R019–R026). TimeSeries is bundled in Redis 8 / Redis Enterprise. Replaces the old Flex transaction store. |
| `fraud.redis.log-calls` | `true` | Log one line per Redis call (command type + latency) for both stores. Verbose at high TPS — set `false` for a full-scale load run. |
| *(reference data)* | — | Customer/beneficiary profiles and IP-geo are **loaded into Redis** by the fixture loader (`load_redis.py`, §7.2) and read from there; the engine has no reference-data file path. |
| `fraud.model.enabled` | `false` | `true` loads and calls the embedded model; `false` bypasses it entirely. |
| `fraud.seed.*` | *(see §13.2)* | Transaction-store seeder job, active only under `--spring.profiles.active=seed`: `customers`, `days`, `txns-per-customer`, `flagged`, `id-format`, `customer-start`, `flush`, plus throughput knobs `writers` (32) and `pipeline` (1000). |

### Observability endpoints
- `GET /actuator/health` — liveness/readiness.
- `GET /actuator/prometheus` — Micrometer metrics (latency percentiles, decision mix, per-rule fire rates, degraded counter).
- `GET /metrics/live` — compact JSON snapshot for the live dashboard (§13.5).

### Per-Redis-call latency logging
With `fraud.redis.log-calls=true` (default), the engine logs one line per Redis
call on both stores, e.g.:
```
redis-call store=feature op=HMGET took_ms=0.42
redis-call store=signal op=PFCOUNT took_ms=0.31
redis-call store=signal op=PFADD took_ms=0.12
```
Both stores use Lettuce, so their commands are captured at the driver level (a
`CommandListener`, one line per command in a pipelined batch with its own latency).
Latency is measured with
`System.nanoTime()` around issue→result. Silence it without disabling entirely via
`logging.level.com.redis.fraud.redis.RedisCallLog=WARN`.

---

## Load testing

The Gatling simulation (`loadtest/`) drives seeded traffic with a small hot-key
skew and ~2% correctness-injection (blacklist / velocity), and asserts the §2.2
SLA: p50 < 10 ms, p99 < 30 ms, failures < 1%.

Scenarios (via `-Dscenario`): `smoke` (default), `steady` (1,000 TPS/15 min),
`ramp` (0→1,000/2 min), `burst` (4,000 TPS/5 min), `soak` (1,000 TPS/2 h),
`failure` (steady while a dependency is degraded).

### Local smoke (laptop)
A scaled-down harness check against a locally running engine:
```bash
engine/mvnw -f loadtest/pom.xml \
  -DbaseUrl=http://localhost:8080 -Dscenario=smoke \
  test-compile io.gatling:gatling-maven-plugin:4.21.10:test
```
The HTML report is written to `loadtest/target/gatling/<run>/index.html`.

> A laptop cannot sustain 1,000+ TPS meaningfully. The smoke proves the harness
> and low-latency behavior; the full-scale scenarios below need real
> infrastructure (§13.4).

### Full-scale load test on VM infrastructure

Run the load generator on a **separate VM** from the engine so the two don't
compete for CPU, and keep them in the same low-latency network (same
VPC / availability zone). A dedicated, region-scoped test deployment — never
production (§13.4).

**Suggested topology**

| Role | VM(s) | Notes |
|---|---|---|
| Redis (feature store) | RAM BDB | Redis / Redis Enterprise, sized per §7.12 (~120 GB for 5M). Plain Redis — no modules. |
| Redis (signal store) | RAM BDB | Separate plain-Redis DB for the 90d approximate signals (HyperLogLog / counters / stats — core types, no modules, no flash). Bounded RAM. |
| Scoring engine | 1–N behind a load balancer | Stateless (§3.6), so scale horizontally. JDK 25. |
| Load generator | 1 | JDK 25 + this repo's `loadtest/`. |

See [`LOAD_TEST_RUNBOOK.md`](LOAD_TEST_RUNBOOK.md) for the full GCP runbook (LB setup,
firewall, per-VM commands). Summary of the steps:

1. **Redis:** provision the two RAM databases — feature store + signal store. Note both host/ports.
2. **Seed the feature store:** config + blacklists via `load_redis.py`, then the 5M-customer
   footprint via the parallel `seed_feature_store.py` (see the runbook §3):
   ```bash
   python3 test_data/load_redis.py --flush --redis-url redis://<feature-db-host>:6379
   python3 test_data/seed_feature_store.py --host <feature-db-host> --port 6379 \
     --customers 5000000 --id-width 3 --procs 12
   ```
2b. **Pre-warm the signal store (for R012/R013/R005):** so the hot-window rules fire from
   the first request. Parallel, matching the Gatling identity space (`cust_%03d`):
   ```bash
   python3 test_data/seed_signal_store.py --host <signal-db-host> --port 6381 \
     --customers 5000000 --flagged 50000 --id-width 3 --procs 12
   ```
   No index to build, no document store to size — the signals are bounded core-type
   structures. Shard across VMs with `--customer-start` + `--total-customers`.
3. **Engine:** on each engine VM, deploy the jar and start it against the two stores:
   ```bash
   java -Xms4g -Xmx4g -XX:+UseZGC \
     -jar fraud-scoring-engine-0.0.1-SNAPSHOT.jar \
     --server.port=8080 \
     --fraud.feature-store.uri=redis://<feature-db-host>:6379 \
     --fraud.signal-store.uri=redis://<signal-db-host>:6381 \
     --fraud.redis.log-calls=false
   ```
   Put the engine VMs behind a load balancer and use its URL as `baseUrl` below.
4. **Load generator:** from the generator VM, run the desired scenario against
   the engine (or LB) URL, tuning the SLA thresholds if needed:
   ```bash
   engine/mvnw -f loadtest/pom.xml \
     -DbaseUrl=http://<engine-or-lb-host>:8080 \
     -Dscenario=steady \
     -Dp50Max=10 -Dp99Max=30 -DfailMax=1.0 \
     test-compile io.gatling:gatling-maven-plugin:4.21.10:test
   ```
   Run `burst` and `soak` the same way. For `failure`, start the `failure`
   scenario and, mid-run, degrade a dependency (e.g. kill a Redis node) to
   confirm the engine stays within budget in degraded mode (§3.5).
5. **Results:** Gatling prints a summary and pass/fail for each assertion, and
   writes an HTML report to `loadtest/target/gatling/<run>/`. Copy it off the VM
   to review. Watch the live dashboard (step 7 above, `?live=http://<engine>:8080`)
   or `/actuator/prometheus` during the run.

---

## Regenerating fixtures and retraining

- **Fixtures:** edit the constants at the top of `test_data/generate_test_data.py`
  (customer count, country weights, injected scenarios) and re-run it; it is
  deterministic (seeded). Then reload with `test_data/load_redis.py`.
- **Model:** `python3 ml/train.py` re-derives weak labels from the injected
  patterns and rewrites `engine/src/main/resources/model/model.json`. Rebuild the
  engine to pick it up.

---

## Known limitations

- **ML model is illustrative.** The laptop fixtures have no ground-truth
  `outcome_label`, so the model is trained on weak labels derived from the
  injected fraud patterns (with deliberate leakage). It demonstrates the serving
  pipeline and the enable/bypass toggle, not production accuracy. It uses a
  pure-Java logistic scorer behind the `ModelScorer` interface; an ONNX Runtime
  scorer can drop in unchanged. A real model needs labelled seed data (§12).
- **Seed data is laptop-scale** (50 customers). Load-test numbers are only
  representative at prod-like Redis sizing and seed volume (§7.11–7.12, §13.4).
- **Full 1,000 TPS / p99 ≤ 30 ms SLA** is validated on VM infrastructure, not a
  laptop.
- Open design questions (region routing owner, per-region availability scope,
  `confirmed_legit` maturation window, override-layer bounds) are tracked in
  §12 of the design doc.
```
