# Fraud Scoring — Load Test Runbook (GCP)

Step-by-step to configure and run the load test against the fraud-scoring engine
and its two RAM Redis stores. (The Flex transaction store has been removed — the
90-day hot-window signals are now computed as streaming signals on a plain-Redis
signal store; there are **no search indexes** to create anymore.)

## 0. Starting assumptions (already done)

You have already provisioned:

| Component | Setup |
|---|---|
| **Feature store cluster** | 3× `n2d-highmem-8`, no disks, 9 shards, all-master (no HA). Holds 24h exact signals + aggregates + reference. **Plain Redis — no modules required.** |
| **Signal store cluster** | 3× `n2d-highmem-8`, no disks, all-master (no HA). Holds 90d signals: HyperLogLog / counters / hashes + **fixed-width bucket hashes with per-field TTL** (`HEXPIRE`) + a capped events list (velocity + behavioural rules R017/R019–R026). **Core types only — no modules.** Bounded constant-size reads. No flash. |
| **Engine VMs** | 3× `e2-standard-8`, JDK 25 installed. |
| **Gatling VM** | 1× `e2-standard-8`, JDK 25 installed. |

> The signal store is now RAM-only and **bounded** (a few KB per entity: HLLs cap at
> ~12 KB, plus small counters/stats) — no more billion-doc index, no Flex/NVMe, no
> RAM-ceiling. **No modules** on the signal store; the velocity + behavioural signals
> use core **fixed-width bucket hashes with per-field TTL** (`HEXPIRE`, Redis 8), so each
> read returns a bounded, constant-size reply (flat egress under soak).

**This runbook covers what's left:** the engine **load balancer**, engine
deploy/config, loading + pre-warming the two stores, and running/monitoring the test.

Record these values up front — you'll paste them into the commands below:

```
FEATURE_HOST=<feature-store db endpoint host>
FEATURE_PORT=<feature-store db port>        # e.g. 12000
SIGNAL_HOST=<signal-store db endpoint host>
SIGNAL_PORT=<signal-store db port>          # e.g. 13000
REDIS_PASS=<db password, or empty if none>
CUSTOMERS=5000000     HIGH_RISK=50000       # full scale; must match across both seeders + Gatling
SEED_DAYS=90                                # feature-store history depth (biggest seed-time lever)
VPC=<vpc network>   SUBNET=<subnet>   REGION=<region>   ZONE=<zone>
```

> **No-HA reminder:** these clusters have no replica. A node loss = permanent data
> loss for its shards. Fine for a load test; enable AOF/backups before anything real.

---

## 1. Prerequisites per VM

- **Engine VMs (×3):** JDK 25; the engine jar (built in step 2).
- **Gatling VM:** JDK 25; a clone of this repo (for `loadtest/` + `engine/mvnw`).
- **Loader** (run from any VM that can reach both stores): `python3` + `pip install redis`.
- **Firewall rules** (create if not present):
  - Engine VMs: allow ingress `tcp:8080` from the LB + the GCP health-check ranges
    `35.191.0.0/16` and `130.211.0.0/22`.
  - Engine VMs → Redis: allow egress to `FEATURE_PORT` / `SIGNAL_PORT`.
  - Gatling VM → LB VIP: allow `tcp:8080`.

---

## 2. Build the engine jar (once, on any JDK-25 machine)

```bash
cd engine
./mvnw package -DskipTests
# produces engine/target/fraud-scoring-engine-0.0.1-SNAPSHOT.jar
```

Copy the jar to each of the 3 engine VMs:

```bash
gcloud compute scp engine/target/fraud-scoring-engine-0.0.1-SNAPSHOT.jar \
  <engine-vm>:~/fraud-engine.jar --zone=$ZONE   # repeat per VM
```

---

## 3. Load the feature store (config + 5M customers)

**3a. Config + blacklists + reference** (small, required — the engine reads `cfg:*`
on startup):

```bash
pip install redis   # once
python3 test_data/generate_test_data.py     # (re)generate fixtures if not present
python3 test_data/load_redis.py --flush \
  --redis-url redis://${REDIS_PASS:+:$REDIS_PASS@}$FEATURE_HOST:$FEATURE_PORT
```

**3b. Bulk-load the 5M customers' footprint** — `seed_feature_store.py` is
multiprocessing (one worker process per core, deep pipelines, so it bypasses the GIL
and saturates the cluster's write path). Run **without** `--flush` so it keeps the
config from 3a. Set `--procs` to `nproc-1` on the seeder VM.

This is the heavy seed: 5M customers × `SEED_DAYS` ≈ **~10 billion write ops** at 90
days (~1,940 ops/customer; the daily buckets are ~82% of that, so `SEED_DAYS` is the
big lever). **Shard it across several seeder VMs in the same VPC** (low RTT to Redis)
over disjoint `--customer-start` ranges, all passing the same `--total-customers`:

```bash
# 4-VM shard (each on its own high-core seeder VM). --total-customers keeps the
# per-customer bene-pool + extrapolation identical across shards.
# VM 1:
python3 test_data/seed_feature_store.py --host $FEATURE_HOST --port $FEATURE_PORT \
  ${REDIS_PASS:+--password $REDIS_PASS} --id-width 3 --days $SEED_DAYS --procs 15 \
  --total-customers 5000000 --customer-start 0        --customers 1250000
# VM 2:  --customer-start 1250000 --customers 1250000
# VM 3:  --customer-start 2500000 --customers 1250000
# VM 4:  --customer-start 3750000 --customers 1250000
```

Single VM (dev / smaller runs): drop the shard args and pass `--customers $CUSTOMERS`.

**Throughput / time:** each 15-proc seeder VM sustains ~0.3–0.6M ops/s against a
same-VPC RAM cluster, so 4 VMs ≈ ~1.5–2.4M ops/s → **~1.5 h for the full 90-day, 5M
seed**; a single VM is ~5–9 h. `--days 30` cuts the op count ~55% (~40 min on 4 VMs);
`--days 7` cuts it ~77%. Watch the feature store's `used_memory` climb to ~120 GB.

> Order matters: `load_redis.py --flush` first (owns the one flush), then
> `seed_feature_store.py` **without** `--flush`. The seeders write per-customer data
> only, **not** `cfg:*` — if you flush after 3a you wipe the config and the engine
> logs `Loaded config: 0 windows, 0 metrics, 0 rules`.

---

## 4. Pre-warm the signal store (90d hot-window signals)

So R012/R013/R005/amount fire from the first request instead of a cold store. This
writes the engine's `SignalKeys` layout (HLL fan-out/fan-in, declines counters,
amount stats) for the current rotating month:

```bash
python3 test_data/seed_signal_store.py \
  --host $SIGNAL_HOST --port $SIGNAL_PORT ${REDIS_PASS:+--password $REDIS_PASS} \
  --customers $CUSTOMERS --flagged $HIGH_RISK --id-width 3 --procs 12
```

`--flagged $HIGH_RISK` makes `cust_1..cust_HIGH_RISK` high-risk (≥3 declines, >15
distinct benes) so R012/R013 fire for them. **`--id-width 3` and `--customers` must
match the feature seeder and the Gatling feeder** (all `cust_%03d`). Set `--procs` to
`nproc-1`; shard across VMs with `--customer-start` + `--total-customers` if needed.

**Throughput / time:** this is light — ~20 write-ops/customer (HLL/counter/hash, no
daily history) ≈ ~100M ops for 5M → **~5–10 min on a single seeder VM**. No sharding
needed at 5M.

> The seeder pre-warms the distinct/count/amount signals (R005/R012/R013/R018). The
> **Behavioural bucket-hash signals (R017, R019–R026** — velocity vs baseline, sustained
> elevation, circadian, cadence, bust-out, dormancy, device/payee velocity surge) **build up live** as
> the engine scores during the run; they need no pre-seed (and by design need history
> to accumulate before they engage).

---

## 5. Deploy + start the engine on each of the 3 VMs

Set `log-calls=false` (per-call logging is far too verbose at load). Note: **no
transaction-store / `spring.data.redis` / `hot-window` flags anymore** — just the two
store URIs.

```bash
java -Xms4g -Xmx4g -XX:+UseZGC \
  -jar ~/fraud-engine.jar \
  --server.port=8080 \
  --fraud.feature-store.uri=redis://${REDIS_PASS:+:$REDIS_PASS@}$FEATURE_HOST:$FEATURE_PORT \
  --fraud.signal-store.uri=redis://${REDIS_PASS:+:$REDIS_PASS@}$SIGNAL_HOST:$SIGNAL_PORT \
  --fraud.redis.log-calls=false \
  --fraud.model.enabled=false
```

Recommended: run as a **systemd service**. Example `/etc/systemd/system/fraud-engine.service`:

```ini
[Unit]
Description=Fraud Scoring Engine
After=network-online.target
[Service]
ExecStart=/usr/bin/java -Xms4g -Xmx4g -XX:+UseZGC -jar /home/%i/fraud-engine.jar \
  --server.port=8080 \
  --fraud.feature-store.uri=redis://FEATURE_HOST:FEATURE_PORT \
  --fraud.signal-store.uri=redis://SIGNAL_HOST:SIGNAL_PORT \
  --fraud.redis.log-calls=false
Restart=on-failure
[Install]
WantedBy=multi-user.target
```
`sudo systemctl daemon-reload && sudo systemctl enable --now fraud-engine`

**Verify each engine VM before wiring the LB:**
```bash
curl -s http://localhost:8080/actuator/health      # {"status":"UP"}
```

---

## 6. Set up the engine load balancer (GCP)

An **internal passthrough Network Load Balancer** (L4, regional) keeps Gatling and
the engine in the same VPC with minimal overhead.

```bash
# 6.1 Health check on the engine's readiness endpoint
gcloud compute health-checks create http engine-hc \
  --port=8080 --request-path=/actuator/health \
  --check-interval=5s --healthy-threshold=2 --unhealthy-threshold=3

# 6.2 Unmanaged instance group with the 3 engine VMs
gcloud compute instance-groups unmanaged create engine-ig --zone=$ZONE
gcloud compute instance-groups unmanaged add-instances engine-ig --zone=$ZONE \
  --instances=engine-vm-1,engine-vm-2,engine-vm-3
gcloud compute instance-groups set-named-ports engine-ig --zone=$ZONE \
  --named-ports=http:8080

# 6.3 Internal backend service, attach the group
gcloud compute backend-services create engine-bes \
  --load-balancing-scheme=INTERNAL --protocol=TCP --region=$REGION \
  --health-checks=engine-hc
gcloud compute backend-services add-backend engine-bes --region=$REGION \
  --instance-group=engine-ig --instance-group-zone=$ZONE

# 6.4 Internal forwarding rule (assigns the VIP)
gcloud compute forwarding-rules create engine-fr \
  --load-balancing-scheme=INTERNAL --region=$REGION \
  --network=$VPC --subnet=$SUBNET \
  --backend-service=engine-bes --ip-protocol=TCP --ports=8080

# 6.5 Grab the VIP
LB_VIP=$(gcloud compute forwarding-rules describe engine-fr --region=$REGION \
  --format='value(IPAddress)')
echo "Engine LB: http://$LB_VIP:8080"
```

Make sure the firewall rule from step 1 allows the health-check ranges
(`35.191.0.0/16`, `130.211.0.0/22`) to `tcp:8080`, or all backends show unhealthy.

**Verify from the Gatling VM:** `curl -s http://$LB_VIP:8080/actuator/health`

---

## 7. Tune the Gatling VM, then run the load test

On the **Gatling VM** first (avoids ephemeral-port exhaustion at high TPS):
```bash
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"
sudo sysctl -w net.ipv4.tcp_tw_reuse=1
ulimit -n 1048576
```

`baseUrl` = the LB VIP. `-Dcustomers`/`-DhighRisk` **must match** the seeders'
`--customers`/`--flagged`. Run the full-scale test in three steps:

**1) Warm-up ramp (0 → 1000 TPS over 2 min)** — confirms the LB + all 3 engines take
load cleanly and finds any early ceiling before the sustained run:
```bash
engine/mvnw -f loadtest/pom.xml -DbaseUrl=http://$LB_VIP:8080 \
  -Dscenario=ramp -Dcustomers=$CUSTOMERS -DhighRisk=$HIGH_RISK \
  -Dp50Max=1000000 -Dp99Max=1000000 -DfailMax=100 \
  test-compile io.gatling:gatling-maven-plugin:4.21.10:test
```

**2) Steady state — 1000 TPS for 15 min (the target run), SLA-asserted:**
```bash
engine/mvnw -f loadtest/pom.xml -DbaseUrl=http://$LB_VIP:8080 \
  -Dscenario=steady -Dcustomers=$CUSTOMERS -DhighRisk=$HIGH_RISK \
  -Dp50Max=10 -Dp99Max=30 -DfailMax=1.0 \
  test-compile io.gatling:gatling-maven-plugin:4.21.10:test
```

**3) (Optional) burst 4000 TPS / 5 min and soak 1000 TPS / 2 h** — `-Dscenario=burst`
/ `-Dscenario=soak`, same flags.

Notes:
- If step 2 fails the SLA, relax `-Dp50Max/-Dp99Max/-DfailMax` to `1000000/1000000/100`
  to get a throughput/behaviour read, then read the real p50/p99 from `/metrics/live`.
- `-Dscenario=func -Dtps=<N> -DdurationSec=<s>` is the tunable form for an arbitrary rate.

Gatling writes an HTML report to `loadtest/target/gatling/<run>/index.html` — copy it
off the VM.

---

## 8. Monitor during the run

**Engine** (per VM, or through the LB):
```bash
curl -s http://$LB_VIP:8080/metrics/live            # processed/approve/review/decline/degraded/p50/p99
curl -s http://<engine-vm>:8080/actuator/prometheus | grep -E \
  'fraud_rule_fired_total|process_cpu_usage|jvm_memory_used_bytes|jvm_threads_live'
```
Watch: `p99_ms` vs SLA, `degraded` (should stay 0), decision mix, per-rule fires
(`R012`/`R013`/`R005` non-zero → the signal pre-warm is working; `R014`–`R018` fire
under load as 24h/velocity/amount signals build up).

**Redis (both stores)** — plain Redis now, so watch memory + ops with `redis-cli`
(or the Enterprise console / `:9443` REST API):
```bash
redis-cli -h $SIGNAL_HOST -p $SIGNAL_PORT ${REDIS_PASS:+-a $REDIS_PASS} INFO stats | grep instantaneous_ops
redis-cli -h $SIGNAL_HOST -p $SIGNAL_PORT ${REDIS_PASS:+-a $REDIS_PASS} INFO memory | grep used_memory:
```
The signal store is bounded RAM (no index, no flash) — memory should stay flat/small
relative to the feature store. `evicted_keys` should stay 0.

---

## 9. Between runs / teardown

- **Reset a store** between configurations: `redis-cli … FLUSHALL` (no indexes to drop),
  then re-run the relevant seeder (step 3b / 4). Re-run `load_redis.py` (no `--flush`)
  if you flushed the feature store, to restore `cfg:*`.
- **Restart engines** to reset in-process counters (`/metrics/live` is cumulative).
- **Delete the Gatling VM** when done (it's transient).

---

## Sizing — GCP environment for 5M customers @ 1000 tx/s (90-day window, no HA)

Figures below are **measured on a laptop docker-compose run** (per-customer/-series
memory deltas + engine CPU at 500 & 1000 tx/s), then extrapolated to 5M — much more
accurate than the earlier estimates.

| Tier | VMs | Shards | Measured basis (extrapolated to 5M) |
|---|---|---|---|
| **Feature store** (RAM) | **3× `n2d-highmem-8`** (8 vCPU / 64 GB = 192 GB) | **9 master** | **20.6 KB/customer × 5M ≈ ~105 GB** (~55% util); reads are trivial RAM lookups |
| **Signal store** (RAM, core types) | **3× `n2d-highmem-8`** (8 vCPU / 64 GB = 192 GB) | **9 master** | **< 20 GB** = HLL/counters/z-score hash (~0.35 KB/cust ≈ 1.8 GB) + bounded bucket hashes + capped events list (~1–3 KB/active cust). Far lower than the earlier TimeSeries estimate (~90 GB) — the bucket hashes hold only fixed windows, not full-resolution series. Comfortable headroom; single-node would suffice on RAM, keep 3×/9-shard for CPU + HA. |
| **Engine** | **3× `e2-standard-4`** (4 vCPU / 16 GB) behind an internal L4 LB | — | **~1.1 ms CPU/txn** (measured) → ~1.1 core @ 1000 tx/s → ~0.4 core/VM; 3 VMs for LB + N+1 + burst headroom. `-Xmx2g` |
| **Gatling** | **1× `e2-standard-8`** (transient) | — | tune sysctl (§7); **2 injectors** for the 4k burst scenario |

Sizing notes:
- **The engine is far lighter than first estimated** — measured ~1.1 ms CPU/txn (not ~5–7), so 1000 tx/s needs only ~1 core total. `e2-standard-4` ×3 is generous; the 3 VMs are for availability/LB/burst, not raw throughput. Heap drops to `-Xmx2g` (working set is small).
- **The signal store is no longer a RAM driver.** Replacing the per-customer velocity/amount TimeSeries (which grew to ~90 GB and, worse, returned ever-larger `TS.RANGE` replies under soak) with fixed-width bucket hashes bounds both RAM and per-call egress: each customer holds ≤ ~170 hourly + ≤ 24 five-min velocity buckets, ≤ 8 weekly amount buckets, a ≤ 64-element events list, and a last-event marker — all self-trimming via per-field TTL. The amount z-score still comes from the count/sum/sumsq monthly hash; R023 bust-out trend now reads the small weekly bucket hashes.
- **No-HA / single copy.** For HA, add one replica per shard → ~2× the Redis node count.
- **Burst 4–5k tx/s:** the engine is stateless — scale to 5–6 VMs; both Redis tiers have ample headroom. Signal-store reads are now bounded constant-size `HGETALL`s (no growing `TS.RANGE`), so per-call cost is flat; watch per-shard CPU for key-skew, not reply growth.
- **Approx cost** (GCP on-demand, us-central1): ~$1.9k/mo (feature ~$0.7k + signal ~$0.7k + engines ~$0.3k + Gatling ~$0.2k); ~**$1.2k/mo with a 1-yr CUD**. Verify in the pricing calculator.

---

## Config quick-reference — what to set and where

| Setting | Where | Value |
|---|---|---|
| Feature store endpoint | engine flag `--fraud.feature-store.uri` | `redis://[:pass@]FEATURE_HOST:FEATURE_PORT` |
| Signal store endpoint | engine flag `--fraud.signal-store.uri` | `redis://[:pass@]SIGNAL_HOST:SIGNAL_PORT` |
| Per-call Redis logging | engine flag `--fraud.redis.log-calls` | **`false`** for load runs |
| ML model | engine flag `--fraud.model.enabled` | `false` (rules-only) |
| JVM | `java` args | `-Xms4g -Xmx4g -XX:+UseZGC` |
| HTTP port | engine flag `--server.port` | `8080` |
| Feature-store config | `test_data/load_redis.py --redis-url` (no `--flush` on reload) | `cfg:*`, blacklists, profiles |
| Feature-store customers | `test_data/seed_feature_store.py --host/--port --customers --id-width 3` | 24h + aggregate footprint |
| Signal-store pre-warm | `test_data/seed_signal_store.py --host/--port --customers --flagged --id-width 3` | 90d HLL/counters/stats |
| Load driver | Gatling `-D` props | `baseUrl`(LB VIP), `scenario`, `tps`, `durationSec`, `customers`, `highRisk`, `p50Max/p99Max/failMax` |

**Alignment rules that bite if you skip them:**
1. `cust_%03d` everywhere — Gatling (hardcoded), `seed_feature_store.py --id-width 3`,
   `seed_signal_store.py --id-width 3`. A wider width zero-pads low ids and breaks the match.
2. `--customers` / `--flagged` on both seeders = Gatling `-Dcustomers` / `-DhighRisk`.
3. Load the feature-store config (`load_redis.py`) **before** the feature seeder, and
   never `--flush` after the config is in.

---

## Feedback-loop run (large scale, 5M / 1,000 tx/s)

Runs the continuous-learning loop (design doc §8.4) at production scale so the live
cross-instance dashboard (§10.3) shows **fraud detection rising and false positives
falling** as the loop retrains. Builds on the load-test topology above; adds the audit
stream, label pipeline, aggregator/dashboard, and the retrain/fast loops. Small-scale
equivalents are proven in CI (`pipeline/` + engine tests); this is the distributed run.

**Collapsed store:** feature + signal stores are one Redis (design doc §13.6.6) — point
both `fraud.feature-store.uri` and `fraud.signal-store.uri` at it.

### 0. Prerequisites & vars (feedback-loop run)
```
STORE=<collapsed Redis endpoint host>       # one cluster; see "Redis cluster sizing" below
LB_VIP=<internal L4 LB VIP>                 # fronts the engines (§6)
DATA=/data/parquet                          # shared path (Filestore mount) OR a gs:// bucket
MODELS=/models                              # shared path engines read fraud.model.path from
```
- Pipeline VM: `pip install -r pipeline/requirements.txt` (redis, pyarrow, scikit-learn, duckdb, fastapi/uvicorn).
- Shared storage for `$DATA` / `$MODELS` — see "Shared storage" below (GCS bucket recommended;
  Filestore if you want a plain POSIX mount).
- The audit sink writes the `txn:events` stream in the collapsed store; the Parquet writer
  drains it to `$DATA` (system of record); DuckDB in `retrain_loop` reads it back.

### 1. Seed (D3-large)
```bash
# reference data + cfg (rules incl. the blunt device rule + cfg:bands) into the one store
python3 test_data/load_redis.py --redis-url redis://$STORE:6379/0        # config + fixtures
# 5M warm signals, sharded across VMs (both families land in the one store)
python3 test_data/seed_feature_store.py --host $STORE --port 6379 --customers 5000000 --procs 16
python3 test_data/seed_signal_store.py  --host $STORE --port 6379 --customers 5000000 --flagged 50000 --procs 16
```

### 2. Pipeline services (one small VM; scale the Parquet writer + embedding-style consumers horizontally)
```bash
REDIS_URL=redis://$STORE:6379 PIPELINE_DATA=/data/parquet \
  python3 pipeline/parquet_writer.py &                 # A3: txn:events -> Parquet (system of record; run N in the group)
python3 pipeline/fast_loop.py &                        # B6: confirmed fraud -> bl:* (R001/R002 catch repeats live)
python3 -m uvicorn --app-dir pipeline aggregator:create_app --factory --port 8090 &   # C1 + dashboard
python3 pipeline/retrain_loop.py --audit /data/parquet/transactions --labels /data/parquet/labels \
  --model-out /models --interval 900 &                 # slow loop: retrain -> ONNX -> publish model:invalidate
```

### 3. Engines (N instances, behind the L4 LB as in §6)
Set per instance: `fraud.model.enabled=true`, `fraud.model.type=onnx`,
`fraud.model.path=/models/fraud-model.onnx` (a **shared/GCS-backed** path all engines read),
`fraud.audit-sink.type=redis-stream`, both store URIs → `$STORE`. Each engine subscribes to
`model:invalidate` (hot-swap the model) and `cfg:invalidate` (rules/bands).

### 4. Drive traffic + labels (D4-large)
```bash
# Gatling @ 1,000 tx/s — fraud rings (mule_/farm_ entities) + look-alike legit (shared_ device)
cd loadtest && ./mvnw -q gatling:test -Dgatling.simulationClass=fraud.FeedbackLoopSimulation \
  -DbaseUrl=http://$LB_VIP:8080 -Dscenario=steady -Dtps=1000 -Dcustomers=5000000 &
# Label feed — infers fraud from the mule_/farm_ entity scheme, submits matured labels
REDIS_URL=redis://$STORE:6379 python3 pipeline/chargeback_feed.py &
```
`FeedbackLoopSimulation` emits the same three-process mixture as the offline generator; the
chargeback feed is the ground truth the retrain + fast loops consume (compress the maturation
lag for the demo). Both are provided and CI-checked (the Gatling sim compiles; the feed is
unit-tested).

### 5. Watch
Open `http://<pipeline-vm>:8090/` — recall / FPR / precision **aggregated across all engines**
(from the shared stream + Redis counters, not per-instance actuators). As `retrain_loop`
ships a better ONNX (or you click **Apply feedback**), recall steps up and FPR steps down at
the marker; `SCARD bl:accounts` grows as the fast loop contains rings.

### Compute (feedback-loop run)
| Component | VM | Count |
|---|---|---|
| Scoring engines | `e2-standard-8` (`-Xmx4g`) | 3 (→5 for 3–5k burst) |
| Pipeline services (Parquet writer×2–4, aggregator+dashboard, fast loop, chargeback feed) | `e2-standard-8` | 1 |
| retrain_loop | `e2-standard-8` (or share pipeline VM) | 1 |
| Gatling driver | `e2-standard-8` | 1 |
| Seeder VMs (one-off, then release) | `e2-standard-16` | 4 |

### Redis cluster sizing (collapsed store, 5M / 1,000 tx/s)
Capacity (90-day history): feature ~120 GB + signal <20 GB + cached decisions ~13 GB
(24 h TTL) + streams/labels/metrics/bl/cfg ~3–5 GB ≈ **~155 GB raw** → **~195 GB** with
~25% overhead → **provision ~300 GB usable** (keep util < ~65%).

- **Shards:** **12 primaries** (~16 GB each — under the ~25 GB/shard ceiling). Throughput is
  ~75–110K ops/s (≤250K burst) — trivially served; this is **memory-bound**, so capacity drives
  the shard count, not ops.
- **Nodes — no-HA (demo):** **3 × `n2d-highmem-16`** (128 GB → 384 GB) at ~50% util.
- **Nodes — HA (replica ×2):** **6 × `n2d-highmem-16`** across 3 AZs (12 primary + 12 replica).
- **Lever:** seed `--days 30` → feature ~55 GB (total ~90 GB raw), fits **3 × `n2d-highmem-8`**
  (192 GB) no-HA. `--days 7` smaller still. Full 90d isn't needed for a few-hours feedback demo.

*(Production may split back into two clusters per §7.12; this run collapses them.)*

### Shared storage
Three things are shared: the **Parquet system-of-record** (writer→retrain/analytics), the
**labels Parquet**, and the **ONNX model** (retrain→every engine).
- **Recommended — GCS bucket** for `$DATA` (transactions + labels): the Parquet writer / DuckDB
  read+write `gs://…` directly (pyarrow + gcsfs / DuckDB httpfs) — durable, cheap, no mount.
- **Model artifact — GCS + fetch-on-`model:invalidate`:** publish `gs://…/fraud-model.onnx`(+manifest);
  each engine, on the Pub/Sub message, downloads to a local `$MODELS` path then `reload()`s
  (strongly consistent per fetch, avoids NFS). This small fetch step is the one remaining code
  hook; without it, use a **Filestore** (managed NFS) mounted read-only on engines as `$MODELS`.
- **Simplest single POSIX path:** one **Filestore** (e.g. 1 TB SSD) mounted on all VMs as `$DATA`
  + `$MODELS` — zero glue, pricier, single-region. Do **not** use multi-attach Persistent Disk.

### Sizing (added components, on top of §Sizing)
| Component | Guidance |
|---|---|
| Redis Stream `txn:events` | bounded (`MAXLEN ~`), a few hundred MB; transport only — Parquet is the archive |
| Parquet writer | 1–2 vCPU per consumer; **rolls one large atomic file per partition** every ~200k rows / 60 s (not per batch), so a few-hours run produces ~tens of files, not tens of thousands |
| Parquet store | ~1–2 KB/txn × retention; on GCS — the durable training corpus. A few-hours run ≈ ~11M rows ≈ a few GB; delete the bucket after the run |
| Aggregator + dashboard | 1 small VM (rolling Redis counters + SSE); stateless |
| retrain_loop | 1 VM, a few GB RAM. **Reads via DuckDB** — the label↔snapshot join + maturation filter + 14-feature extraction + reservoir **sample** run in SQL over the Parquet/GCS globs (out-of-core, bounded RAM), returning only the training matrix (default cap 1M rows). Minutes per round, independent of the 5M population (§13.6.4). sklearn HGB needs no native libs; LightGBM would need `libgomp1` |
| Model artifact store | shared/GCS path for `/models/*.onnx`; engines read on `model:invalidate` |

### GCP-side notes
1. **Engine feature snapshot** exposes all 14 model feature keys incl. `new_payee` (derived
   from the pair state) — verified by `HotWindowRuleIT.featureSnapshotCarriesTheModelFeatureContract`.
   `retrain_loop` rebuilds vectors from `feature_snapshot_json` directly.
2. **Gatling feedback scenario** (`FeedbackLoopSimulation`) + **label feed** (`chargeback_feed.py`)
   provided (step 4).
3. **Model path** must be a shared/GCS-backed volume all engines read (`fraud.model.path`), so a
   `retrain_loop` export + `model:invalidate` (cluster-wide Pub/Sub) hot-swaps every instance.
   This is the one remaining deployment-config item (no code).
