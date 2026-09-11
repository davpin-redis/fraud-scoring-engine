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
