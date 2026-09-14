# Implementation Plan — Continuous-Learning Feedback Loop

Companion to `Fraud_Scoring_System_Design_Doc.md` §8.4 (two tracks), §10.3
(real-time dashboard) and §13.6 (two-scale simulation data). Turns that design
into a buildable slice, sequenced so the learning loop is *provable on a laptop*
before it is run at scale.

**Goal:** a run where **fraud detection rises and false positives fall the longer
it runs** — demonstrated by a deterministic small-scale test and, at 5M / 1,000
tx/s, by the live cross-instance dashboard.

**Locked decisions:** ONNX model served in-engine · Python for the data/ML
services (engine stays Java) · labels held in Redis (online) **and** Parquet
(training) · CPU **LightGBM** as the Track-A model · feature + signal stores
**collapsed into one Redis** for this work.

---

## Components to implement

Legend: **[me]** built/run on the laptop · **[you]** run on GCP · **S/L** small/large preset.

### A. Data foundation (§8.4.1)
- **A1 Collapse to one Redis** — both `fraud.*-store.uri` → one instance; one `redis:8` in compose. *(both, [me])*
- **A2 `TransactionSink` → Redis Stream** — replace `NoOpTransactionSink`; `XADD txn:events` bounded `MAXLEN ~`; payload = `ScoredTransaction` incl. `featureSnapshotJson`. *(engine, [me])*
- **A3 Parquet writer** — consumer group on `txn:events` → date-partitioned Parquet = system of record; single writer. *(Python, [me])*
- **A4 Label store + ingestion** — `(label, source, labeled_at)` by `transaction_id`; maturation window; online copy in Redis + appended to Parquet. *(Python, [me])*

### B. Learning core — Track A (§8.4.2, §13.6)
- **B1 Point-in-time join** — matured labels ↔ stored feature snapshots (Parquet), time-ordered, no leakage. *(Python, [me])*
- **B2 LightGBM round trainer** — sampled labelled set → versioned artifact; class-imbalance handling. *(Python, [me])*
- **B3 Champion/challenger eval harness** — fixed holdout, fixed operating point; recall@FPR, precision, PR-AUC, £; per-round learning curve. *(Python, [me])*
- **B4 ONNX `ModelScorer` + hot-reload** — export LightGBM → ONNX; engine scorer loads the artifact + `model:invalidate` Pub/Sub. *(engine, [me])*
- **B5 Decision bands → `cfg`** — thresholds hot-reload like rules. *(engine, [me])*
- **B6 Fast reactive loop** — label consumer → `bl:*` + `cfg:invalidate`; confidence/caps/expiry guards. *(Python/engine, [me])*

### C. Metrics & UI (§10.3)
- **C1 Metrics aggregator** — `txn:events` + labels → global Redis confusion counters `metrics:{modelVersion}:{minute}` → recall/precision/FPR/£; SSE `/metrics/fraud/stream` + `POST /apply-feedback`. *(Python FastAPI, [me])*
- **C2 Dashboard wiring** — `feedback_dashboard_mockup.html` `USE_SIMULATION=false` → C1. *(done-ish, [me])*

### D. Simulation data (§13.6)
- **D1 Parameterised generator** (`--scale small|large`) — population (customers/benes+mules/devices+farm/tpps), warm-start backfill, streaming rounds (legit + look-alike legit + fraud rings with entity reuse), per-round label feed, fixed holdout, cfg incl. the blunt baseline rule. *(Python, [me])*
- **D2 Generator self-checks** — joint-signature separability, look-alike overlap, ~1% imbalance, entity reuse, seeded RNG. *(Python, [me])*
- **D3 Collapsed-store seeder** — small: direct Python; large: extend `seed_*` into one store + fraud entities. *(S [me] / L [me-code, you-run])*
- **D4 Round orchestrator** — small: script (score → reveal labels → retrain → eval → push to aggregator); large: Gatling streaming feeder @1,000 tx/s + retrain cadence. *(S [me] / L [me-code, you-run])*

### E. Tests
- **E1 Component tests** — aggregator counters + label-join, trainer, eval harness, point-in-time assembly. *([me])*
- **E2 Learning-curve assertion (headline)** — deterministic K-round small run: recall↑ & FP↓ monotone-within-tolerance, challenger ≥ champion each round & strictly better by round K, fixed operating point. *([me])*
- **E3 Engine ITs** — stream publisher, ONNX scorer load + `model:invalidate`, `bl:*` reload, collapsed-store config. *([me])*

### F. Docs / runbook
- **F1 Runbook: large preset** — seed → engines → Gatling streaming demo → aggregator/dashboard → retrain cadence → observe the curve. *([me-write, you-run])*

---

## Phases (dependency-ordered)

- **Phase 0 — Foundations:** A1, A2, A3, A4. *Unlocks everything.* ✅ **done** (engine 44 green; pipeline e2e verified)
- **Phase 1 — Simulation data:** D1, D2. ✅ **done** (D3 Redis warm-signal seed deferred to Phase 3/4 — the offline core trains on the generated feature Parquet). *(needs A1)*
- **Phase 2 — Learning core, offline:** B1, B2, B3 + E1, E2. ✅ **done** — deterministic learning curve: feedback model R1→R10 recall 0.76→0.86, FPR down, precision up, PR-AUC 0.67→0.85; beats the blunt baseline on every axis. **Model note:** uses scikit-learn `HistGradientBoostingClassifier` (no OpenMP dep) as the CPU GBT; LightGBM is the documented prod swap once `libomp` is installed. *(needs 0–1)*
- **Phase 3 — Aggregator + live dashboard:** C1, C2, D4-small. ✅ **done** — aggregator joins `txn:events` + labels into global Redis confusion counters and serves the dashboard over SSE; the orchestrator streams champion(blunt)+challenger(fb) decisions; e2e smoke shows the live inflection (challenger recall 0.63→0.76 on feedback, FPR far below the blunt champion). 20 pipeline tests green.
- **Phase 4 — Engine-in-the-loop:** B4, B5, B6, E3. ✅ **done** — B4 in-engine ONNX scoring + `model:invalidate` hot-reload; B5 decision bands in `cfg:bands`, hot-reloaded; B6 fast reactive loop (confirmed fraud → `bl:*`, caught live by R001/R002). Engine 50 green; pipeline 24 green. *(Remaining small wiring: feed the orchestrator's fraud labels into `labels:events` with entities so the fast loop grows blacklists during the live demo — needs entity columns loaded in `dataset.holdout`.)*
- **Phase 5 — Large scale (GCP):** D3-large, D4-large/Gatling, F1. ← **next** *([you] run)* *(needs 0–4)*
- **Phase 6 — (deferred) Track B embeddings (GPU).** Out of scope for this demo (§8.4.3).

Phases 0–3 deliver the whole loop **runnable and provable on the laptop** (green
learning-curve test + live dashboard); Phase 4 makes it engine-live; Phase 5 is
the GCP scale run.

---

## Run the live small-scale demo (Phase 3)

```bash
docker compose up -d                                   # collapsed Redis on :6379
./.venv/bin/python pipeline/generator.py --scale small --out pipeline/data/sim/small
./.venv/bin/python -m uvicorn --app-dir pipeline aggregator:create_app --factory --port 8090 &
./.venv/bin/python pipeline/orchestrator.py --sim pipeline/data/sim/small --tps 400 &
open http://localhost:8090/                            # dashboard (live SSE), click "Apply feedback"
```

The dashboard is the same `feedback_dashboard_mockup.html`, served by the aggregator with
`USE_SIMULATION=false` so it reads the real SSE feed. "Apply feedback" promotes the
challenger; recall steps up and FPR steps down at the marker.

## Layout (new)

```
pipeline/                 # Python data/ML services (Phase 0+)
  common.py               # redis + parquet paths, schema, config
  parquet_writer.py       # A3: txn:events consumer -> Parquet
  labels.py               # A4: label store (Redis + Parquet) + maturation
  requirements.txt
  tests/
engine/ …                 # A2 RedisStreamTransactionSink, B4/B5 later
```
