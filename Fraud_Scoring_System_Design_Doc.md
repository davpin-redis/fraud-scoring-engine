# Real-Time Transaction Fraud Scoring System

**Design Document**

Author: David Pinto
Date: August 10, 2026
Status: Draft — for review

---

> ## ⚠ Architecture amendment (2026-08-18) — transaction store removed
>
> The engine no longer stores individual transactions in a **Redis Flex transaction
> store** or reads the hot window via **`FT.SEARCH`**. That path (RedisJSON/`@RedisHash`,
> OM Spring, the Query-Engine `txnIdx`/`pbIdx` indexes, and `TransactionSeeder`) has
> been **removed**. §2.3, §3.1–§3.6, §4.2/§4.3, §5, §7.12, §8.1, and §13.1/§13.2 below
> have been updated to reflect this; this banner is retained as a change record.
>
> **New design — streaming signals on two RAM stores, core types only (no modules):**
> - **Feature store** (Lettuce, `fraud.feature-store.uri`): 24h **exact** hot-window
>   signals — a repeat-declines counter and a distinct-beneficiary Set — written
>   idempotently inside the existing customer Lua script, alongside the aggregates.
> - **Signal store** (Lettuce, `fraud.signal-store.uri`): 90d **approximate** signals —
>   **HyperLogLog** (rotating monthly buckets) for distinct beneficiary fan-out (R013)
>   and distinct-sender fan-in (R005), **counters** for repeat declines (R012),
>   **count/sum/sumsq** for the amount z-score (R018), and **RedisTimeSeries** for the
>   time-shaped behavioural signals — velocity vs baseline, sustained elevation,
>   circadian off-hour, machine cadence, amount bust-out trend, dormancy, per-device
>   velocity surge, and per-payee inbound velocity surge (R019–R026). Every TimeSeries key
>   is hash-tagged by a **high-cardinality** entity (`{cid}` / `{device}` / `{bene}`) so
>   writes and range-scans spread evenly across shards — no low-cardinality hot key (an
>   earlier per-country geo series was dropped because ~5 country keys concentrated all
>   traffic onto a handful of shards). TimeSeries is bundled in Redis 8 / Redis Enterprise
>   and driven from Lettuce via async `dispatch`. Bounded RAM, no search index, no flash.
> - **Audit sink** (`TransactionSink`): every scored txn flows to a durable
>   per-transaction "third store" — currently a `NoOpTransactionSink` placeholder.
>
> Hot-window signals are computed over **both** a 24h and a 90d window where it makes
> sense. Rules: R012/R013/R005 (declines / fan-out / fan-in) + R014–R018 (24h declines,
> 24h fan-out, 90d fan-in, 5-min velocity, amount z-score) + **R019–R026** (TimeSeries
> behavioural: rate-vs-baseline spike, sustained elevation, circadian anomaly, machine
> cadence, amount bust-out, dormant reactivation, device velocity surge, payee inbound
> velocity surge). New-payee (R006)
> still uses the existing pair-state. See `README.md` (Architecture) and
> `LOAD_TEST_RUNBOOK.md` for the current operational picture.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Requirements](#2-requirements)
3. [High-Level Architecture](#3-high-level-architecture)
4. [Data Model](#4-data-model)
5. [Scoring Signals & Features](#5-scoring-signals--features)
6. [Feature Windowing & Time Categories](#6-feature-windowing--time-categories)
7. [Feature Store Data Model (Redis)](#7-feature-store-data-model-redis)
8. [Scoring Engine Design](#8-scoring-engine-design)
9. [Security, Privacy & Compliance](#9-security-privacy--compliance)
10. [Monitoring & Observability](#10-monitoring--observability)
11. [Tradeoffs & Alternatives Considered](#11-tradeoffs--alternatives-considered)
12. [Open Questions & Future Work](#12-open-questions--future-work)
13. [Test Data & Load Testing](#13-test-data--load-testing)

---

## 1. Overview

This document describes the design of a real-time fraud scoring system for payment transactions. Every incoming transaction (card-not-present e-commerce checkout, in-app purchase, or ACH/bank transfer) is evaluated synchronously at authorization time and assigned a fraud risk score. The score, combined with a decision policy, determines whether the transaction is approved, declined, or routed to manual review. Every scored transaction, along with its inputs and outcome, is persisted for auditing, dispute handling, and future model training.

The system is designed to mimic the core components of a production payment fraud engine: real-time feature computation, a hybrid rules-plus-machine-learning scoring approach, a low-latency decision path, and a durable data store that feeds back into model improvement.

### 1.1 Problem Statement

Payment fraud (stolen card numbers, account takeover, synthetic identities, friendly fraud) causes direct financial loss through chargebacks and erodes customer trust. A fraud system must catch a high proportion of fraudulent transactions while declining as few legitimate transactions as possible, and it must do so within the latency budget of a payment authorization flow — typically well under a second, alongside the payment processor's own latency budget.

### 1.2 Goals

- Score 100% of transactions synchronously, in the critical authorization path, within a p99 latency budget of ~30 ms for the scoring service itself.
- Combine deterministic rules (explainable, fast to update) with a machine-learned risk score (adaptive, higher discriminative power).
- Persist every transaction, its computed features, and its final score/decision for audit, dispute resolution, and model retraining.
- Support tunable decision thresholds so risk/ops teams can trade off fraud loss against false-decline rate without a code deploy.
- Provide a feedback loop so confirmed fraud/chargeback outcomes improve future scoring.

### 1.3 Non-Goals

- Building a full payment processing / settlement system — this design assumes an upstream payment gateway or PSP that calls into the fraud engine and owns actual fund movement.
- Identity verification / KYC onboarding checks (treated as a separate upstream system whose outputs may be consumed as a feature).
- Chatbot-style manual review UI — the review queue is described at a data-model level only, not a full case-management UI spec.

---

## 2. Requirements

### 2.1 Functional Requirements

- Accept a transaction event (payment attempt) and return a risk score plus a decision (approve / review / decline) synchronously.
- Compute features from the transaction itself, the requesting account/card/device history, and aggregate velocity signals.
- Apply a rules layer for hard blocks (e.g., known-bad card, sanctioned country) and allowlists (e.g., trusted returning customer).
- Apply an ML model to produce a continuous fraud probability score.
- Blend rule outcomes and model score into a single final decision using a configurable policy.
- Persist the full transaction record, computed features, score, and decision — durably and queryable — for every transaction, regardless of outcome.
- Ingest fraud/chargeback outcome labels after the fact and associate them with the original transaction record.
- Expose scoring metrics and model performance for monitoring.

### 2.2 Non-Functional Requirements

| Requirement | Target | Notes |
|---|---|---|
| Customer / account volume | 10,000,000 accounts | Drives feature-store and hot-store sizing (§2.3, §4.3, §7.11). |
| Sustained throughput | 1,000 transactions/second | Steady-state design load; see §2.3 for burst-headroom assumption. |
| Scoring latency (p99) | ≤ 30 ms | Time from request received to decision returned, excluding network hop to caller. Revised down from an initial 100 ms target. |
| Scoring latency (p50) | ≤ 10 ms | Typical case should be well under the p99 ceiling. |
| Availability | 99.95% (per region) | Measured per region (§3.4); with no cross-region failover by design, a regional outage is total for that region's customers. Fraud scoring sits in the critical payment path; an outage should degrade gracefully, not block payments entirely. |
| Durability of transaction log | No data loss | Every transaction must be persisted even if scoring itself fails. |
| Auditability | Full reconstruction | Any past decision must be explainable: which rules fired, model version, feature values, final score. |
| Explainability | Rule + top feature attribution | Risk analysts need to see why a transaction was flagged, not just a number. |

### 2.3 Scale & Capacity Assumptions

The 30 ms p99 target is materially tighter than a typical fraud-scoring SLA (100 ms is more common), and it applies at sustained load, not just for a single request in isolation. This has direct architectural consequences, noted throughout this document:

- **1,000 TPS sustained** ≈ 86.4M transactions/day ≈ 31.5B/year at steady state. Design for a burst multiplier (recommend 3–5x, i.e. 3,000–5,000 TPS) to absorb traffic spikes (promotions, flash sales) without breaching the latency SLA — sustained-load capacity alone is not sufficient.
- **10M customer accounts** sets the working-set size for account/card/device history that the Feature Service must keep "hot" (low-latency-reachable) at all times — this is not a dataset that can be paged in from cold storage within a 10 ms feature-assembly budget (see §3.3), so it needs to live in an in-memory or memory-tier store (e.g., Redis or a similar in-memory data store) rather than a disk-oriented database.
- **Data volume:** the engine does **not** store individual transactions on the scoring path. The 90-day history that two rules used to scan is replaced by **streaming approximate signals** kept in a second RAM store (the **signal store**, §4.3, §7.12): HyperLogLog / counters / a stats hash + RedisTimeSeries, sized in the tens-to-~130 GB range for 5M customers (measured, §7.12) rather than the multi-TB a raw 90-day transaction log would need. Every scored transaction is still persisted in full to a durable **audit sink** (§4.2/§4.3) for audit and training, but that write is off the hot path and behind a pluggable interface, not a Redis Flex database the engine reads from. Both Redis stores stay fully in RAM.
- These figures are back-of-envelope planning inputs, not committed capacity numbers; they should be revisited once real traffic and record-size data are available.

---

## 3. High-Level Architecture

The system is composed of five logical layers: ingestion, feature computation, scoring (rules + ML), decision, and persistence. A synchronous path handles the real-time score; asynchronous paths handle logging, label ingestion, and model retraining.

### 3.1 Component Overview

| Component | Responsibility |
|---|---|
| Transaction Gateway / API | Receives the transaction event from the merchant/PSP integration; the single entry point into the fraud system; enforces request schema and auth. *(In this reference build the endpoint is intentionally unauthenticated for demo simplicity — see §9 for how to add auth.)* |
| Feature Service | Computes and fetches features in real time: transaction attributes, velocity aggregates, device/IP reputation, historical account behavior. Backed by an in-memory feature store (Redis — see §7) holding pre-aggregated counters and per-account/card/device state for all 10M accounts — a disk-backed lookup does not fit the 10 ms feature-assembly budget (§3.3). |
| Rules Engine | Evaluates a configurable set of deterministic rules (blocklists, allowlists, thresholds) against the transaction and features. Rules can force an outcome (hard block/allow) or contribute signal to the score. |
| ML Scoring (embedded) | Loads a versioned trained model in-process and returns a fraud probability given the feature vector. Runs as an in-process, low-latency inference call rather than a remote endpoint (§3.6). |
| Decision Engine | Blends rule outcomes and the ML score using a configurable policy into a final decision: approve, decline, or route to manual review. |
| Signal Store | A second in-memory Redis database holding **90-day approximate hot-window signals** derived from the transaction stream — HyperLogLog (distinct fan-out/fan-in), counters (repeat declines), a count/sum/sumsq hash (amount z-score), and RedisTimeSeries (velocity + behavioural rules). Written per-transaction off the response path (`SignalWriter`) and read into the feature vector at scoring time (`SignalReader`). Replaces the removed Flex transaction store; core types only, no search index. |
| Audit Sink | Durable, append-only record of every scored transaction, its features, score, model version, and decision — source of truth for audit and training. A pluggable `TransactionSink` interface written off the response path (`TransactionWriter`); the reference build ships a `NoOpTransactionSink` placeholder, adaptable to a log/queue/warehouse. The engine never reads it back on the scoring path. |
| Label / Outcome Ingestion | Consumes chargeback and confirmed-fraud reports (typically arriving days to months later) and attaches ground-truth labels to historical transactions. |
| Model Training Pipeline | Offline batch pipeline that retrains the ML model periodically using labeled historical transactions; publishes new model versions to the scoring service. |
| Monitoring & Alerting | Tracks latency, throughput, rule-fire rates, score distributions, model drift, and business metrics (approval rate, fraud loss, false-positive rate). |
| Manual Review Queue | Holds transactions routed to "review" for human analyst decision; analyst outcomes feed back as labels. |

### 3.2 Synchronous Scoring Flow

A transaction is scored inline, before the payment is authorized:

1. Merchant/PSP sends a transaction event to the Transaction Gateway.
2. Gateway validates the request and normalizes the transaction amount to the configured base currency (GBP in this build; `fraud.base-currency`) — in the reference implementation via a stubbed FX hook (`FxService`); a production system uses a periodically-refreshed FX source. The raw `amount` + `currency` are kept alongside the derived `amount_base`, then forwarded to the Feature Service.
3. Feature Service assembles the feature vector: transaction-level fields, account/card/device history from the feature store, and real-time velocity counters.
4. Rules Engine evaluates hard-block and hard-allow rules first (short-circuit path); if none fire conclusively, soft rules contribute weighted signal.
5. ML Scoring Service scores the feature vector, returning a fraud probability (0–1).
6. Decision Engine combines rule signal and model score per the active policy, and maps the result to approve / review / decline using configured thresholds.
7. Decision is returned synchronously to the caller.
8. In parallel (fire-and-forget, does not block the response), three off-path writes run: (a) the feature store's rolling aggregates + 24h exact hot-window signals are updated (§7.9); (b) the signal store's 90-day approximate signals are updated (`SignalWriter` — HyperLogLog / counters / stats hash / TimeSeries, §7.12); (c) the full record — transaction, features, rule fires, model version, score, decision — is handed to the durable audit sink (`TransactionWriter`, §4.2/§4.3). The feature-store write is guarded by the idempotency key from §7.8 (`SET decision:{<customer_id>}:<transaction_id> ... NX`, co-located with the customer's keys) and the signal-store write by its own co-located `SET … NX` guard (`sig:seen:{<customer_id>}:<transaction_id>`): only the writer that wins the `NX` set applies the update, so a retried transaction is never double-counted into velocity/aggregates/signals.

*Step 8 is intentionally decoupled from the response path: persistence and signal updates must never add to caller-facing latency, but the audit write must not be lossy either (see §4.3 on durability).*

### 3.3 Latency Budget

A 30 ms p99 budget at 1,000+ TPS sustained leaves very little room for anything other than in-memory lookups and a lightweight model — there is no slack for disk-backed reads or a network hop to a remote feature store on the critical path.

| Stage | Budget | Notes |
|---|---|---|
| Request validation / routing | 2 ms | Gateway-level. |
| Feature assembly | 10 ms | In-memory store only (Redis — §7); pre-aggregated velocity counters, not on-the-fly scans or joins against a disk-backed database. |
| Signal-store read (§7.12) | (overlapped) | The 90-day approximate signals (R012/R013/R005/R016/R018 + the TimeSeries behavioural rules R019–R026, §8.1) are read from the signal store — `MGET`/`PFCOUNT`/`HMGET` and `TS.RANGE`/`TS.GET`, all pipelined. Run **concurrently** with feature assembly on a virtual thread (`SignalReader`), so it overlaps that 10 ms rather than adding to it. Every read is O(1)/O(bounded) against RAM (no search index, no SSD). It **degrades** (§3.5): on failure the signals fall back to safe defaults (`SignalReader.empty()`) and never block the response. |
| Rules evaluation | 2 ms | In-memory rule set. |
| ML model inference | 10 ms | Single feature vector against a compiled/optimized in-process runtime (e.g., gradient-boosted trees exported to a fast native/ONNX runtime), invoked as a local call — no network hop (§3.6). |
| Decision blending | 2 ms | Simple policy evaluation. |
| Headroom / network jitter | 4 ms | Reserved buffer so the total stays under budget under load, not just in the best case. |
| **Total (target p99)** | **≤ 30 ms** | The audit-sink write and the feature-/signal-store updates happen after the response is sent (§3.2 step 8) and are excluded from this budget. |

### 3.4 Regional Deployment

To avoid cross-border data-residency issues, the system is deployed as an independent stack per region — each region runs its own full instance of the Transaction Gateway, Feature Service/Redis feature store, signal store, Rules Engine, ML Scoring Service, Decision Engine, and audit sink. There is no cross-region replication of customer or transaction data.

- Each customer has a home region (e.g., derived from `customer_portfolio_country` or the merchant/entity they're onboarded under), and all of that customer's feature-store state and signal-store signals are read and written only within that region's deployment.
- A transaction is scored entirely within the initiating customer's home region, even when the counterparty (beneficiary account, TPP) is in another country — geography/TPP/country-level aggregates (§6.3, §7.6) are computed from the data visible inside that region, not from a global cross-region view.
- Model training (§8.2, §8.4) runs per region on that region's labeled data by default; a shared/global model trained on pooled data across regions is possible but would itself need to be evaluated against each region's residency requirements before being adopted.
- One direct consequence: fraud patterns that only become visible by correlating activity across two regions' data (e.g., a mule ring operating through multiple regional deployments) are not caught by this design — this is a known gap, not an oversight, and is listed as an open question (§12).

### 3.5 Failure Modes & Fallbacks

- **ML Scoring Service timeout or unavailable:** fall back to rules-only decisioning with a conservative default (e.g., bias toward "review" rather than "decline") so legitimate traffic isn't blocked outright by an infrastructure issue.
- **Feature Service or signal-store partial failure** (e.g., velocity counters or 90-day signals unavailable): score with the available feature subset — the signal reader substitutes safe defaults (`SignalReader.empty()`) — and flag the record as "degraded features" for later analysis; do not fail the transaction.
- **Audit-sink write failure:** buffer off-path and retry (`TransactionWriter` holds a bounded retry buffer); scoring responses are never blocked on this write.
- **Total fraud-system outage:** gateway falls back to a pre-agreed default policy set by the business (e.g., approve with tighter downstream monitoring, or fail closed) — this is a business decision, not a technical default, and should be explicitly configured.

### 3.6 Technology & Statelessness

The synchronous scoring engine — Transaction Gateway, Rules Engine, and Decision Engine (§3.1) — is implemented in Java/Spring Boot and designed to be stateless: no per-customer or per-transaction data is held in an instance's memory across requests. Every piece of state a request needs already lives outside the instance:

- Account/card/device history, velocity aggregates, and cross-entity aggregates come from the Redis feature store (§7) on every request; the 90-day approximate signals come from the signal store (§7.12) on every request. Both are external RAM stores, not instance memory.
- Rule and window/metric definitions (§6.1–6.2, §8.1) are read from Redis-backed config (`cfg:rules`, `cfg:windows`, `cfg:metrics`) and only held in an instance's local memory as a read-through cache, invalidated via Pub/Sub when changed (§7.2) — this is a performance optimization, not session state; any instance can rebuild it from scratch and is safe to drop at any time (e.g., on restart).
- The transaction and its decision are handed to the durable audit sink after the response is sent (§3.2 step 8), not held by the instance afterward.

Because no instance owns any customer's state, horizontal scaling is a matter of adding more Spring Boot instances behind a load balancer, with no session affinity or sharding logic in the scoring engine itself — all sharding/partitioning happens one layer down, in the feature-store and signal-store Redis Clusters (§7.3, §7.12). This also means any instance can serve any request and can be drained and replaced (rolling deploys, autoscaling, failover) without losing in-flight customer state, which directly supports the 99.95% availability target (§2.2).

The ML model (§8.2) is embedded in-process in the scoring engine rather than run as a separate service — the trained gradient-boosted model is exported to a fast in-process runtime (§3.3) and invoked as a local call on the assembled feature vector, so inference adds no network hop to the 30 ms budget. The model artifact is versioned and shipped as part of the Spring Boot deploy (§8.2). Inference remains stateless in the same sense as the rest of the engine — the feature vector is the entire input and the score the entire output, with no server-side session state — at the cost of coupling model-release cadence to the app-release cadence; §8.2 covers how A/B and rollback are handled via rolling/canary deploys rather than request routing.

---

## 4. Data Model

### 4.1 Core Entities

- **Transaction** — a single payment attempt; the unit of scoring.
- **Account** — the merchant customer/user initiating the transaction.
- **Payment Instrument** — card (PAN token/BIN, expiry, issuing bank), or bank account (for ACH).
- **Device** — the client device/browser session used, identified via a device fingerprint.
- **Merchant** — the seller/merchant account the transaction is against.
- **Session / IP** — network-level context: IP address, ASN, geolocation.

### 4.2 Transaction Record Schema

| Field | Type | Description |
|---|---|---|
| transaction_id | UUID | Unique identifier for the transaction; primary key. |
| timestamp | datetime | Time the transaction was received. |
| account_id | string | Merchant customer/account identifier. |
| merchant_id | string | Merchant the transaction is against. |
| amount | decimal | Transaction amount in its original (native) currency, as received. |
| currency | string (ISO 4217) | Transaction currency. |
| amount_base | decimal | `amount` normalized to the configured base currency (GBP in this build) at ingest (§3.2); used for all monetary aggregates (§7.4). Raw `amount`/`currency` are retained for audit and dispute handling. |
| payment_instrument | object | Tokenized card (BIN, last4, expiry) or bank account reference — never raw PAN/account numbers. |
| device_fingerprint | string | Hashed device/browser identifier. |
| ip_address | string | Originating IP (hashed/truncated per retention policy where required). |
| billing_address / shipping_address | object | Address fields, used for AVS and geolocation mismatch checks. |
| feature_vector | object | Snapshot of computed features used for this scoring decision. |
| rules_fired | array | List of rule IDs that triggered, with outcome (block/allow/signal). |
| model_version | string | Identifier of the ML model version used. |
| model_score | float (0–1) | Raw fraud probability from the model. |
| final_score | float (0–1) | Blended score after rules + model combination. |
| decision | enum | approve / review / decline. |
| outcome_label | enum, nullable | Ground truth added later: confirmed_fraud / confirmed_legit / chargeback / unknown. |
| label_source | string, nullable | Where the outcome label came from (chargeback feed, manual review, none yet). |
| label_updated_at | datetime, nullable | When the outcome label was attached. |

*Sensitive fields (full PAN, bank account numbers, CVV) are never stored — only tokens/references issued by the payment processor. See §9 (Security & Compliance).*

**Where this record goes (reference build).** This full record is the payload the engine hands to the durable **audit sink** (`TransactionSink`) off the response path (§3.2 step 8, §3.4/§4.3), modelled by the `ScoredTransaction` record and written by `TransactionWriter`. The engine **does not read it back** on the scoring path — nothing about a transaction's own record feeds its own or later scores. The 90-day behavioural signals that scoring needs are instead maintained incrementally in the **signal store** (§7.12), so there is no indexed transaction database and no `FT.SEARCH`/Flex tier. The reference build ships a `NoOpTransactionSink` placeholder; a production sink is a log/queue/warehouse writer behind the same interface (§4.3).

### 4.3 Storage Strategy

Persistence serves two distinct needs — real-time behavioural signals for scoring, and a durable record for audit/training — and each is met by a purpose-built path rather than one indexed transaction store:

- **Signal store (RAM, on the scoring path)** — a **separate Redis database from the feature store** (§7.12) holding **90-day approximate** hot-window signals, maintained incrementally as each transaction is scored (`SignalWriter`) and read into the feature vector at scoring time (`SignalReader`). It uses **core Redis types only**: HyperLogLog for distinct fan-out (R013) and fan-in (R005/R016), counters for repeat declines (R012), a count/sum/sumsq hash for the amount z-score (R018), and **RedisTimeSeries** for the time-shaped behavioural rules (R019–R026). There is **no raw transaction history, no search index, and no flash** — the store is bounded by the number of active entities and their signal buckets, not by transaction volume, so it stays in the tens-to-~130 GB range for 5M customers (measured, §7.12) rather than the multi-TB a raw 90-day log would need. This is what replaced the earlier Redis Flex / Query-Engine transaction store, and it removes the Flex constraints (`ON HASH`/TAG-only/`SKIPINITIALSCAN`/no `FT.AGGREGATE`) that used to shape the design. Signals are keyed with high-cardinality hash tags (`{customer_id}` / `{device}` / `{bene}`) so load spreads evenly across shards (§7.12). Reads run concurrently with feature assembly and degrade to safe defaults on failure (§3.3, §3.5).
- **Audit sink (durable, off the scoring path)** — an append-only record of every scored transaction (the full §4.2 record), used for model training, offline analytics, and long-term audit/compliance retention. It is a pluggable `TransactionSink` interface written off the response path by `TransactionWriter` (with a bounded retry buffer, §3.5); the reference build ships a `NoOpTransactionSink` placeholder, adaptable to a durable log/queue or a data lake / warehouse (JSONL, Parquet, BigQuery, object storage). The engine **never reads it back** on the scoring path.
- Note both of the above are distinct from the in-memory **feature store** (§3.1, §7), which holds current aggregated state (counters, recent behaviour summaries, and the 24h *exact* hot-window signals) for all customers, not raw transaction history.

---

## 5. Scoring Signals & Features

Features fall into several categories. In production systems, the majority of fraud-catching power comes from velocity and historical-behavior features rather than static transaction attributes alone.

| Category | Example Signals | Why It Matters |
|---|---|---|
| Transaction attributes | Amount, currency, time of day, merchant category | Unusual amounts or off-hours activity relative to the account's norm are classic anomaly signals. |
| Velocity | # transactions per card/account/device/IP in last 1 min / 1 hr / 24 hr; sum of amounts in window | Fraud rings and card testing show up as bursts of activity across a short window. |
| Payment instrument risk | BIN country vs. billing country, card age, prior decline/chargeback rate on this instrument | Stolen cards and card testing tools have distinguishing instrument-level patterns. |
| Device & session | Device fingerprint reuse across unrelated accounts, emulator/bot signals, new vs. known device | A single device transacting across many unrelated accounts is a strong fraud-ring indicator. |
| Network / geolocation | IP geolocation vs. billing/shipping address mismatch, VPN/proxy/Tor detection, ASN reputation | Location mismatches and anonymization proxies correlate strongly with fraud. |
| Account history | Account age, past chargeback history, typical spend pattern, login/behavioral biometrics if available | Established accounts with a consistent pattern are lower risk than brand-new or erratic accounts. |
| Merchant risk | Merchant category code risk tier, merchant's historical fraud rate | Certain merchant categories (e.g., gift cards, electronics) are disproportionately targeted. |
| Network/graph signals | Shared attributes (device, address, email) across multiple accounts | Fraud rings link seemingly unrelated accounts through shared infrastructure. |
| External reputation | Third-party blocklists, email/phone risk scores, prior fraud reports | Cross-merchant signal not visible from this system's own history alone. |

The table above is the target-state catalog. The reference build implements a
subset; the rest are deferred.

**Implemented in the reference build**

| Category | Feature(s) | Rule |
|---|---|---|
| Transaction attributes | `amount_base`, `local_hour` | R003, R006, R011 (amount thresholds) |
| Velocity | `customer_txn_count_1h`, `bene_distinct_senders_24h`, `pair_txn_count_90d`, positional last-payment, per-window amount stddev | R004, R005, R006 |
| Device & session | `device_distinct_customers` (shared-device fan-out) | R009 |
| Network / geolocation | `ip_country` (IP-geo vs portfolio country), `ip_proxy` (proxy/VPN list) — IP-geo reference in Redis (`geo:ip_prefixes` / `geo:proxy_ips`, §7.2) | R010 |
| Account history | `account_age_days` (new-account risk) | R011 |
| Hot-window declines/fan-out — 24h **exact** (feature store) | `customer_declines_24h` (counter `c:{id}:declines:24h`), `customer_distinct_bene_24h` (Set `c:{id}:benes:24h`) — written idempotently in the customer Lua write | R014, R015 |
| Hot-window signals — 90d **approximate** (signal store, §7.12) | `customer_declines_90d` (rotating counters), `customer_distinct_bene_90d` (HyperLogLog fan-out), `bene_distinct_senders_90d` (HyperLogLog fan-in), `amount_zscore_90d` (count/sum/sumsq hash) | R012, R013, R016, R018 |
| Behavioural time-series (signal store, RedisTimeSeries, §7.12) | `customer_txn_rate_5m`/`_1h`, `velocity_ratio_1h`, `velocity_elevated_hours`, `hod_share_now`, `interarrival_cv`, `amount_trend`, `dormancy_days`, `device_surge`, `bene_surge` — velocity per-customer TS + amount per-customer TS + per-device and per-payee surge TS | R017, R019–R026 |
| Payment instrument risk / external reputation | blacklist membership (`bl:accounts`, `bl:devices`), watchlist | R001, R002, R008 |

**Deferred / stubbed** (need master data or feeds not in the fixtures):
payment-instrument risk (BIN country, card age, prior chargeback rate),
merchant MCC risk tier and merchant fraud rate, prior-chargeback and typical-spend
account history, ASN reputation and Tor/VPN detection beyond a static proxy list,
network/graph shared-attribute signals, and third-party (email/phone) reputation.

---

## 6. Feature Windowing & Time Categories

### 6.1 Configurable Time Windows

Rather than hardcoding "last 24 hours" and "24 hours–90 days" into feature logic, time windows are defined as data — a small config table that the Feature Service and Feature Store schema (§7) both read from. Two windows ship as defaults, but any number can be added later (a 1-hour window, a 7-day window, a 365-day window) without a code change, only a config update and a corresponding backing-bucket rollout.

| Config field | Description |
|---|---|
| window_id | Stable identifier referenced by metric definitions (e.g., `last_24h`, `aged_24h_90d`). |
| lookback_start / lookback_end | Offset from "now" defining the window's range (e.g., `last_24h` = [now−24h, now]; `aged_24h_90d` = [now−90d, now−24h]). |
| bucket_resolution | Granularity of the underlying rollup buckets used to answer queries over this window (e.g., 5 min for `last_24h` positional/velocity metrics, 1 hr for `last_24h` aggregates, 1 day for `aged_24h_90d`) — trades memory/compute for recency precision. In practice, the finest (5-min) resolution is only retained for a short recent sub-window (e.g., the last 1–2 hours), not the full 24h, since sub-minute-level precision beyond that has little marginal fraud-detection value and would otherwise multiply the hot tier's bucket count ~12x (§7.12). |
| tier | `hot` (kept in the low-latency in-memory feature store, always resident) or `warm` (coarser, longer-lived, still in Redis but sized/expired differently — see §7.11). |
| retention | How long a bucket for this window is kept before eviction — slightly longer than `lookback_end − lookback_start`, to allow for late-arriving events and clock skew. |

Default configuration:

| window_id | Range | Bucket resolution | Tier |
|---|---|---|---|
| `last_24h` | now − 24h → now | 5 min (positional/velocity), 1 hr (aggregates) | hot |
| `aged_24h_90d` | now − 90d → now − 24h | 1 day | warm |

This is deliberately a two-tier split, not just two arbitrary windows: `last_24h` needs fine-grained, low-latency, per-event resolution — it feeds the velocity and positional metrics that most directly catch fast-moving fraud — while `aged_24h_90d` only needs to establish a customer's or entity's longer-run baseline and tolerates much coarser (daily) buckets. Both are configuration entries of the same shape, so adding a future `last_1h` or `rolling_7d` window follows the same pattern and requires no code change to the Feature Service.

**Ownership:** for now, window and metric definitions (§6.1–6.2) are owned by engineering as config-as-code — changes go through a normal review/PR/deploy cycle into `cfg:windows`/`cfg:metrics` (§7.2), rather than through a risk-ops-facing UI. This still avoids a full code change/redeploy of the Feature Service for a new window or metric, but adding one is currently an engineering task, not a self-serve risk-ops action. A self-serve UI for risk ops remains a candidate future step (§12) if the change volume or turnaround-time pressure justifies it.

### 6.2 Metric Types & Naming

Metric names follow the convention `<entity>_<agg-or-type>_<field>_<window>[_<suffix>]`. Three metric types cover the examples in scope, and each maps to a different backing structure in the feature store (§7):

| Metric | Entity | Type | Window | What it computes |
|---|---|---|---|---|
| `time_since_last_failed_logon_24h_hrs` | customer_id | Time-since-last-event | `last_24h` | Hours since the customer's most recent failed logon; undefined/large if none occurred within the window. |
| `customerid_stddev_sendertransactionamo_1d` | customer_id | Streaming aggregate (stddev) | `last_24h` | Standard deviation of the sender's transaction amount, in base currency (USD, normalized at ingest — §3.2/§7.4), over the last day. |
| `c_snd_3rd_lst_pymt_dt_1h_rt` | customer_id | Positional / sequence | `last_24h` (1h sub-window) | Timestamp of the (n-2)-th previous payment — the event two transactions before the current one — restricted to the last hour. |

- **Time-since-last-event** metrics need only the single most recent occurrence timestamp per (entity, event type) — cheap to store and update, and directly answers "how long ago."
- **Streaming aggregate** metrics (mean, stddev, sum, count, min, max, rate) must not be computed by re-scanning raw events on every transaction at 1,000+ TPS — they are maintained incrementally as sufficient statistics (count, sum, sum-of-squares) in time-bucketed rollups, combined algebraically at read time (§7.4).
- **Positional / sequence** metrics need the actual ordered history of recent raw events per entity, not just aggregates — answered from a bounded ring buffer of the N most recent events (§7.5).

Every metric definition also declares its group-by entity (§6.3) and window_id, so the same three metric types apply uniformly whether the entity is `customer_id`, a beneficiary account, or a composite pair — only the key changes.

### 6.3 Group-By Entities

Rules and metrics are computed against several different entities, not just the customer:

| Group-by key | Entity | Fraud use |
|---|---|---|
| `customer_id` | Customer (primary) | Per-customer velocity |
| `receiver_transaction_bank_account_number` | Beneficiary account | Mule fan-in / bad-bene reuse |
| `[customer_id, receiver_transaction_bank_account_number]` | Customer → beneficiary pair | New-payee / relationship velocity |
| `customer_portfolio_country` | Geography | Cross-border, market cohort |
| `tpp_name_ud` | TPP / third-party provider | Payee / merchant concentration |
| `[receiver_transaction_bank_account_number, customer_portfolio_country]` | Beneficiary × country | Cross-border mule rings |
| `[tpp_name_ud, customer_portfolio_country]` | TPP × country | Merchant risk by market |

The same window and metric-type framework (§6.1–6.2) applies to every entity above — a `stddev` streaming aggregate over `last_24h` is computed identically whether it's grouped by `customer_id` or by `[tpp_name_ud, customer_portfolio_country]`; only the group-by key used to address the feature store changes (§7.3, §7.6).

---

## 7. Feature Store Data Model (Redis)

The Feature Service (§3.1) is backed by Redis, chosen because the 10 ms feature-assembly budget (§3.3) at 10M accounts and 1,000+ TPS sustained rules out any disk-backed lookup or full scan on the synchronous path — every read and write here must be O(1) or O(small constant), not O(n).

### 7.1 Design Principles

- **No raw-event scans on the hot path.** Streaming aggregates are maintained as pre-computed sufficient statistics, updated incrementally on write, never recomputed from history on read.
- **Bounded structures only.** Every per-entity structure has a fixed or capped size (ring buffers via `LTRIM`, bucket counts via TTL) so memory grows with the number of active entities, not with transaction history.
- **One round trip per read, one per write.** Feature assembly and the post-decision update each use a single pipelined batch (or Lua script) covering every key involved, so network round trips don't eat the latency budget.
- **Cluster-aware key design.** Keys for the same customer use a hash tag so multi-key reads/writes for one transaction land on a single shard and can be pipelined or scripted atomically.
- **Separate exact-precision needs from approximate ones.** Blacklist/membership checks that can tolerate a small false-positive rate use a Bloom filter to save memory at 10M+ entries; everything else uses exact Redis structures.

### 7.2 Data Category → Redis Structure Mapping

| Data category | Redis structure | Why |
|---|---|---|
| Last-event-timestamp trackers (e.g., `time_since_last_failed_logon`) | `HASH` — one hash per entity, one field per event type | Groups many small values under one key; `HSET`/`HGET` are O(1); avoids a key-per-metric explosion. |
| Streaming aggregates (mean, stddev, sum, count, rate) | `HASH` per (entity, metric family, time bucket), fields `cnt`/`sum`/`sumsq`/`min`/`max` | O(1) incremental update via `HINCRBY`/`HINCRBYFLOAT`; window aggregate = combine a bounded number of bucket hashes at read time (§7.4). |
| Positional / sequence history (e.g., "3rd previous payment") | `LIST` per entity, capped via `LPUSH` + `LTRIM` | `LINDEX`/`LRANGE` give O(1)/O(n) access to the last N raw events by position — the only structure that preserves order and identity of individual events. |
| Blacklists (bad accounts, devices, sanctioned entities) | `SET` for exact/moderate size; Bloom filter (RedisBloom `BF.ADD`/`BF.EXISTS`) for very large lists | `SISMEMBER`/`BF.EXISTS` are O(1); Bloom trades a small, tunable false-positive rate (never a false negative) for large memory savings at 10M+ entries. |
| Customer membership lists (VIP, trusted, watchlist, allowlist) | `SET` per list | Same O(1) membership semantics; small, curated lists don't need the Bloom trade-off. |
| Cross-entity aggregates (bene, pair, country, TPP, composite) | Same bucketed `HASH` pattern as streaming aggregates, keyed by the relevant group-by entity (§7.6) | Identical read/write pattern as customer-level aggregates — only the key prefix differs. |
| List / reference-data lookups (customer & beneficiary profile, IP-geo/proxy, BIN ranges, MCC risk tiers, ASN reputation) | `HASH` or `SET` (e.g. `c:{id}:profile`, `bene:{acct}:profile`, `geo:ip_prefixes`, `geo:proxy_ips`), refreshed by a batch job | Read-only or slowly-changing; held in Redis so the engine stays stateless and reads them in the same pipelined batch as everything else (§7.10) — never an in-process cache. No per-transaction write path. |
| Cached decisions (idempotency for retried requests) | `STRING` with TTL, set via `SET ... NX` | O(1) get/set; TTL bounds memory; `NX` gives idempotent "first writer wins" semantics for retries of the same transaction_id. |
| Window / metric configuration (§6.1–6.2) | `HASH` or `JSON` (RedisJSON), read-through cached by the Feature Service, invalidated via Pub/Sub on change | Lets risk ops add a window or metric without a deploy; a small, low-QPS config path, distinct from the per-transaction hot path. |

### 7.3 Key Schema

All keys for a given customer share a Redis Cluster hash tag (`{customer_id}`) so a single transaction's reads/writes stay on one shard and can be pipelined without cross-shard fan-out:

```
c:{<customer_id>}:last                            HASH    last-event timestamps (failed_logon, payment, login, ...)
c:{<customer_id>}:ring:payment                     LIST    capped ring buffer of recent raw payment events
c:{<customer_id>}:agg:<metric>:5m:<bucket_ts>       HASH    5-min rollup bucket (last_24h tier, fine resolution)
c:{<customer_id>}:agg:<metric>:1h:<bucket_ts>       HASH    1-hr rollup bucket (last_24h tier)
c:{<customer_id>}:agg:<metric>:1d:<bucket_date>     HASH    1-day rollup bucket (aged_24h_90d tier)
c:{<customer_id>}:pair:<bene_acct>:agg:...          HASH    customer→beneficiary pair aggregates (co-located w/ customer)
c:{<customer_id>}:declines:24h                      STRING  24h exact repeat-declines counter (R014; INCR in the customer Lua)
c:{<customer_id>}:benes:24h                         SET     24h exact distinct-beneficiary set (R015; SADD in the customer Lua)
c:{<customer_id>}:profile                          HASH    reference/master data: country, account_open_date, risk_segment (§7.2)
```

Cross-customer entities that aren't naturally addressed via a customer hash tag get their own key space, sharded by their own value:

```
bene:{<bene_acct>}:agg:<metric>:<res>:<bucket>      HASH    beneficiary-account aggregates (mule fan-in)
geo:{<country>}:agg:<metric>:<res>:<bucket>         HASH    geography-level aggregates
tpp:{<tpp_name>}:agg:<metric>:<res>:<bucket>        HASH    TPP-level aggregates
bene:{<bene_acct>}:geo:{<country>}:agg:...          HASH    beneficiary × country composite
tpp:{<tpp_name>}:geo:{<country>}:agg:...            HASH    TPP × country composite
bene:{<bene_acct>}:profile                          HASH    reference/master data: beneficiary country (§7.2)
device:{<device_fp>}:customers                      SET     distinct customers seen on a device (device-sharing, §5)
```

Non-per-entity structures:

```
bl:accounts                   SET or Bloom filter    global blacklisted beneficiary accounts
bl:devices                    SET or Bloom filter    blacklisted device fingerprints
list:vip_customers             SET                    trusted/low-friction customers
list:watchlist                 SET                    elevated-scrutiny customers
decision:{<customer_id>}:<transaction_id>  STRING (TTL)  cached final decision; hash-tagged by customer so the NX guard is atomic with the write (§7.8, §7.9)
cfg:windows                    HASH / RedisJSON        window definitions (§6.1)
cfg:metrics                    HASH / RedisJSON        metric definitions (§6.2)
geo:ip_prefixes                HASH                    IP prefix → country (reference/master data, §7.2)
geo:proxy_ips                  SET                     known proxy / VPN IPs (§7.2)
```

### 7.4 Streaming Aggregates: Bucketed Sufficient Statistics

To get `stddev`/`mean`/`sum`/`count` over an arbitrary window without ever scanning raw transactions:

1. On each transaction, `HINCRBY`/`HINCRBYFLOAT` the current time bucket's `cnt`, `sum`, and `sumsq` fields. Monetary amounts are normalized to the configured base currency (GBP in this build) at ingest (§3.2) *before* this increment, so a bucket's `sum`/`sumsq` are always single-currency and combine algebraically without any read-time FX conversion; the raw native amount is kept only on the durable record (§4.2). `min`/`max`, where a metric needs them, are maintained via a compare-and-set Lua snippet (Redis has no native atomic min/max on a hash field) and added only for the metrics that actually require them, to avoid the extra per-write cost.
2. To answer a window query (e.g., `last_24h`), read the last N bucket hashes covering that window (24 hourly buckets, or 288 five-minute buckets) in one pipelined `HMGET` batch, and combine algebraically:
   - `count = Σ cnt`, `sum = Σ sum`
   - `mean = sum / count`
   - `variance = (Σ sumsq / count) − mean²`, `stddev = √variance`
3. Each bucket carries a TTL of window length plus a small safety margin (e.g., 26h for `last_24h`, ~95d for `aged_24h_90d`), so old buckets self-evict — no explicit cleanup job needed.

This bounds read cost to a fixed number of bucket reads (24, or 90 for the daily tier) regardless of how many transactions occurred, and bounds write cost to a single O(1) increment per transaction per metric.

### 7.5 Positional / Sequence Metrics: Per-Entity Ring Buffer

For metrics like `c_snd_3rd_lst_pymt_dt_1h_rt` that need an actual ordered event, not an aggregate:

- Each relevant entity (e.g., customer) has a capped `LIST`: `LPUSH` a compact record (timestamp, amount, counterparty — packed, not full JSON, to keep entries small) on every new event, then `LTRIM 0 N-1` to cap it at N entries (N sized to the deepest positional lookback any metric needs, e.g., 20–50).
- Reading "the 3rd most recent, if within 1h" is `LINDEX key 2` (0-indexed) followed by an application-side timestamp check against the 1h bound — O(1).
- This ring buffer is intentionally separate from the durable audit sink (§4.3): it exists purely to serve positional feature reads at low latency and is not the system of record — the async persistence path (§3.2 step 8) remains the durable log.

### 7.6 Cross-Entity Aggregates

The seven group-by keys in §6.3 all reuse the same bucketed-hash pattern (§7.4); only the key prefix and the write fan-out differ. A single incoming transaction updates several aggregate families in parallel (pipelined, not sequential): `customer_id`, the beneficiary account, the customer→beneficiary pair, the customer's portfolio country, the TPP, and the two composite pairs (bene×country, TPP×country) — up to seven bucket updates per transaction, batched into one round trip via pipelining or a single Lua script so it stays within the write-path budget (§7.9).

Composite keys (pair, bene×country, TPP×country) are formed by concatenating the component values with a fixed delimiter; where a component value can be long or high-cardinality (e.g., full account numbers), consider hashing it into the key to bound key length, while keeping the readable form in the value/config for debugging.

### 7.7 Blacklists, Membership Lists & List Lookups

- Small-to-moderate curated lists (VIP, watchlist, internal blocklists in the thousands-to-low-millions) use a plain `SET` with `SISMEMBER`/`SMISMEMBER` (the latter checks multiple members in one round trip).
- Large reference blocklists (e.g., a global bad-account or bad-device list in the tens of millions) use a Bloom filter (RedisBloom `BF.ADD` on ingest, `BF.EXISTS`/`BF.MEXISTS` on read) to keep memory sublinear in list size, accepting a small, tunable false-positive rate — which only ever pushes a transaction toward extra scrutiny, never causes a missed block, since a true bad entity is never a false negative in a Bloom filter.
- Both are read-mostly on the hot path; updates (new blacklist entries, membership changes) come from an out-of-band feed, not from the transaction path itself.

### 7.8 Cached Decisions

`decision:{<customer_id>}:<transaction_id>` is a `STRING` (JSON-encoded score/decision/model_version) written with `SET ... NX EX <ttl>` immediately after a decision is made. It is hash-tagged by customer so it shares a slot with that customer's feature keys, letting the NX guard and the customer-side feature write run in one atomic Lua script (§7.9). If the gateway retries a request (e.g., after a client timeout that the server actually completed), the retry does a cache lookup first and returns the cached decision instead of rescoring — this is primarily an idempotency mechanism for a payment-safety-critical path, not a latency optimization. TTL (e.g., 24h) bounds memory; it does not need to match any feature-window TTL.

### 7.9 Write Path (Post-Decision Update)

Feature-store writes happen after the synchronous decision is returned (alongside the signal-store update and the audit-sink write, §3.2 step 8), since a transaction's own effect should not be visible to its own scoring. A single Lua script (or pipelined `MULTI`/`EXEC`) per transaction performs every update — ring-buffer push+trim, bucket increments for every applicable window/entity combination, last-event timestamp updates — as one atomic round trip per affected shard, so write amplification (up to ~7 entity families × 2–3 window tiers) doesn't turn into 15+ separate network round trips.

### 7.10 Read Path (Feature Assembly)

To fit the 10 ms feature-assembly budget (§3.3), the Feature Service issues one pipelined batch per transaction covering every metric the active rule set and model actually need: `HMGET` for last-event trackers, `HMGET` across the bounded set of bucket hashes for each streaming aggregate and window, `LRANGE`/`LINDEX` for positional metrics, and `SMISMEMBER`/`BF.MEXISTS` for list/blacklist checks — all in one round trip, with the algebraic combination (§7.4) done in the Feature Service after the pipeline returns.

### 7.11 Sizing Notes

- 10M customers × a handful of last-event fields + a bounded ring buffer + ~24 hourly and ~90 daily aggregate-bucket hashes per active metric family is a large but bounded and roughly linear-in-customers footprint — dominated by the hot-tier (`last_24h`) buckets, since the warm tier (`aged_24h_90d`) buckets are far fewer per customer (90 daily vs., e.g., 24 hourly × several metric families) but held for longer.
- Cross-entity aggregates (bene account, TPP, country, composites) scale with the number of distinct entities of each type, which is normally far smaller than 10M customers (fewer beneficiary accounts, far fewer countries/TPPs), so these are cheap by comparison — the pair aggregate (customer × bene) is the exception, since it can approach customer-count × average-payee-count in the worst case, and should be monitored for memory growth.
- These are again back-of-envelope directional notes (§2.3); a concrete per-metric byte budget should be modeled once the final metric catalog is fixed, to size the Redis cluster (node count, memory per shard, replica factor).

### 7.12 Redis Enterprise Sizing & Topology

This turns §7.11's directional notes into a concrete estimate for a **single region's** cluster (i.e., a region carrying the full 10M-customer / 1,000 TPS baseline from §2.2–§2.3; a region with a smaller customer share scales down proportionally, per §3.4). Figures below are back-of-envelope planning inputs, not a committed capacity plan — they should be revalidated against the load test in §13 before finalizing hardware.

**Key-space memory estimate**

| Key space | Entities | Structure / entity | Est. size / entity | Est. total |
|---|---|---|---|---|
| Customer core (`c:{id}:last`, `ring`, own `agg` buckets) | 10,000,000 | 1 last-event HASH + 1 ring LIST (~50 entries) + hourly buckets (24, last_24h) + fine 5-min buckets (24, last ~2h only — §6.1) + daily buckets (90, aged_24h_90d) | ~30–35 KB | ~300–350 GB |
| Customer→beneficiary pair (`c:{id}:pair:*`) | ~4 pairs/customer avg (10M × 4) | Lightweight rolling counters (first-seen ts, hot count/sum, warm count/sum) — deliberately *not* the full bucket set used for the primary customer entity, to bound the growth risk flagged in §7.11 | ~0.3 KB/pair | ~12 GB |
| Beneficiary account (`bene:{acct}:agg:*`) | ~0.5–2M distinct (§13.1) | Same bucketed pattern as customer core, at bene scale | ~24 KB | ~12–48 GB |
| Beneficiary × country (`bene:*:geo:*:agg:*`) | ≈ beneficiary count (most benes map to ~1 country) | Same pattern | ~24 KB | ~12–48 GB |
| Geography, TPP, TPP × country | Hundreds total | Same pattern, trivial cardinality | ~24 KB | < 0.1 GB |
| Blacklists (`bl:accounts`, `bl:devices`) | 10–20M entries | RedisBloom filter, ~0.1% target FP rate | ~1.8 bytes/entry | ~0.02–0.04 GB |
| Membership lists (VIP/watchlist) | Thousands | Plain `SET` | negligible | < 0.1 GB |
| Cached decisions (`decision:*`) | ~86M/day resident (24h TTL, §2.3) | `STRING`, ~150 bytes incl. key overhead | ~150 bytes | ~13 GB |
| Config (`cfg:windows`, `cfg:metrics`, `cfg:rules`) | Low hundreds of entries | `HASH`/RedisJSON | negligible | < 0.1 GB |
| **Raw subtotal** | | | | **~350–475 GB** |
| + Redis object/fragmentation overhead (~25%) | | | | **~440–590 GB** |
| + growth headroom (target ~1.5x today's customer count) | | | | **~650–900 GB target provisioned primary capacity** |

**Throughput check**

- **Feature store**, estimated Redis commands per transaction: ~20 on the read path (feature assembly, §7.10) + ~30 on the write path (post-decision update fanning out across the 7 group-by entities, §7.9) ≈ **~50 commands/transaction**.
- **Signal store** (separate cluster), per transaction: ~12 on the read path (`MGET` declines + 2× `PFCOUNT` + 4× `HMGET` amount stats + ~5 `TS.RANGE`/`TS.GET`, `SignalReader`) + ~10 on the write path (`SET NX` guard, `INCR`, 2× `PFADD`, 4× `TS.ADD`, 3× `HINCRBY*`, `SignalWriter`) ≈ **~22 commands/transaction**, all pipelined.
- At 1,000 TPS sustained → ~50K ops/sec (feature) + ~22K ops/sec (signal); at 3,000–5,000 TPS burst (§2.3) → up to ~250K + ~110K ops/sec.
- Both clusters are comfortably below what even a modest shard count can serve — at this customer/TPS ratio each is **memory-bound, not throughput-bound**: shard count is driven by the memory estimates, which then provide far more aggregate ops/sec capacity than needed. The one caveat is **key skew**: signal-store TimeSeries keys must be hash-tagged by a high-cardinality entity (`{customer_id}`/`{device}`/`{bene}`) so no single shard becomes a hot spot (an earlier low-cardinality per-country series concentrated load onto ~4 shards — see the change record at the top and §10.1).

**Recommended topology (Redis Enterprise Software, per region)**

| Parameter | Recommendation | Rationale |
|---|---|---|
| Cluster nodes | 6 nodes, 2 per availability zone across 3 AZs within the region | HA and rack/AZ-level fault tolerance without any cross-region data movement (§3.4). |
| Node memory | ~512 GB RAM per node (~450 GB usable after Redis Enterprise's reserved overhead) | 6 × 450 GB ≈ 2.7 TB usable cluster capacity against a ~900 GB target — roughly 3x headroom for growth, replica overhead, and uneven shard placement. |
| Feature-store database (BDB) | ~40 primary shards, ~20–25 GB each | Redis Enterprise's commonly cited shard-size ceiling for fast failover/rebalance; 40 shards on the ~900 GB estimate keeps individual shards well under that ceiling. |
| Replication | Replica-per-shard (factor 2), replica placed on a different node than its primary | Standard Redis Enterprise HA; ~1.8 TB total RAM footprint (primary + replica) fits within the 2.7 TB usable capacity. |
| Separate signal-store BDB | 90-day **approximate** hot-window signals (§4.3, §7.12): HyperLogLog (fan-out/fan-in), rotating monthly counters (declines), a count/sum/sumsq hash (amount z-score), and **RedisTimeSeries** (velocity + behavioural rules R019–R026). **RAM-only, no flash, no search index.** Measured (docker-compose): non-TS signals ~355 B/customer (~1.8 GB at 5M); per-customer velocity TS ~4.4 KB + amount TS ~12.7 KB/series (~88 GB at 5M); device/payee surge TS use a short 2-day retention so they add only ~1–2 GB. → **~90–95 GB** total at 5M customers. | A second RAM Redis cluster, sized independently of the feature store. Keys are hash-tagged by high-cardinality entities so shards stay balanced. Blacklists/membership/cached-decisions/config stay in the feature BDB. |
| Modules | RedisBloom (blacklists, §7.7); **RedisTimeSeries** (signal store, §4.3). | Both bundled into core Redis 8 / Redis Enterprise — no separate install. The old Query Engine / search module is no longer required (the transaction store was removed). |
| Persistence | AOF (`appendfsync everysec`) + periodic RDB snapshot on both stores | Fast recovery after a node event; not the *sole* durability guarantee — feature/signal state is derivable and can be rebuilt (feature aggregates via the write path §7.9; signals re-warm from live traffic), and the durable audit sink (§4.3) is the system of record. |
| Auto Tiering (Redis Flash) | **Neither store.** | Both the feature working set and the signal set must stay fully in RAM to meet the 10 ms budget (§3.3); with the multi-TB transaction store removed there is no longer any flash-tiered database in the design. |
| Active-Active (CRDB) | **Not used** | Each region is an independent, isolated deployment by design (§3.4) — there is no cross-region replication to support. |

This sizing should be treated as a starting point for the load test in §13, not a final number — actual per-metric byte sizes, real payee-count distributions, and observed Redis overhead should replace the estimates above once measured.

---

## 8. Scoring Engine Design

### 8.1 Rules Engine

The rules engine evaluates a configurable, ordered set of deterministic rules. Rules are managed outside of code (e.g., a rules table/config service) so risk operations can add or adjust them without a deploy.

- **Hard block rules** — short-circuit to an immediate decline (e.g., payment instrument on a confirmed-fraud blocklist, sanctioned-country billing address).
- **Hard allow rules** — short-circuit to an immediate approve for very low-risk, trusted cases (e.g., long-tenured account with a strong history and low transaction amount), used sparingly to reduce friction.
- **Soft/scoring rules** — do not force an outcome but contribute signal to the score (e.g., "new device + high amount" adds risk). These are incorporated as model features (§8.3) and so take full effect on the next retrain (§8.4); for urgent patterns that can't wait for a retrain, risk ops use the thin post-model override layer (§8.3) or express the pattern as a hard rule.
- Rules read their inputs from the feature store (§7) — a rule referencing, say, `[tpp_name_ud, customer_portfolio_country]` velocity simply addresses that entity's aggregate keys, using the same window/metric framework as every other rule.
- Every rule fire is logged with its rule ID and outcome, both for the audit trail and to measure each rule's individual precision/recall over time.

**Hot-window rules computed from streaming signals (§4.3, §7.12).** Most rules read pre-aggregated feature-store counters. The hot-window rules that need history no single feature counter summarizes are computed from **streaming approximate signals** in the signal store rather than by scanning a transaction log — updated per-transaction by `SignalWriter` and read by `SignalReader` (concurrently with feature assembly; degrade to safe defaults on failure, §3.3/§3.5). Where it makes sense the same idea is computed over **both** a 24h *exact* window (feature store) and a 90d *approximate* window (signal store):

| Rule | Type (weight) | Condition | Signal source |
|---|---|---|---|
| **R014** repeat declines 24h | soft (0.45) | `customer_declines_24h >= 3` | feature-store counter `c:{id}:declines:24h` (exact) |
| **R015** beneficiary fan-out 24h | soft (0.35) | `customer_distinct_bene_24h > 8` | feature-store Set `c:{id}:benes:24h` (exact `SCARD`) |
| **R012** repeat declines 90d | soft (0.4) | `customer_declines_90d >= 3` | signal-store rotating monthly counters (`MGET` + sum) |
| **R013** beneficiary fan-out 90d | soft (0.35) | `customer_distinct_bene_90d > 15` | signal-store HyperLogLog `PFCOUNT` (distinct benes) |
| **R016** mule fan-in 90d | soft (0.4) | `bene_distinct_senders_90d > 30` | signal-store HyperLogLog `PFCOUNT` (distinct senders per payee) |
| **R017** velocity burst 5m | soft (0.4) | `customer_txn_rate_5m > 5` | signal-store velocity TimeSeries (`TS.RANGE`, recent count) |
| **R018** amount anomaly 90d | soft (0.4) | `amount_zscore_90d > 3` | signal-store count/sum/sumsq hash (z-score) |
| **R019** velocity spike vs baseline | soft (0.45) | `velocity_ratio_1h > 8` | velocity TS, current-1h vs mean hourly baseline |
| **R020** sustained elevation | soft (0.4) | `velocity_elevated_hours >= 4` | velocity TS, hours above 2× baseline in last 6h |
| **R021** off-hour activity | soft (0.3) | `hod_share_now < 0.01` | velocity TS, this hour-of-day's historical share |
| **R022** machine cadence | soft (0.4) | `interarrival_cv < 0.15` | velocity TS, coefficient of variation of inter-arrival gaps |
| **R023** amount bust-out | soft (0.45) | `amount_trend > 0.15` | amount TS, weekly-average upward trend |
| **R024** dormant reactivation | soft (0.35) | `dormancy_days > 60` | velocity TS, gap since last event |
| **R025** device velocity surge | soft (0.4) | `device_surge > 5` | per-**device** velocity TS `sig:dev:{device}:vel` (2-day retention) |
| **R026** payee inbound velocity surge | soft (0.4) | `bene_surge > 5` | per-**payee** velocity TS `sig:b:{bene}:vel` (2-day retention) |

The velocity/amount TimeSeries are keyed per customer (`sig:c:{id}:vel`, `:amt:ts`); the surge series are keyed per device / per payee — all **high-cardinality** hash tags, so writes and range-scans spread across shards. TimeSeries is bundled in Redis 8 / Redis Enterprise and driven from Lettuce via async `dispatch` (`SignalTimeSeries`). New-payee (R006) still uses the existing customer→beneficiary pair state.

### 8.2 Machine Learning Model

A supervised binary classifier predicts the probability that a transaction is fraudulent, trained on historical labeled transactions (confirmed fraud/chargeback vs. confirmed legitimate).

- **Model family:** gradient-boosted decision trees (e.g., XGBoost/LightGBM) as a starting point — strong performance on tabular features, fast inference, and reasonably interpretable via feature importance/SHAP values. A deep/sequence model can be considered later if behavioral/sequence features become central.
- **Training data:** historical transactions joined with outcome labels from the feedback loop (§8.4), with careful handling of label latency (a transaction isn't "confirmed legitimate" until enough time has passed with no chargeback).
- **Class imbalance:** fraud is rare relative to legitimate volume; use class weighting or resampling during training, and evaluate with precision/recall and PR-AUC rather than raw accuracy.
- **Output:** a calibrated probability (0–1) so scores are comparable across model versions and interpretable as "likelihood of fraud."
- **Enable/bypass toggle:** ML scoring is gated by `fraud.model.enabled` (default `false`). When `false`, the model is not even loaded and scoring bypasses it entirely, relying on the rules + decision bands (§8.3); when `true`, the embedded model is loaded and consulted as an extra check. This lets the engine run rules-only or rules-plus-model with a single config flag.
- **Reference-implementation note:** the model shipped in this build is *illustrative* — a logistic model trained by `ml/train.py` on weak labels derived from the injected fraud patterns (§13.1), behind a `ModelScorer` interface. It demonstrates the serving pipeline and the toggle, not production accuracy; a real model (gradient-boosted, ONNX-served) needs labelled seed data and drops in behind the same interface.
- **Model deployment & versioning:** the model is embedded in the scoring engine and shipped as part of the app deploy (§3.6). Every deployed model has a version ID, recorded on every scored transaction. A/B comparison and rollback are done at the deploy layer — canary/blue-green cohorts of instances running different bundled model versions, compared on the §10.2 metrics — rather than by routing to a separate model service. Because the weekly retrain cadence (§8.4) now rides the app-release path, model promotion needs a lightweight, frequent release process.

### 8.3 Decision Blending

The Decision Engine combines rule outcomes and the model score into a single final decision:

- If a hard-block or hard-allow rule fired, that outcome wins outright (rules override the model in these cases by design — they encode known, certain business logic) and stays immediately editable by risk ops without a retrain (§8.1).
- Otherwise, the fired rules produce a deterministic risk score (the sum of their contributions), mapped through the decision bands below. **These bands apply whether or not the ML model is enabled.** When the model is enabled (`fraud.model.enabled=true`, §8.2) it runs as an *extra check*: the final score becomes `max(rule_score, model_probability)`, so the model can only escalate risk, never soften a rule-derived outcome. When disabled, the model is bypassed entirely and the rule score alone drives the bands.

| Final Score Range | Decision | Typical Action |
|---|---|---|
| [0.00, 0.30) | Approve | Transaction proceeds automatically. |
| [0.30, 0.70) | Review | Returned to the PSP as a soft-decline the customer can retry via another method; the transaction is also routed to the asynchronous manual review queue, which produces labels (§8.4) but never holds the live authorization. |
| [0.70, 1.00] | Decline | Transaction is blocked automatically. |

*Thresholds are configurable per merchant/segment and tuned against business targets for fraud loss vs. false-decline rate; they are not hardcoded constants. Bands are half-open so every score falls in exactly one band.*

### 8.4 Feedback Loop

Ground-truth labels arrive after the fact, from two main sources:

- **Chargebacks** — arrive from the payment processor, typically 1–90+ days after the transaction; a strong but delayed and imperfect fraud signal (chargebacks also occur for non-fraud disputes).
- **Manual review outcomes** — analysts reviewing "review"-bucket transactions produce a faster, more direct fraud/not-fraud label, usually within hours to days.

Labels are attached to the original transaction record (see `outcome_label` field, §4.2). The training pipeline runs on a regular cadence (e.g., weekly) to incorporate newly labeled data, evaluate the candidate model against the current production model on a held-out set, and promote it if it improves target metrics without regressing others.

---

## 9. Security, Privacy & Compliance

- **PCI DSS scope:** the fraud system must never store raw PAN, full magnetic-stripe/chip data, or CVV. It operates on tokens issued by the payment processor/tokenization service.
- **Encryption:** all data encrypted in transit (TLS) and at rest; sensitive fields (IP, device fingerprint) are hashed or encrypted with restricted-access keys.
- **Access control:** role-based access to the audit sink and review queue; raw feature/score data access is logged and auditable, since it can reveal behavioral patterns about real customers.
- **Authentication (reference build gap):** the scoring endpoint is currently unauthenticated for demo simplicity. In production the Transaction Gateway (§3.1) must authenticate every caller. Recommended: a Spring Security filter validating a short-lived signed token (JWT with the PSP/merchant as issuer) or mutual TLS between the PSP integration and the gateway, plus per-caller rate limiting; reject unauthenticated requests before feature assembly. The load test (§13.3) is written to attach such a token per virtual user once auth is enabled.
- **Data residency:** the system is deployed per region (§3.4), with no cross-region replication of customer or transaction data — this is the primary mechanism for satisfying data-residency requirements, rather than a data-masking or field-level residency control within a single global deployment.
- **Data retention:** the signal store keeps a rolling ~90–120-day window of approximate signals (bucket/series TTLs, §7.12) plus the short-lived 2-day surge series — self-trimming, no raw transactions retained; audit-sink retention follows the business's compliance/audit requirements (often multi-year for financial records), with PII minimization/anonymization applied where feasible (§4.3).
- **Explainability for disputes:** because declines affect real customers, the system must be able to reconstruct, for any transaction, which rule(s) fired and which features drove the model score — required for both customer service and regulatory inquiries.

---

## 10. Monitoring & Observability

### 10.1 System Health Metrics

- Latency (p50/p95/p99) for the overall scoring path and each component.
- Throughput and error rates per component.
- Fallback/degraded-mode activation frequency (see §3.5).
- Redis feature-store health: memory usage per key category (§7.11), hit rate, hot-key/shard imbalance (particularly for high-cardinality composites like the customer×bene pair).
- Redis signal-store health: per-shard CPU and memory, and **hot-key/shard imbalance** in particular — the behavioural signals live on shared TimeSeries/HyperLogLog keys, so a low-cardinality hash tag would concentrate load onto a few shards (as an earlier per-country velocity series did). Alert on skewed per-shard CPU; signal keys are deliberately tagged by high-cardinality entities (`{customer_id}`/`{device}`/`{bene}`) to keep the distribution flat (§7.12).

### 10.2 Model & Business Metrics

- Score distribution over time, monitored for drift (a sudden shift suggests either a changing fraud pattern or a data/feature pipeline bug).
- Approval / review / decline rate, overall and by merchant/segment.
- Precision and recall against confirmed labels, once available, per model version.
- False-positive rate (legitimate transactions declined) — tracked as a business cost (lost revenue, customer friction), not just a model metric.
- Fraud loss rate (confirmed fraud that was approved) — the primary business cost the system exists to reduce.
- Per-rule fire rate and precision, to identify stale or noisy rules.

---

## 11. Tradeoffs & Alternatives Considered

| Decision | Alternative Considered | Rationale |
|---|---|---|
| Synchronous scoring in the auth path | Async scoring, approve-then-reverse | Blocking bad transactions before funds move is far cheaper than clawing back after the fact; the business explicitly wants real-time blocking. |
| Gradient-boosted trees for the ML model | Deep neural network / sequence model | GBTs offer strong tabular performance, fast CPU inference, and easier feature-importance explainability; a sequence model is a reasonable future upgrade once rich behavioral/session-sequence data is available. |
| Hybrid rules + ML | ML-only scoring | Hard rules give instant, fully explainable control for known-bad patterns (sanctions lists, confirmed fraud rings) that risk ops can change without a retrain; soft signals fold into the model as features for joint calibration, with a thin live override layer (§8.3) preserving fast reaction between retrains. |
| Streaming approximate signals in RAM (HyperLogLog / counters / stats hash / TimeSeries) | Storing every transaction in an indexed 90-day store (the earlier Redis Flex + Query-Engine design) and counting via `FT.SEARCH` at scoring time | The scoring path only needs *aggregate* answers (how many declines, how many distinct payees, current velocity vs baseline), not the raw rows. Maintaining those incrementally is bounded by active-entity count (tens-to-~130 GB, all RAM) instead of transaction volume (multi-TB on flash), removes the Flex Query-Engine constraints, and keeps every read O(1)/O(bounded). The cost is approximation (HyperLogLog ~0.8% error) and that a brand-new signal needs history to warm — both acceptable for these rules. |
| Durable audit sink behind a pluggable interface, off the scoring path | An indexed transaction store the engine reads back on the hot path | The full record is still needed for audit/training, but nothing about a transaction's own record feeds scoring — so persistence can be a fire-and-forget durable write (log/queue/warehouse) with no index and no read-back, keeping it off the latency budget entirely. |
| Fail-open to rules-only on ML outage | Fail closed (decline everything) on ML outage | Blocking all payments during an infrastructure blip causes direct, immediate revenue loss and customer harm that likely exceeds the fraud risk of a temporary rules-only mode. |
| In-memory feature store (Redis) for all customer/entity aggregates | Disk-backed key-value store (e.g., a managed NoSQL database) | A 10 ms feature-assembly budget at 10M accounts / 1,000+ TPS requires memory-speed reads; disk-backed stores' typical single- to double-digit ms p99, plus network/disk tail latency, leave no margin for the rest of the request. |
| Bucketed sufficient-statistics rollups for streaming aggregates | Store raw event history and compute aggregates on read | Recomputing stddev/mean from raw events on every request doesn't stay bounded as transaction volume grows, and would blow both the latency budget and memory; bucketed counters give O(1) writes and O(bucket count) reads regardless of history depth. |
| Bloom filter for large reference blocklists | Exact `SET` for all blocklists | At 10M+ entries, an exact set costs materially more memory for a check that only needs to bias toward extra scrutiny, not guarantee zero false positives. |
| Stateless Java/Spring Boot scoring engine, all state externalized to the Redis feature/signal stores + audit sink | Stateful instances with local caching/session affinity | A stateless engine scales by adding instances behind a load balancer with no sharding logic of its own, and supports rolling deploys/failover without losing in-flight state — the sharding/partitioning complexity is pushed down into Redis Cluster (§7.3), where it belongs given the feature data already lives there. |

---

## 12. Open Questions & Future Work

- Should decision thresholds vary by merchant risk tier from day one, or start global and be split out later?
- What is the acceptable manual-review queue volume/staffing, and how should it bound the width of the "review" score band?
- Should network/graph-based signals (shared device/address across accounts) be a first-class real-time feature, or computed offline and periodically refreshed given their higher compute cost?
- What is the target initial fraud-loss and false-decline rate the business wants to hit, to calibrate the score-band thresholds in §8.3?
- ~~Multi-region/data-residency requirements for the Transaction Store?~~ Resolved: deploy per region, one full independent stack per region, no cross-region replication (§3.4). Open follow-up: how to detect fraud rings that deliberately span multiple regions, given this design can't correlate across them by construction.
- Real-time vs. near-real-time graph feature computation trade-off, and whether a graph database is warranted at current scale.
- ~~What per-metric Redis byte budget and cluster topology does the design imply?~~ Estimated in §7.12 (~650–900 GB target capacity, 6-node/40-shard Redis Enterprise cluster per region) — treat as a starting point to be validated against §13's load test, not a final number.
- Should the customer×beneficiary pair aggregate have its own memory cap/eviction policy, given its worst-case growth toward customer-count × average-payee-count?
- ~~Who owns the window/metric configuration (§6.1–6.2) day to day?~~ Resolved: engineering owns it as config-as-code for now (§6.1). Revisit if change volume grows enough to justify a risk-ops self-serve UI.
- What is the final injected fraud-pattern mix and ratio for seed data (§13.1) — to be finalized with risk/data science rather than assumed by engineering alone.
- Should the failure-injection load-test scenario (§13.3, scenario 5) be a one-off validation or a standing release gate?
- Which layer guarantees a transaction reaches the initiating customer's home-region gateway (§3.4) — PSP-side routing, a global geo-router, or a gateway-level redirect? A misrouted request lands on a region whose feature store has none of that customer's state and would mis-score.
- Is the 99.95% availability target (§2.2) measured per region? With no cross-region failover by design (§3.4), a regional outage is total for that region's customers, and the "total outage" fallback (§3.5) is itself region-scoped — both should be stated as per-region.
- How long must a transaction go without a chargeback before it is labeled `confirmed_legit` (§8.2)? This maturation window gates training-data freshness and the retrain cadence (§8.4).
- Should the thin post-model soft-override layer (§8.3) carry a hard magnitude bound and a mandatory expiry, so live risk-ops adjustments can't silently persist and de-calibrate the model indefinitely?

---

## 13. Test Data & Load Testing

Two distinct test-data needs follow from the design above: (1) realistic seed data so the system isn't tested against an empty, unrealistic feature store, and (2) a Gatling load test that actually exercises the 1,000 TPS / p99 ≤ 30 ms target from §2.2, using that same seed data rather than synthetic traffic disconnected from it.

### 13.1 Initial Setup / Seed Data

Seed data should approximate the scale assumptions in §2.3 — testing against 10,000 customers when the design targets 10M customers would validate correctness but tell us nothing about the latency and memory behavior the rest of this document is built around.

A separate, much smaller fixture set — `test_data/` in the project folder — exists purely to check functional correctness on a laptop: 50 customers, 20 beneficiaries, 5 TPPs, a 482-transaction backfill with deliberately injected scenarios (velocity burst, mule fan-in, blacklist hits, an established vs. new payee, a watchlist customer, a VIP fast-track), and 11 sample requests with known expected decisions. It includes a generator (`generate_test_data.py`), a Redis loader implementing the §7.3 key schema at reduced resolution (`load_redis.py`), and a checker that posts the sample requests and diffs actual vs. expected decisions (`run_sample_requests.py`) — see `test_data/README.md` for the exact scenario table and the simplifications it takes versus this section's production-scale design (e.g., plain `SET` blacklists instead of RedisBloom, no 5-min sub-window buckets).

| Data category | Target volume | Purpose | Generation notes |
|---|---|---|---|
| Customers | 10,000,000 | `customer_id`, `customer_portfolio_country`, account-open date, risk segment — drives most single-entity aggregates (§6.3) and rules (§8.1). | Realistic country-distribution skew (a handful of core markets dominate); account age skewed toward "established," with a deliberate minority (~2–5%) opened in the last 30 days, since account age is itself a risk feature. |
| Payment instruments | ~15–25M | 1–3 per customer. | Tokenized, per §4.2 — never real PAN/account data even in test. |
| Beneficiary accounts | 500K–2M | Deliberately smaller pool than the customer base, to create realistic reuse. | Skewed so a small number of accounts receive from a disproportionate number of distinct customers — this is what exercises the mule fan-in group-by key (§6.3) and lets its rules actually fire during testing. |
| TPPs | Tens to low hundreds | Third-party providers, each with a country distribution. | Small, curated cardinality — matches real-world TPP concentration. |
| Devices | ~12–15M | 1–2 per customer typical. | A small deliberately-reused fraction across unrelated customers, to exercise the device-sharing fraud signal (§5). |
| Blacklists / membership lists | 10–20M (global blocklist), thousands (VIP/watchlist) | Exercises the Bloom-filter vs. `SET` split (§7.7) at realistic scale. | Include a known, fixed set of "seed bad" entities the load test can deliberately target (§13.3) to verify block rules still fire under load. |
| Historical backfill (feature + signal stores) | ~8.6 transactions/customer/day on average (from §2.3), with realistic diurnal/day-of-week variation, not uniform | Warms the feature-store `aged_24h_90d`/`last_24h` windows (§6.1) and the signal-store 90-day approximate signals (§7.12/§13.2) so aggregates, ring buffers, and distinct/count signals aren't cold on day one. Seeded directly into the target structures (no transaction log to replay). | Inject a small, deliberate fraction (~0.1–1%) of known fraud patterns (velocity bursts, mule fan-in, new-payee spikes) via the `--flagged` slice — gives rules something genuine to catch in testing, and (with `outcome_label`) seeds the first model-training run (§8.4). |
| Rule / window / metric configuration | Full catalog | Populates `cfg:rules`/`cfg:windows`/`cfg:metrics` (§7.2) with the default `last_24h`/`aged_24h_90d` windows and the group-by-key metric definitions (§6.3). | Owned/authored by engineering per §6.1's config-as-code process, even for test environments. |
| Initial ML model artifact | 1 | A cold-start model, version-tagged `seed-v0` (§8.2), trained on the injected synthetic fraud patterns above (or a simple heuristic stub if no training is done yet). | Explicitly a placeholder — replaced once the feedback loop (§8.4) accumulates real or injected labels. |

### 13.2 Feature-Store Backfill Strategy

The backfill must warm **both** RAM stores so nothing is cold on day one of a load test: the **feature store** (bucketed aggregates + ring buffers + the 24h exact hot-window signals, §7.4–7.5) and the **signal store** (the 90-day approximate signals — HyperLogLog / counters / stats hash, §7.12). It is seeded directly into the target Redis structures rather than replayed as raw transactions, since there is no longer any transaction log to replay.

Two parallel Python seeders (`test_data/`) do this and are the load-test critical path (`LOAD_TEST_RUNBOOK.md`):

- **`seed_feature_store.py`** — writes per-customer profile, aggregate buckets, ring buffer, and the 24h exact signals for the target customer count. Multiprocessing (`--procs`), sharded by `--customer-start`/`--total-customers` so several instances on separate VMs seed disjoint ranges concurrently; `--days` controls history depth and `--id-width` the id zero-padding.
- **`seed_signal_store.py`** — pre-warms the distinct/count/amount signals (R005/R012/R013/R016/R018) as HyperLogLog / counters / stats hashes, with a `--flagged` slice of high-risk customers (≥3 declines, >15 distinct beneficiaries, high fan-in) so the bundled Gatling feeder trips those rules out of the box. Also parallel and shardable.

The **behavioural TimeSeries signals (R017, R019–R026)** are deliberately **not** pre-seeded — they build up live as the engine scores during the run, and by design need history to accumulate before they engage (the surge series in particular use a 2-day retention). See `LOAD_TEST_RUNBOOK.md` for the exact 5M-customer / 1,000-TPS seeding commands and GCP sizing.

### 13.3 Gatling Load Test Design

Goal: validate the §2.2 targets — 1,000 TPS sustained, p50 ≤ 10 ms, p99 ≤ 30 ms — and the §2.3 burst-headroom assumption (3–5x, i.e., 3,000–5,000 TPS) — against the Transaction Gateway's scoring endpoint (§3.2).

- **Feeder:** a Gatling feeder drawn from the same seed corpus (§13.1) — real seeded `customer_id`s, beneficiary accounts, devices, TPPs — not freshly random values, so traffic exercises warm Redis keys matching real steady-state access patterns rather than an artificial 100%-cold-miss workload. The feeder should mirror realistic skew: most requests drawn from the general customer/beneficiary pool, with a small percentage deliberately targeting "hot" entities (a popular TPP, a handful of high-frequency customers) to stress per-key/hot-shard behavior in Redis Cluster, which §10.1 monitors.
- **Correctness injection:** a small fraction (1–2%) of requests deliberately constructed to hit a seeded blacklist entry or trigger a known velocity rule, so the test verifies decisions stay *correct* under load, not just fast — a pure throughput test can hide a rule silently failing to fire under contention.
- **Auth:** the scenario's request builder generates/reuses a valid signed token per virtual user or feeder row (per the Gateway's auth enforcement, §3.1), so the auth path is exercised rather than bypassed.

| Scenario | Injection profile | Purpose |
|---|---|---|
| Steady-state | `constantUsersPerSec(1000)` sustained for 15–30 min | Validates core p50/p99 targets under target load, not just an instantaneous burst. |
| Ramp-up | `rampUsersPerSec` 0 → 1,000 over ~2 min | Confirms the system reaches steady state smoothly (connection-pool ramp, JVM/JIT warm-up) without an initial-latency spike breaching SLA. |
| Burst / peak | 3,000–5,000 TPS for ~5 min | Directly validates the §2.3 burst-headroom assumption — this is the scenario that actually proves that requirement, not just the steady-state one. |
| Soak / endurance | 1,000 TPS sustained for 2–4 hours | Surfaces slow memory leaks, Redis memory growth beyond expected bucket-TTL eviction (§7.4), and GC-pause-driven tail-latency growth in the JVM (§3.6) — classes of problem a short burst test won't show. |
| Failure injection (recommended) | 1,000 TPS steady-state while degrading/killing the ML Scoring Service or a Redis node | Confirms the §3.5 fallback behavior actually activates and stays within budget under real load, rather than only ever exercising the happy path. |

**Assertions** (per §2.2): global p50 < 10 ms, p99 < 30 ms; failed-request rate ≈ 0% (a `decision: decline` response is a successful 200, not a failure); no meaningful fraction of requests exceed the hard 30 ms bound beyond the accepted p99 tail.

### 13.4 Test Environment & Isolation

- Load tests run against a dedicated, region-scoped test deployment (§3.4) — never against a production region — to avoid polluting real customer aggregates/blacklists or triggering production alerting.
- Both Redis stores (feature and signal) in the test environment should match the production sizing assumptions (§7.11–§7.12) as closely as practical: latency and memory behavior at full key/aggregate depth is not representative of a tiny test cluster, and testing against an undersized cluster would give a false pass.
- Synthetic `transaction_id`s carry a reserved prefix/namespace so any test traffic that leaks into logs or monitoring dashboards (§10) is visibly distinguishable from real traffic.

### 13.5 Real-Time Test Dashboard

A live view is needed while a test is running — not just the static HTML report Gatling produces after a run completes — so a person driving the test can see throughput and latency degrade (or not) as it happens, and stop early if something is clearly wrong.

- **Metrics source:** Gatling's open-source reporting is post-run only (no built-in live dashboard), so the dashboard reads from the *system under test's* own metrics rather than from Gatling directly — the Scoring Engine (§3.6) already emits per-request counters and latency histograms (Micrometer, consistent with §10's monitoring metrics) for every scored transaction. This also has the advantage of measuring what the service itself saw, not just what the client observed.
- **"Pass" vs "fail" definition:** pass = a request that received a scored decision within the response (any of approve/review/decline — a decline is a correct, successful response, not a failure, consistent with §13.3's assertions); fail = a transport-level error, timeout, or 5xx/exception — i.e., the load-test sense of pass/fail, distinct from the fraud decision itself.
- **Aggregation window:** metrics are aggregated over a short rolling window (e.g., the last 1s for the "current tx/s" and latency figures, and a running total since test start for the processed/pass/fail counts) — matching the p50/p99 definitions used elsewhere in this document (§2.2, §10).
- **Delivery to the UI:** a small metrics endpoint on the test harness (or a lightweight aggregator sitting between the Scoring Engine's metrics and the dashboard) is polled or pushed (Server-Sent Events/WebSocket) at ~1s intervals — frequent enough to feel "live" without materially adding load to the system under test.
- **Displayed fields:** transactions processed (running total), current throughput (tx/s, against the 1,000 tx/s target), pass count and pass %, fail count and fail %, p50 latency, and p99 latency (flagged visually if it crosses the 30 ms target, §2.2) — plus short rolling time-series for throughput and for p50/p99 so a trend or spike is visible, not just the instantaneous number.
- **Scope:** this view is specific to an active test run in the dedicated test environment (§13.4); it is not the production monitoring dashboard (§10), though it deliberately reuses the same metric definitions so a number that looks fine in testing means the same thing in production.

A working mockup of this dashboard, with simulated live data matching the fields above, is in [`load_test_dashboard_mockup.html`](load_test_dashboard_mockup.html) — a self-contained HTML file (open it in any browser; the Redis UI theme and fonts are inlined). It drives every displayed field from a 1s live simulation and exposes the five §13.3 scenarios as tabs; the p99 readout and its chart line turn Hyper Red when latency crosses the 30 ms target, which the burst and failure-injection scenarios exercise.
