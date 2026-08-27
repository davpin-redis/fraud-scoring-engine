# Laptop-scale test fixtures

Small, deterministic dataset + config for checking the fraud scoring system's
functional behavior locally — not a scaled-down copy of the 10M-customer
production dataset (see `Fraud_Scoring_System_Design_Doc.md` §13 for that).
This is sized to exercise every rule/metric path with a handful of entities
whose expected outcome is known ahead of time.

## What's here

```
test_data/
  reference/
    customers.json          50 customers (normal / new / vip / watchlist segments)
    beneficiaries.json       20 beneficiary accounts (2 flagged as mule pattern)
    tpps.json                 5 third-party providers
    blacklists.json          1 blacklisted account, 1 blacklisted device
    membership_lists.json    VIP + watchlist customer lists
  config/
    windows.json             last_24h / aged_24h_90d window definitions (§6.1)
    metrics.json              6 metric definitions, incl. the 3 examples from §6.2
    rules.json                13 rules: 2 hard-block, 1 hard-allow, 10 soft (§8.1);
                                 R012/R013 read the 90-day transaction hot window (§4.3)
  transactions/
    historical_backfill.jsonl   482 transactions over the last 10 days
    sample_test_requests.json   11 requests to send after loading, each with
                                 an expected_decision and expected_trigger

generate_test_data.py   regenerates everything above (seeded, deterministic)
load_redis.py           loads it all into a local Redis, per §7.3's key schema
run_sample_requests.py  POSTs sample_test_requests.json to your running
                         Scoring Engine and checks the returned decision
verify_loader.py         internal check only — not needed to use the fixtures
```

Everything is anchored to a fixed reference time, `2026-08-10T12:00:00Z`, baked
into both the generator and the loader — this is what makes the sample
requests' expected outcomes deterministic (e.g., "cust_012 made 9 transactions
in the last hour" is only true relative to that fixed clock, not real time).

## Scenarios encoded in the backfill

| Customer / entity | What's injected | Rule it should trip |
|---|---|---|
| `cust_012` → `bene_005` | 9 transactions in a 12-minute window, ~30 min before the reference time | `R004_velocity_burst` |
| `bene_018` | Paid by 14 distinct customers within 24h | `R005_mule_fan_in` |
| `cust_030` → `bene_017` | 3 payments over the last 90 days (establishes payee history, used as a *negative* test — large amount here should NOT trip the new-payee rule) | none (control) |
| `bene_020` | On the account blacklist | `R001_blacklisted_beneficiary` |
| `device_099` | On the device blacklist | `R002_blacklisted_device` |
| `cust_010`, `cust_027`, `cust_033` | Share `device_shared_01` | not wired to a rule in this fixture — available if you add a device-sharing rule |
| `cust_046`–`cust_048` | VIP membership list | `R003_vip_low_amount_fast_track` |
| `cust_049`, `cust_050` | Watchlist membership list | `R008_watchlist_customer` |

**R012/R013 are not in this backfill.** They read the *transaction store* (§4.3), which
this feature-store loader doesn't populate. To exercise them, seed the transaction
store with the engine's `TransactionSeeder` job (`--spring.profiles.active=seed`) — see
the root `README.md`. The Testcontainers integration test `HotWindowRuleIT` seeds a few
records directly and asserts both rules fire.

`transactions/sample_test_requests.json` turns each of these into a concrete
request + expected decision — see that file directly for the exact payloads.

## Running it on a laptop

1. **Start Redis** (any local instance works — Docker is the fastest path):
   ```
   docker run --rm -p 6379:6379 redis:7
   ```
2. **Load the fixtures**:
   ```
   pip install redis
   python3 load_redis.py --flush
   ```
   This replays the 482 backfill transactions through the same write-path
   logic described in §7.9/§13.2, so the aggregates, ring buffers, blacklists,
   membership lists, and config end up in the same state a real 10-day history
   would produce — not an empty feature store.
3. **Point your Scoring Engine at this Redis instance** and start it.
4. **Run the functional check**:
   ```
   python3 run_sample_requests.py --base-url http://localhost:8080 --path /v1/transactions/score
   ```
   This posts all 11 sample requests and prints a PASS/FAIL table against the
   `expected_decision` in each one. Adjust `--base-url`/`--path` to match your
   actual endpoint, and adjust `extract_decision()` inside the script if your
   response shape differs from the `decision` field in §4.2.

## Simplifications vs. the production design

These are deliberate, and called out in code comments in `load_redis.py` too:

- Blacklists use a plain Redis `SET`, not RedisBloom — avoids a module
  dependency for a two-entry test list. §7.7 covers the real Bloom-filter design.
- Only 1h (hot tier) and 1d (warm tier) buckets are populated — production
  also keeps a 5-min sub-window (§6.1) that isn't needed at this volume.
- `customer_txn_count_1h` is answered by scanning the small per-customer ring
  buffer rather than a dedicated bucket — fine at ≤50 entries; §7.4's bucketed
  counters are what production uses at scale.
- `bene_distinct_senders_24h` uses a `SET` with a 24h TTL (`SADD`/`SCARD`)
  rather than bucketed stats, since distinct-count doesn't combine additively
  across time buckets the way sum/count/stddev do.
- The customer→beneficiary pair is a single lightweight hash (count, sum,
  first-seen), matching §7.12's mitigation for that entity's memory-growth risk
  — not the full per-resolution bucket set used for the primary customer entity.
- Beneficiary×country and TPP×country composite aggregates (§6.3) are skipped
  here — no rule in this fixture reads them, and they follow the identical
  pattern as the other aggregates if you need to add them.
- `reference/*.json` (customers, beneficiaries, TPPs, IP-geo) stand in for
  slowly-changing master data. `load_redis.py` loads them into Redis reference
  keys — customer/beneficiary profile hashes (`c:{id}:profile`,
  `bene:{acct}:profile`) and IP-geo (`geo:ip_prefixes`, `geo:proxy_ips`) —
  alongside the aggregates, ring buffers, blacklists, and config (§7.2/§7.3).
  The engine reads reference data from Redis, not from files, so it stays
  stateless (any instance sees the same, current data).

## Regenerating with different scale/scenarios

Edit the constants at the top of `generate_test_data.py` (customer count,
country weights, injected scenarios) and re-run it — it's deterministic
(seeded) so the same edits always produce the same dataset, which is what lets
`sample_test_requests.json`'s expected outcomes stay valid.
