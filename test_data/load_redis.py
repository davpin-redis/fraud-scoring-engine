#!/usr/bin/env python3
"""
Loads the test_data/ fixtures into a local Redis instance, following the
Redis key schema in Fraud_Scoring_System_Design_Doc.md §7.3, at laptop scale.

Requires: pip install redis
Usage:    python3 load_redis.py [--redis-url redis://localhost:6379/0] [--flush]

Simplifications vs. the full production design (all noted so they're not
mistaken for the real design — see README.md):
  - Blacklists use a plain SET, not a RedisBloom filter (avoids a module
    dependency for a functional-only check; §7.7 covers the production case).
  - Only two rollup resolutions are used: 1h buckets for the last_24h tier
    and 1d buckets for aged_24h_90d (production also keeps a 5-min
    sub-window per §6.1 — unnecessary at this data volume).
  - customer_txn_count_1h is answered by scanning the (small, capped)
    per-customer ring buffer rather than a separate bucket structure —
    reasonable at N<=50 ring entries; §7.4's bucketed counters are what
    production uses at scale.
  - bene_distinct_senders_24h uses a Redis SET with a 24h TTL (SADD per
    transaction, SCARD to read) rather than bucketed sufficient statistics,
    since distinct-count doesn't combine additively across buckets the way
    sum/count/stddev do.
  - Customer -> beneficiary pair state is a single lightweight hash
    (cnt/sum/first_seen), per §7.12's mitigation for that entity's
    memory-growth risk, not the full per-resolution bucket set used for
    the primary customer entity.
  - Geography/TPP-level aggregates are populated for completeness but no
    sample rule in this fixture reads them; bene x country / TPP x country
    composites are skipped entirely to keep this script short — they follow
    the identical pattern (§7.6) if you need them.
"""
import argparse
import json
import os
from datetime import datetime, timedelta, timezone

import redis

REFERENCE_NOW = datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc)
HOT_CUTOFF = REFERENCE_NOW - timedelta(hours=24)
RING_CAP = 50

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def load_json(*parts):
    with open(os.path.join(BASE_DIR, *parts)) as f:
        return json.load(f)


def load_jsonl(*parts):
    with open(os.path.join(BASE_DIR, *parts)) as f:
        return [json.loads(line) for line in f if line.strip()]


def hour_bucket(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H")


def day_bucket(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d")


def bump_amount_stats(r, key, amount):
    """HINCRBY/HINCRBYFLOAT cnt/sum/sumsq, read-modify-write min/max.
    A real-time write uses a Lua script for atomicity (§7.9); a batch loader
    doesn't need that guarantee, so plain commands are fine here."""
    pipe = r.pipeline()
    pipe.hincrby(key, "cnt", 1)
    pipe.hincrbyfloat(key, "sum", amount)
    pipe.hincrbyfloat(key, "sumsq", amount * amount)
    pipe.execute()
    cur_min = r.hget(key, "min")
    if cur_min is None or amount < float(cur_min):
        r.hset(key, "min", amount)
    cur_max = r.hget(key, "max")
    if cur_max is None or amount > float(cur_max):
        r.hset(key, "max", amount)


def apply_transaction(r, txn):
    cid = txn["customer_id"]
    bene = txn["receiver_account"]
    country = txn["customer_portfolio_country"]
    tpp = txn["tpp_name_ud"]
    amount = float(txn["amount_base"])
    ts = datetime.fromisoformat(txn["timestamp"])

    hot = ts >= HOT_CUTOFF
    hb, db = hour_bucket(ts), day_bucket(ts)

    # --- ring buffer (positional/sequence metrics, §7.5) ---
    ring_key = f"c:{{{cid}}}:ring:payment"
    r.lpush(ring_key, json.dumps({"ts": txn["timestamp"], "amount": amount, "bene": bene}))
    r.ltrim(ring_key, 0, RING_CAP - 1)

    # --- customer-level amount stats (§7.3, §7.4) ---
    if hot:
        bump_amount_stats(r, f"c:{{{cid}}}:agg:amount:1h:{hb}", amount)
    else:
        bump_amount_stats(r, f"c:{{{cid}}}:agg:amount:1d:{db}", amount)

    # --- beneficiary-level amount stats + distinct-sender set (§7.6) ---
    if hot:
        bump_amount_stats(r, f"bene:{{{bene}}}:agg:amount:1h:{hb}", amount)
    else:
        bump_amount_stats(r, f"bene:{{{bene}}}:agg:amount:1d:{db}", amount)
    if hot:
        dkey = f"bene:{{{bene}}}:distinct_senders:last_24h"
        r.sadd(dkey, cid)
        r.expire(dkey, 26 * 3600)

    # --- customer -> beneficiary pair state (lightweight, §7.12) ---
    pkey = f"c:{{{cid}}}:pair:{bene}:state"
    r.hsetnx(pkey, "first_seen_ts", txn["timestamp"])
    r.hincrby(pkey, "cnt_90d", 1)
    r.hincrbyfloat(pkey, "sum_90d", amount)

    # --- geography / TPP aggregates (populated for completeness, §7.6) ---
    if hot:
        bump_amount_stats(r, f"geo:{{{country}}}:agg:amount:1h:{hb}", amount)
        bump_amount_stats(r, f"tpp:{{{tpp}}}:agg:amount:1h:{hb}", amount)
    else:
        bump_amount_stats(r, f"geo:{{{country}}}:agg:amount:1d:{db}", amount)
        bump_amount_stats(r, f"tpp:{{{tpp}}}:agg:amount:1d:{db}", amount)

    # device-sharing set (§5): distinct customers seen on a device
    r.sadd(f"device:{{{txn['device_fingerprint']}}}:customers", cid)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default="redis://localhost:6379/0")
    ap.add_argument("--flush", action="store_true", help="FLUSHDB before loading")
    args = ap.parse_args()

    r = redis.from_url(args.redis_url, decode_responses=True)
    r.ping()

    if args.flush:
        r.flushdb()

    # blacklists (§7.7 — plain SET here, RedisBloom in production)
    bl = load_json("reference", "blacklists.json")
    if bl["bl_accounts"]:
        r.sadd("bl:accounts", *bl["bl_accounts"])
    if bl["bl_devices"]:
        r.sadd("bl:devices", *bl["bl_devices"])

    # membership lists
    ml = load_json("reference", "membership_lists.json")
    if ml["vip_customers"]:
        r.sadd("list:vip_customers", *ml["vip_customers"])
    if ml["watchlist"]:
        r.sadd("list:watchlist", *ml["watchlist"])

    # config (§6.1, §6.2, §8.1) — HASH keyed by id, value = JSON blob
    for window in load_json("config", "windows.json"):
        r.hset("cfg:windows", window["window_id"], json.dumps(window))
    for metric in load_json("config", "metrics.json"):
        r.hset("cfg:metrics", metric["metric_id"], json.dumps(metric))
    for rule in load_json("config", "rules.json"):
        r.hset("cfg:rules", rule["rule_id"], json.dumps(rule))

    # reference / master data (§7.2) -> Redis (customer & beneficiary profiles, IP-geo)
    for c in load_json("reference", "customers.json"):
        r.hset(f"c:{{{c['customer_id']}}}:profile", mapping={
            "country": c["customer_portfolio_country"],
            "account_open_date": c["account_open_date"],
            "risk_segment": c["risk_segment"],
        })
    for b in load_json("reference", "beneficiaries.json"):
        r.hset(f"bene:{{{b['receiver_account']}}}:profile", "country", b["country"])
    ipgeo = load_json("reference", "ip_geo.json")
    for prefix, country in ipgeo["prefixes"].items():
        r.hset("geo:ip_prefixes", prefix, country)
    if ipgeo["proxy_ips"]:
        r.sadd("geo:proxy_ips", *ipgeo["proxy_ips"])

    # replay the historical backfill through the write path (§13.2)
    txns = load_jsonl("transactions", "historical_backfill.jsonl")
    for txn in txns:
        apply_transaction(r, txn)

    print(f"Loaded {len(txns)} backfill transactions, "
          f"{len(bl['bl_accounts'])} blacklisted accounts, "
          f"{len(bl['bl_devices'])} blacklisted devices, "
          f"{len(ml['vip_customers'])} VIP customers, "
          f"{len(ml['watchlist'])} watchlist customers, "
          f"and cfg:windows/cfg:metrics/cfg:rules into {args.redis_url}.")
    print("Spot-check: redis-cli HGETALL 'c:{cust_012}:agg:amount:1h:2026-08-10T11'")


if __name__ == "__main__":
    main()
