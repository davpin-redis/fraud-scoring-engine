#!/usr/bin/env python3
"""
Accurately size the feature store (RAM working set, §7) on a REAL Redis database
by generating a representative N-customer x 90-day history through the same
write-path key schema the engine uses (§7.3), then measuring per-customer memory
and extrapolating to 5M customers.

Reproduces the structures the write path builds (matches load_redis.py's
apply_transaction): per-customer ring buffer + 1h/1d amount aggregates + pair
state, per-beneficiary aggregates + 24h distinct-sender set, geo/TPP aggregates,
and the device->customers set.

Measurement uses the used_memory DELTA over the seeded sample, so it's accurate
on a shared/non-empty DB. Nothing is flushed unless --flush is passed.

Usage:
  python3 size_feature_store.py \
      --host <feature-host> --port <port> \
      --password '<pw>' [--user default] [--tls] \
      --customers 2000 --days 90 --txns-per-day 17 --available-gb 30
"""
import argparse
import os
import random
import time
from datetime import datetime, timedelta, timezone

import redis

TARGET_CUSTOMERS = 5_000_000
RING_CAP = 50
COUNTRIES = ["US", "GB", "DE", "SG", "BR"]
TPPS = ["tpp_Alpha", "tpp_Beta", "tpp_Gamma", "tpp_Delta", "tpp_Epsilon"]


def connect(a):
    kw = dict(host=a.host, port=a.port, password=a.password, decode_responses=True)
    if a.user:
        kw["username"] = a.user
    if a.tls:
        kw["ssl"] = True
        kw["ssl_cert_reqs"] = None
    r = redis.Redis(**kw)
    r.ping()
    return r


def bump(pipe, key, amt):
    pipe.hincrby(key, "cnt", 1)
    pipe.hincrbyfloat(key, "sum", amt)
    pipe.hincrbyfloat(key, "sumsq", amt * amt)
    # min/max would need read-modify-write; approximated with HSETNX-style skip for sizing


def seed(r, a):
    """Realistic per-customer behaviour: a small fixed payee set, full rate in the
    last 24h (populates the 1h tier + distinct-sender sets) and ~1/day before
    (populates the 90 daily buckets). Beneficiaries are a shared pool sized to the
    production bene:customer ratio (~0.2), so the per-customer delta includes a
    representative share of beneficiary-side memory."""
    now = datetime.now(timezone.utc)
    rnd = random.Random(42)
    bene_pool = max(50, int(a.customers * 0.2))

    def apply(pipe, cid, device, country, bene, amt, ts, hot):
        hb, db = ts.strftime("%Y-%m-%dT%H"), ts.strftime("%Y-%m-%d")
        iso = ts.isoformat()
        ring = f"c:{{{cid}}}:ring:payment"
        pipe.lpush(ring, f'{{"ts":"{iso}","amount":{amt},"bene":"{bene}"}}')
        pipe.ltrim(ring, 0, RING_CAP - 1)
        if hot:
            bump(pipe, f"c:{{{cid}}}:agg:amount:1h:{hb}", amt)
            bump(pipe, f"bene:{{{bene}}}:agg:amount:1h:{hb}", amt)
            dk = f"bene:{{{bene}}}:distinct_senders:last_24h"
            pipe.sadd(dk, cid); pipe.expire(dk, 26 * 3600)
            bump(pipe, f"geo:{{{country}}}:agg:amount:1h:{hb}", amt)
            bump(pipe, f"tpp:{{{country}}}:agg:amount:1h:{hb}", amt)
        else:
            bump(pipe, f"c:{{{cid}}}:agg:amount:1d:{db}", amt)
            bump(pipe, f"bene:{{{bene}}}:agg:amount:1d:{db}", amt)
            bump(pipe, f"geo:{{{country}}}:agg:amount:1d:{db}", amt)
            bump(pipe, f"tpp:{{{country}}}:agg:amount:1d:{db}", amt)
        pk = f"c:{{{cid}}}:pair:{bene}:state"
        pipe.hsetnx(pk, "first_seen_ts", iso)
        pipe.hincrby(pk, "cnt_90d", 1)
        pipe.hincrbyfloat(pk, "sum_90d", amt)
        pipe.sadd(f"device:{{{device}}}:customers", cid)

    pipe = r.pipeline(transaction=False)
    ops = 0
    for c in range(a.customers):
        cid = f"cust_{c:07d}"
        device = f"device_{c:07d}"
        country = COUNTRIES[c % len(COUNTRIES)]
        payees = [f"bene_{rnd.randrange(bene_pool):06d}" for _ in range(a.payees)]
        pipe.hset(f"c:{{{cid}}}:profile", mapping={
            "country": country, "account_open_date": "2023-01-01", "risk_segment": "standard"})
        # last 24h at full rate (hot tier)
        for _ in range(a.hot_txns):
            ts = now - timedelta(seconds=rnd.randrange(86400))
            apply(pipe, cid, device, country, rnd.choice(payees), round(rnd.uniform(20, 500), 2), ts, True)
            ops += 1
        # prior (days-1) days at ~1/day (warm/daily tier)
        for d in range(1, a.days):
            ts = now - timedelta(days=d, seconds=rnd.randrange(86400))
            apply(pipe, cid, device, country, rnd.choice(payees), round(rnd.uniform(20, 500), 2), ts, False)
            ops += 1
        if ops >= 2000:
            pipe.execute(); pipe = r.pipeline(transaction=False); ops = 0
        if c % 500 == 0 and c:
            print(f"  seeded {c:,}/{a.customers:,} customers")
    pipe.execute()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    ap.add_argument("--user", default=None)
    ap.add_argument("--tls", action="store_true")
    ap.add_argument("--customers", type=int, default=2000)
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--payees", type=int, default=12, help="distinct payees per customer")
    ap.add_argument("--hot-txns", type=int, default=17, help="txns in the last 24h (1h tier)")
    ap.add_argument("--available-gb", type=float, default=30.0)
    ap.add_argument("--flush", action="store_true")
    a = ap.parse_args()

    r = connect(a)
    if a.flush:
        print("FLUSHALL requested — clearing DB")
        r.flushall()

    used_before = int(r.info("memory")["used_memory"])
    t0 = time.time()
    print(f"seeding {a.customers:,} customers: {a.hot_txns} hot(24h) + {a.days-1} daily txns, {a.payees} payees each...")
    seed(r, a)
    dt = time.time() - t0
    used_after = int(r.info("memory")["used_memory"])

    per_cust = (used_after - used_before) / a.customers
    print("\n================ MEASURED (delta over sample) ================")
    print(f"seed time            : {dt:.0f}s")
    print(f"per-customer memory  : {per_cust/1024:.1f} KB")
    print(f"\n================ EXTRAPOLATION → {TARGET_CUSTOMERS/1e6:.0f}M customers ================")
    print(f"feature-store RAM    : {TARGET_CUSTOMERS*per_cust/1e12:.2f} TB")
    avail = a.available_gb * 1e9
    print(f"\n================ CAPACITY of this {a.available_gb:.0f} GB database ================")
    print(f"fits ~{avail/per_cust/1e3:,.0f}K customers (at this per-customer footprint)")
    print("\nNOTE: min/max fields are skipped in this sizer (read-modify-write); add ~5-10%.")


if __name__ == "__main__":
    main()
