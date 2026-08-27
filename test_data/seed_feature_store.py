#!/usr/bin/env python3
"""
Parallel bulk seeder for the feature store — a multiprocessing version of
size_feature_store.py for loading millions of customers fast.

Writes the SAME write-path key schema the engine reads (§7.3), identical to
size_feature_store.py's apply(): per-customer profile + ring buffer + 1h/1d
amount aggregates + pair state, per-beneficiary aggregates + 24h distinct-sender
set, geo/TPP aggregates, and the device->customers set. The only differences vs
the sizer are (a) it forks N worker processes over disjoint customer ranges to
bypass the GIL, and (b) the customer id width is configurable so it can match the
Gatling feeder / TransactionSeeder (cust_%03d).

The feature store is plain RAM (no index), so ingest is client-parallelism-bound:
more procs (and more seeder VMs via --customer-start) scale ~linearly until the
shards saturate.

Usage (single VM):
  python3 seed_feature_store.py \
      --host <feature-host> --port <port> [--password <pw>] [--user default] [--tls] \
      --customers 5000000 --procs 12 --days 90 --hot-txns 17 --payees 12

Usage (shard across VMs — give each a disjoint slice of the SAME total):
  # VM 1:  --customers 2500000 --customer-start 0
  # VM 2:  --customers 2500000 --customer-start 2500000
  (keep --total-customers 5000000 on both so the bene pool + extrapolation match)
"""
import argparse
import os
import random
import time
import multiprocessing as mp
from datetime import datetime, timedelta, timezone

import redis

RING_CAP = 50
COUNTRIES = ["US", "GB", "DE", "SG", "BR"]
TPPS = ["tpp_Alpha", "tpp_Beta", "tpp_Gamma", "tpp_Delta", "tpp_Epsilon"]
TARGET_CUSTOMERS = 5_000_000


def _mk_redis(host, port, password, user, tls):
    kw = dict(host=host, port=port, password=password, decode_responses=True)
    if user:
        kw["username"] = user
    if tls:
        kw["ssl"] = True
        kw["ssl_cert_reqs"] = None
    return redis.Redis(**kw)


def bump(pipe, key, amt):
    pipe.hincrby(key, "cnt", 1)
    pipe.hincrbyfloat(key, "sum", amt)
    pipe.hincrbyfloat(key, "sumsq", amt * amt)


def _seed_slice(cfg, lo, hi):
    """Seed customers [lo, hi) — identical structures to size_feature_store.py."""
    r = _mk_redis(cfg["host"], cfg["port"], cfg["password"], cfg["user"], cfg["tls"])
    now = datetime.fromtimestamp(cfg["now_ts"], timezone.utc)
    rnd = random.Random(1000 + lo)          # deterministic per slice
    bene_pool = max(50, int(cfg["total_customers"] * 0.2))
    wid = cfg["id_width"]

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
    done = 0
    for c in range(lo, hi):
        cid = f"cust_{c:0{wid}d}"
        device = f"device_{c:0{wid}d}"
        country = COUNTRIES[c % len(COUNTRIES)]
        payees = [f"bene_{rnd.randrange(bene_pool):06d}" for _ in range(cfg["payees"])]
        pipe.hset(f"c:{{{cid}}}:profile", mapping={
            "country": country, "account_open_date": "2023-01-01", "risk_segment": "standard"})
        for _ in range(cfg["hot_txns"]):
            ts = now - timedelta(seconds=rnd.randrange(86400))
            apply(pipe, cid, device, country, rnd.choice(payees), round(rnd.uniform(20, 500), 2), ts, True)
            ops += 1
        for d in range(1, cfg["days"]):
            ts = now - timedelta(days=d, seconds=rnd.randrange(86400))
            apply(pipe, cid, device, country, rnd.choice(payees), round(rnd.uniform(20, 500), 2), ts, False)
            ops += 1
        if ops >= 2000:
            pipe.execute(); pipe = r.pipeline(transaction=False); ops = 0
        done += 1
        if done % 5000 == 0:
            print(f"  [slice {lo:,}] {done:,}/{hi-lo:,} customers", flush=True)
    pipe.execute()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    ap.add_argument("--user", default=None)
    ap.add_argument("--tls", action="store_true")
    ap.add_argument("--customers", type=int, default=100_000, help="customers to seed in THIS invocation")
    ap.add_argument("--customer-start", type=int, default=0, help="first customer index (for multi-VM sharding)")
    ap.add_argument("--total-customers", type=int, default=None,
                    help="grand total across all VMs (sets bene-pool + extrapolation); defaults to --customers")
    ap.add_argument("--procs", type=int, default=max(1, (os.cpu_count() or 4) - 1))
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--payees", type=int, default=12, help="distinct payees per customer")
    ap.add_argument("--hot-txns", type=int, default=17, help="txns in the last 24h (1h tier)")
    ap.add_argument("--id-width", type=int, default=3,
                    help="zero-pad width of the numeric id; 3 => cust_%%03d, matching the Gatling feeder")
    ap.add_argument("--flush", action="store_true")
    ap.add_argument("--measure", action="store_true",
                    help="measure used_memory delta over this run (single invocation / idle DB only)")
    ap.add_argument("--available-gb", type=float, default=150.0)
    a = ap.parse_args()

    r = _mk_redis(a.host, a.port, a.password, a.user, a.tls); r.ping()
    if a.flush:
        print("FLUSHALL requested — clearing DB")
        r.flushall()

    total = a.total_customers or a.customers
    cfg = dict(host=a.host, port=a.port, password=a.password, user=a.user, tls=a.tls,
               days=a.days, payees=a.payees, hot_txns=a.hot_txns, id_width=a.id_width,
               total_customers=total, now_ts=time.time())

    # contiguous disjoint slices for this invocation's [start, start+customers)
    start, n, procs = a.customer_start, a.customers, a.procs
    chunk = -(-n // procs)
    ranges = [(start + p * chunk, min(start + n, start + (p + 1) * chunk)) for p in range(procs)]
    ranges = [(lo, hi) for lo, hi in ranges if lo < hi]

    used_before = int(r.info("memory")["used_memory"]) if a.measure else None
    t0 = time.time()
    print(f"seeding {n:,} customers [{start:,}..{start+n:,}) across {len(ranges)} procs; "
          f"{a.days}d ({a.hot_txns} hot + {a.days-1} daily), {a.payees} payees, id=cust_%0{a.id_width}d")
    workers = [mp.Process(target=_seed_slice, args=(cfg, lo, hi)) for lo, hi in ranges]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    dt = time.time() - t0
    print(f"\ndone: {n:,} customers in {dt:.0f}s ({n/max(dt,1):,.0f} cust/s)")

    if a.measure:
        used_after = int(r.info("memory")["used_memory"])
        per = (used_after - used_before) / n
        print(f"per-customer memory : {per/1024:.1f} KB")
        print(f"extrapolation → {TARGET_CUSTOMERS/1e6:.0f}M : {TARGET_CUSTOMERS*per/1e12:.2f} TB RAM")
        print(f"fits ~{a.available_gb*1e9/per/1e3:,.0f}K customers in {a.available_gb:.0f} GB")
        print("NOTE: min/max fields skipped (as in the sizer); add ~5-10%.")


if __name__ == "__main__":
    main()
