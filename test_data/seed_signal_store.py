#!/usr/bin/env python3
"""
Parallel bulk seeder for the SIGNAL store — pre-warms the 90-day approximate
hot-window signals so the load test exercises R012/R013/R005/amount from the first
request instead of a cold store. Writes the SAME key layout the engine's SignalKeys
produces (design doc §8.1, revised — Flex removed):

  sig:c:{cid}:benes:<YYYYMM>      HyperLogLog of distinct beneficiaries (R013 fan-out)
  sig:b:{bene}:senders:<YYYYMM>   HyperLogLog of distinct senders     (R005 fan-in)
  sig:c:{cid}:declines:<YYYYMM>   declines counter                    (R012)
  sig:c:{cid}:amt:<YYYYMM>        count/sum/sumsq for the amount z-score (R018)

All core Redis (PFADD / INCR / HINCRBY) — no modules. Seeds the current rotating
monthly bucket (the engine reads the last SignalKeys.WINDOW_MONTHS buckets, so the
current one is enough to warm the window). Forks N processes over disjoint customer
ranges to bypass the GIL.

IMPORTANT: point --host/--port at the SIGNAL store (fraud.signal-store.uri), and keep
--id-width 3 so ids match the Gatling feeder / feature seeder (cust_%03d).

Usage:
  python3 seed_signal_store.py --host <signal-host> --port <port> [--password <pw>] \
      --customers 5000 --flagged 20 --procs 12
"""
import argparse
import os
import random
import time
import multiprocessing as mp
from datetime import datetime, timezone

import redis

TARGET_CUSTOMERS = 5_000_000
WINDOW_MONTHS = 4                       # matches SignalKeys.WINDOW_MONTHS
MONTH_TTL_SEC = (WINDOW_MONTHS + 1) * 31 * 24 * 3600


def _mk_redis(host, port, password, user, tls):
    kw = dict(host=host, port=port, password=password, decode_responses=True)
    if user:
        kw["username"] = user
    if tls:
        kw["ssl"] = True
        kw["ssl_cert_reqs"] = None
    return redis.Redis(**kw)


def _seed_slice(cfg, lo, hi):
    r = _mk_redis(cfg["host"], cfg["port"], cfg["password"], cfg["user"], cfg["tls"])
    month = cfg["month"]
    wid = cfg["id_width"]
    flagged = cfg["flagged"]
    rnd = random.Random(2000 + lo)
    bene_pool = max(50, int(cfg["total_customers"] * 0.2))

    pipe = r.pipeline(transaction=False)
    ops = 0
    done = 0
    for c in range(lo, hi):
        cid = "cust_%0*d" % (wid, c)
        high = 1 <= c <= flagged                       # high-risk seeded customers (trip R012/R013)
        n_benes = 18 if high else rnd.randint(3, 8)
        benes = ["bene_%03d" % rnd.randrange(bene_pool) for _ in range(n_benes)]

        bk = "sig:c:{%s}:benes:%s" % (cid, month)      # R013 fan-out HLL
        for b in benes:
            pipe.pfadd(bk, b)
            sk = "sig:b:{%s}:senders:%s" % (b, month)  # R005 fan-in HLL
            pipe.pfadd(sk, cid)
            pipe.expire(sk, MONTH_TTL_SEC)
        pipe.expire(bk, MONTH_TTL_SEC)

        if high:                                       # R012 repeat declines
            dk = "sig:c:{%s}:declines:%s" % (cid, month)
            for _ in range(4):
                pipe.incr(dk)
            pipe.expire(dk, MONTH_TTL_SEC)

        # amount distribution for the z-score (R018): ~90d of history around a mean
        n = 150
        mean = 200.0
        var = 80.0 * 80.0
        amk = "sig:c:{%s}:amt:%s" % (cid, month)
        pipe.hincrby(amk, "cnt", n)
        pipe.hincrbyfloat(amk, "sum", n * mean)
        pipe.hincrbyfloat(amk, "sumsq", n * (mean * mean + var))
        pipe.expire(amk, MONTH_TTL_SEC)

        ops += n_benes * 2 + 4
        if ops >= 2000:
            pipe.execute(); pipe = r.pipeline(transaction=False); ops = 0
        done += 1
        if done % 5000 == 0:
            print("  [slice %d] %d/%d customers" % (lo, done, hi - lo), flush=True)
    pipe.execute()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    ap.add_argument("--user", default=None)
    ap.add_argument("--tls", action="store_true")
    ap.add_argument("--customers", type=int, default=100_000, help="customers to seed in THIS invocation")
    ap.add_argument("--customer-start", type=int, default=0, help="first customer index (multi-VM sharding)")
    ap.add_argument("--total-customers", type=int, default=None, help="grand total across all VMs (sets bene pool)")
    ap.add_argument("--flagged", type=int, default=20, help="# high-risk customers (cust_1..flagged) that trip R012/R013")
    ap.add_argument("--procs", type=int, default=max(1, (os.cpu_count() or 4) - 1))
    ap.add_argument("--id-width", type=int, default=3, help="zero-pad width; 3 => cust_%%03d, matches the Gatling feeder")
    a = ap.parse_args()

    r = _mk_redis(a.host, a.port, a.password, a.user, a.tls); r.ping()
    total = a.total_customers or a.customers
    cfg = dict(host=a.host, port=a.port, password=a.password, user=a.user, tls=a.tls,
               id_width=a.id_width, flagged=a.flagged, total_customers=total,
               month=datetime.now(timezone.utc).strftime("%Y%m"))

    start, n, procs = a.customer_start, a.customers, a.procs
    chunk = -(-n // procs)
    ranges = [(start + p * chunk, min(start + n, start + (p + 1) * chunk)) for p in range(procs)]
    ranges = [(lo, hi) for lo, hi in ranges if lo < hi]

    t0 = time.time()
    print("seeding signals for %d customers [%d..%d) across %d procs, month=%s, id=cust_%%0%dd, flagged=%d"
          % (n, start, start + n, len(ranges), cfg["month"], a.id_width, a.flagged))
    workers = [mp.Process(target=_seed_slice, args=(cfg, lo, hi)) for lo, hi in ranges]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    print("done: %d customers in %.0fs (%.0f cust/s)" % (n, time.time() - t0, n / max(time.time() - t0, 1)))


if __name__ == "__main__":
    main()
