#!/usr/bin/env python3
"""
Accurately size the transaction store (hot window, §4.3) on a REAL Redis
Enterprise / Flex v2 database by seeding a representative sample and measuring
actual memory + Query-Engine index usage, then extrapolating to production.

It reproduces the engine's exact on-Redis format (verified against the running
engine): key ``t:<id>``, RedisJSON doc with short field names, and a 4-field
index ``txnIdx`` over ``$.ci/$.ra/$.de/$.ts`` (the @Id is deliberately NOT
indexed — see TransactionRecord). So per-doc numbers match what the engine
produces in production.

Measurement method: record ``used_memory`` before and after seeding N docs and
use the DELTA, so it's accurate even on a shared/non-empty database.

Usage:
  pip install redis
  python3 size_transaction_store.py \
      --host <transaction-host> --port <port> \
      --password '<pw>' [--user default] [--tls] \
      --docs 5000000 --customers 5000 --benes 200000 \
      --available-gb 50            # the DB's RAM limit, for capacity planning

Nothing is flushed unless --flush is passed explicitly.
"""
import argparse
import os
import random
import time

import redis

INDEX = "txnIdx"
PREFIX = "t:"
TARGET_TPS = 1000
TARGET_WINDOW_DAYS = 90
TARGET_DOCS = TARGET_TPS * 86_400 * TARGET_WINDOW_DAYS  # 7.776e9
DECISIONS = ["approve", "review", "decline"]


def connect(a):
    kw = dict(host=a.host, port=a.port, password=a.password, decode_responses=True)
    if a.user:
        kw["username"] = a.user
    if a.tls:
        kw["ssl"] = True
        kw["ssl_cert_reqs"] = None  # demo clusters often use self-signed certs
    r = redis.Redis(**kw)
    r.ping()
    return r


def ftinfo(r, idx):
    arr = r.execute_command("FT.INFO", idx)
    return {arr[i]: arr[i + 1] for i in range(0, len(arr) - 1, 2)}


def ensure_index(r):
    # Flex v2 supports ONLY `ON HASH` indexes (RedisJSON cannot be indexed on Flex),
    # so the transaction store must store records as Hashes.
    try:
        ftinfo(r, INDEX)
        return "existing"
    except redis.ResponseError:
        # Flex indexes support TAG only (no NUMERIC). The 90-day window is enforced by
        # the TTL (retention == window), so a TAG-only query counts within the window
        # without a timestamp range filter. `ts` is still stored (unindexed).
        # SKIPINITIALSCAN is mandatory on Flex — the index does NOT back-fill existing
        # keys, so it must be created before the data is written.
        r.execute_command(
            "FT.CREATE", INDEX, "ON", "HASH", "PREFIX", "1", PREFIX, "SKIPINITIALSCAN", "SCHEMA",
            "ci", "AS", "customerId", "TAG",
            "ra", "AS", "receiverAccount", "TAG",
            "de", "AS", "decision", "TAG",
        )
        return "created"


def seed(r, a):
    base = int(time.time() * 1000)
    window_ms = TARGET_WINDOW_DAYS * 86_400_000
    ttl = a.ttl_days * 86_400
    rnd = random.Random(42)
    pipe = r.pipeline(transaction=False)
    n = 0
    for i in range(a.docs):
        cid = f"cust_{i % a.customers:07d}"
        bene = f"bene_{i % a.benes:06d}"
        ts = base - rnd.randrange(window_ms)
        amt = round(rnd.uniform(20, 920), 2)
        dec = DECISIONS[i % 3]
        key = f"{PREFIX}{i:x}"
        # Hash record with the same short field names (Flex requires HASH, not JSON)
        pipe.hset(key, mapping={
            "ti": f"{i:x}", "ci": cid, "ra": bene, "de": dec, "ts": ts,
            "mv": "seed-v0", "pc": "GB", "am": amt, "cu": "GBP", "fs": 0.45, "rf": "[]"})
        pipe.expire(key, ttl)
        if i % 2000 == 1999:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
            n = i + 1
            if n % 500_000 == 0:
                print(f"  seeded {n:,}/{a.docs:,}")
    pipe.execute()


def mem(r):
    info = r.info("memory")
    return int(info["used_memory"]), int(info.get("maxmemory", 0) or 0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    ap.add_argument("--user", default=None)
    ap.add_argument("--tls", action="store_true")
    ap.add_argument("--docs", type=int, default=5_000_000)
    ap.add_argument("--customers", type=int, default=5_000)
    ap.add_argument("--benes", type=int, default=200_000)
    ap.add_argument("--ttl-days", type=int, default=100)
    ap.add_argument("--available-gb", type=float, default=50.0,
                    help="the DB RAM limit, for capacity planning")
    ap.add_argument("--flush", action="store_true", help="FLUSHALL first (DANGER)")
    a = ap.parse_args()

    r = connect(a)
    if a.flush:
        print("FLUSHALL requested — clearing DB")
        r.flushall()

    used_before, maxmem = mem(r)
    idx_state = ensure_index(r)
    print(f"index {INDEX}: {idx_state}; used_memory before = {used_before/1e6:.1f} MB; "
          f"maxmemory = {maxmem/1e9:.1f} GB")

    t0 = time.time()
    print(f"seeding {a.docs:,} docs ({a.customers:,} customers, {a.benes:,} benes)...")
    seed(r, a)
    dt = time.time() - t0

    used_after, _ = mem(r)
    fi = ftinfo(r, INDEX)
    index_mb = float(fi["total_index_memory_sz_mb"])
    n = a.docs
    per_doc_total = (used_after - used_before) / n           # RAM incl. value+key+expire+index
    per_doc_index = index_mb * 1e6 / n
    sample = next(iter(r.scan_iter(match=f"{PREFIX}*", count=20)))
    val = r.memory_usage(sample)                             # hash value+key bytes (value tiers to flash)
    per_doc_ram = per_doc_total - val                        # index + key + expire (RAM-mandatory on Flex)

    print("\n================ MEASURED (delta over the sample) ================")
    print(f"seed rate                : {n/dt:,.0f} docs/s ({dt:.0f}s)")
    print(f"per-doc total RAM        : {per_doc_total:6.0f} B  (all in RAM here)")
    print(f"  RediSearch index/doc   : {per_doc_index:6.0f} B  (RAM-mandatory, never tiers)")
    print(f"  RedisJSON value/doc    : {val:6.0f} B  (tiers to flash on Flex)")
    print(f"  key+expire overhead    : {per_doc_total-per_doc_index-val:6.0f} B  (RAM-mandatory)")
    print(f"index fields             : inverted={float(fi['inverted_sz_mb']):.1f}MB "
          f"doc_table={float(fi['doc_table_size_mb']):.1f}MB "
          f"key_table={float(fi['key_table_size_mb']):.1f}MB "
          f"tag_overhead={float(fi['tag_overhead_sz_mb']):.2f}MB")

    ram_mandatory = per_doc_ram                              # index + key + expire
    flash_per_doc = val
    print("\n================ EXTRAPOLATION → production ({:.1f}B docs, 90d@{}TPS) ================".format(
        TARGET_DOCS / 1e9, TARGET_TPS))
    print(f"RAM  (Flex, value on flash): {TARGET_DOCS*ram_mandatory/1e12:5.2f} TB")
    print(f"Flash (values)             : {TARGET_DOCS*flash_per_doc/1e12:5.2f} TB (RAM tree; serialized on flash is smaller)")
    print(f"pure-RAM (no tiering)      : {TARGET_DOCS*per_doc_total/1e12:5.2f} TB")
    ratio = ram_mandatory / (ram_mandatory + flash_per_doc)
    print(f"natural RAM:flash split    : {ram_mandatory:.0f}:{flash_per_doc:.0f}  (~{ratio*100:.0f}% RAM)")

    avail = a.available_gb * 1e9
    print("\n================ CAPACITY of this {:.0f} GB database ================".format(a.available_gb))
    fit_docs_ram = avail / ram_mandatory
    print(f"if {a.available_gb:.0f} GB is the RAM limit: fits ~{fit_docs_ram/1e6:,.0f}M docs "
          f"(= ~{fit_docs_ram/(TARGET_TPS*86400):.1f} days of 90-day window at {TARGET_TPS} TPS)")
    fit_docs_all = avail / per_doc_total
    print(f"if {a.available_gb:.0f} GB is total (RAM-only DB): fits ~{fit_docs_all/1e6:,.0f}M docs")
    print("\nNOTE: for the authoritative Flex RAM-vs-flash split + shard count, read the")
    print("Redis Enterprise REST API: GET https://<cluster>:9443/v1/bdbs/<uid>/stats")


if __name__ == "__main__":
    main()
