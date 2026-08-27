"""Dry-run verification of load_redis.py against a fake in-memory Redis,
since this sandbox has no network access to install redis-server/redis-py.
Not part of the deliverable — just checks the loader's logic is sound
before handing it to the user to run against a real local Redis."""
import sys
import types
import json


class FakePipeline:
    def __init__(self, client):
        self.client = client
        self.ops = []

    def hincrby(self, key, field, amount):
        self.ops.append(("hincrby", key, field, amount))
        return self

    def hincrbyfloat(self, key, field, amount):
        self.ops.append(("hincrbyfloat", key, field, amount))
        return self

    def execute(self):
        for op in self.ops:
            kind, key, field, amount = op
            if kind == "hincrby":
                self.client.hincrby(key, field, amount)
            else:
                self.client.hincrbyfloat(key, field, amount)
        self.ops = []


class FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.sets = {}
        self.lists = {}
        self.ttls = {}

    def ping(self):
        return True

    def flushdb(self):
        self.hashes.clear(); self.sets.clear(); self.lists.clear()

    def pipeline(self):
        return FakePipeline(self)

    def hincrby(self, key, field, amount):
        h = self.hashes.setdefault(key, {})
        h[field] = str(int(float(h.get(field, 0))) + amount)

    def hincrbyfloat(self, key, field, amount):
        h = self.hashes.setdefault(key, {})
        h[field] = str(float(h.get(field, 0)) + amount)

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = str(value)

    def hsetnx(self, key, field, value):
        h = self.hashes.setdefault(key, {})
        if field not in h:
            h[field] = str(value)

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def sadd(self, key, *members):
        self.sets.setdefault(key, set()).update(members)

    def scard(self, key):
        return len(self.sets.get(key, set()))

    def expire(self, key, ttl):
        self.ttls[key] = ttl

    def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)

    def ltrim(self, key, start, end):
        self.lists[key] = self.lists.get(key, [])[start:end + 1]

    def llen(self, key):
        return len(self.lists.get(key, []))


fake_module = types.ModuleType("redis")
_singleton = FakeRedis()
fake_module.from_url = lambda *a, **kw: _singleton
sys.modules["redis"] = fake_module

sys.argv = ["load_redis.py", "--flush"]
import load_redis  # noqa: E402
load_redis.main()

r = _singleton
print("\n--- spot checks ---")
print("bl:accounts:", r.sets.get("bl:accounts"))
print("bl:devices:", r.sets.get("bl:devices"))
print("list:vip_customers:", r.sets.get("list:vip_customers"))
print("cfg:rules count:", len(r.hashes.get("cfg:rules", {})))
print("cust_012 ring len (velocity burst customer):", r.llen("c:{cust_012}:ring:payment"))
print("bene_018 distinct senders (mule fan-in):", r.scard("bene:{bene_018}:distinct_senders:last_24h"))
print("cust_030 -> bene_017 pair state:", r.hgetall("c:{cust_030}:pair:bene_017:state"))
# find cust_012's hourly bucket around the burst
for k in sorted(r.hashes.keys()):
    if k.startswith("c:{cust_012}:agg:amount:1h:"):
        print(k, r.hashes[k])
