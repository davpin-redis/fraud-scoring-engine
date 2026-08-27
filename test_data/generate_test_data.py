#!/usr/bin/env python3
"""
Generates a small, laptop-scale test dataset + config for the fraud scoring
system design (see Fraud_Scoring_System_Design_Doc.md, §6, §7, §13).

This is NOT a scaled-down copy of the 10M-customer production dataset — it's
sized to exercise every rule/metric path (§6.2, §6.3, §8.1) with a handful of
entities, deterministically (fixed seed + fixed reference timestamp), so the
expected outcome of every sample request is known ahead of time.

Output layout:
  reference/customers.json
  reference/beneficiaries.json
  reference/tpps.json
  reference/blacklists.json
  reference/membership_lists.json
  config/windows.json
  config/metrics.json
  config/rules.json
  transactions/historical_backfill.jsonl
  transactions/sample_test_requests.json
"""
import json
import os
import random
from datetime import datetime, timedelta, timezone

random.seed(42)

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
REFERENCE_NOW = datetime(2026, 8, 10, 12, 0, 0, tzinfo=timezone.utc)

COUNTRIES = ["US", "GB", "DE", "SG", "BR"]
COUNTRY_WEIGHTS = [0.40, 0.20, 0.15, 0.15, 0.10]

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

def gen_customers():
    customers = []
    devices = []

    def make_customer(cid, segment, account_age_days, country=None, device_id=None):
        country = country or random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]
        device_id = device_id or f"device_{cid.split('_')[1]}"
        opened = (REFERENCE_NOW - timedelta(days=account_age_days)).date().isoformat()
        octet = {"US": 1, "GB": 2, "DE": 3, "SG": 4, "BR": 5}.get(country, 9)
        num = int(cid.split("_")[1])
        ip = f"51.{octet}.0.{num}"  # deterministic (no RNG draw, keeps backfill stream stable)
        customers.append({
            "customer_id": cid,
            "customer_portfolio_country": country,
            "account_open_date": opened,
            "risk_segment": segment,
            "device_fingerprint": device_id,
            "ip_address": ip,
            "typical_amount_range_usd": [20, 200] if segment != "vip" else [20, 400],
        })
        devices.append(device_id)
        return country, device_id

    # 40 normal, established customers
    for i in range(1, 41):
        cid = f"cust_{i:03d}"
        make_customer(cid, "normal", random.randint(200, 1400))

    # 5 new accounts (<30 days old) — account age is itself a risk feature
    for i in range(41, 46):
        cid = f"cust_{i:03d}"
        make_customer(cid, "new", random.randint(3, 25))

    # 3 VIP customers
    for i in range(46, 49):
        cid = f"cust_{i:03d}"
        make_customer(cid, "vip", random.randint(1500, 3000))

    # 2 watchlist customers
    for i in range(49, 51):
        cid = f"cust_{i:03d}"
        make_customer(cid, "watchlist", random.randint(100, 900))

    # Deliberate device-sharing: cust_010, cust_027, cust_033 share device_shared_01
    for cid in ("cust_010", "cust_027", "cust_033"):
        for c in customers:
            if c["customer_id"] == cid:
                c["device_fingerprint"] = "device_shared_01"

    return customers


def gen_beneficiaries():
    benes = []
    for i in range(1, 21):
        bid = f"bene_{i:03d}"
        benes.append({
            "receiver_transaction_bank_account_number": bid,
            "country": random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0],
            "is_mule_pattern": bid in ("bene_018", "bene_019"),
        })
    return benes


def gen_tpps():
    names = ["tpp_Alpha", "tpp_Beta", "tpp_Gamma", "tpp_Delta", "tpp_Epsilon"]
    return [
        {"tpp_name_ud": n, "typical_country": random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0]}
        for n in names
    ]


def gen_blacklists():
    return {
        "bl_accounts": ["bene_020"],
        "bl_devices": ["device_099"],
    }


def gen_membership_lists():
    return {
        "vip_customers": ["cust_046", "cust_047", "cust_048"],
        "watchlist": ["cust_049", "cust_050"],
    }


def gen_network():
    # Stub IP-geo: first two octets map to a country; a small proxy/VPN list.
    return {
        "prefixes": {"51.1": "US", "51.2": "GB", "51.3": "DE", "51.4": "SG", "51.5": "BR"},
        "proxy_ips": ["51.9.13.7"],
    }


# ---------------------------------------------------------------------------
# Config: windows, metrics, rules (§6.1, §6.2, §8.1)
# ---------------------------------------------------------------------------

def gen_windows_config():
    return [
        {
            "window_id": "last_24h",
            "lookback_start_offset_sec": -86400,
            "lookback_end_offset_sec": 0,
            "bucket_resolution": "1h",
            "tier": "hot",
            "retention_sec": 93600,  # 26h
        },
        {
            "window_id": "aged_24h_90d",
            "lookback_start_offset_sec": -7776000,  # -90d
            "lookback_end_offset_sec": -86400,       # -24h
            "bucket_resolution": "1d",
            "tier": "warm",
            "retention_sec": 8208000,  # ~95d
        },
    ]


def gen_metrics_config():
    return [
        {
            "metric_id": "time_since_last_failed_logon_24h_hrs",
            "entity": "customer_id",
            "type": "time_since_last_event",
            "event_type": "failed_logon",
            "window_id": "last_24h",
        },
        {
            "metric_id": "customerid_stddev_sendertransactionamo_1d",
            "entity": "customer_id",
            "type": "streaming_aggregate",
            "agg": "stddev",
            "field": "amount_base",
            "window_id": "last_24h",
        },
        {
            "metric_id": "c_snd_3rd_lst_pymt_dt_1h_rt",
            "entity": "customer_id",
            "type": "positional",
            "field": "payment_ts",
            "position": 2,
            "window_id": "last_24h",
            "sub_window_sec": 3600,
        },
        {
            "metric_id": "customer_txn_count_1h",
            "entity": "customer_id",
            "type": "streaming_aggregate",
            "agg": "count",
            "field": "amount_base",
            "window_id": "last_24h",
            "sub_window_sec": 3600,
        },
        {
            "metric_id": "bene_distinct_senders_24h",
            "entity": "receiver_transaction_bank_account_number",
            "type": "streaming_aggregate",
            "agg": "distinct_count",
            "field": "customer_id",
            "window_id": "last_24h",
        },
        {
            "metric_id": "pair_txn_count_90d",
            "entity": "[customer_id, receiver_transaction_bank_account_number]",
            "type": "streaming_aggregate",
            "agg": "count",
            "field": "amount_base",
            "window_id": "aged_24h_90d",
        },
    ]


def gen_rules_config():
    return [
        {
            "rule_id": "R001_blacklisted_beneficiary",
            "type": "hard_block",
            "condition": "receiver_transaction_bank_account_number in bl_accounts",
        },
        {
            "rule_id": "R002_blacklisted_device",
            "type": "hard_block",
            "condition": "device_fingerprint in bl_devices",
        },
        {
            "rule_id": "R003_vip_low_amount_fast_track",
            "type": "hard_allow",
            "condition": "customer_id in vip_customers and amount_base < 500",
        },
        {
            "rule_id": "R004_velocity_burst",
            "type": "soft",
            "condition": "customer_txn_count_1h > 5",
            "weight": 0.4,
        },
        {
            "rule_id": "R005_mule_fan_in",
            "type": "soft",
            "condition": "bene_distinct_senders_24h > 10",
            "weight": 0.5,
        },
        {
            "rule_id": "R006_new_payee_high_amount",
            "type": "soft",
            "condition": "pair_txn_count_90d == 0 and amount_base > 1000",
            "weight": 0.35,
        },
        {
            "rule_id": "R007_cross_border_off_hours",
            "type": "soft",
            "condition": "customer_portfolio_country != beneficiary_country and local_hour in [0,1,2,3,4]",
            "weight": 0.3,
        },
        {
            "rule_id": "R008_watchlist_customer",
            "type": "soft",
            "condition": "customer_id in watchlist",
            "weight": 0.3,
        },
        {
            "rule_id": "R009_device_sharing",
            "type": "soft",
            "condition": "device_distinct_customers > 2",
            "weight": 0.3,
        },
        {
            "rule_id": "R010_ip_geo_anomaly",
            "type": "soft",
            "condition": "ip_country != customer_portfolio_country or ip_proxy",
            "weight": 0.3,
        },
        {
            "rule_id": "R011_new_account_high_amount",
            "type": "soft",
            "condition": "account_age_days < 30 and amount_base > 1000",
            "weight": 0.3,
        },
        # --- rules computed from the 90-day transaction hot window (§4.3, §8.1) ---
        # These read the transaction store via the Query Engine at scoring time
        # (customer_declines_90d / customer_distinct_bene_90d), not the feature store.
        {
            "rule_id": "R012_repeat_declines_90d",
            "type": "soft",
            "condition": "customer_declines_90d >= 3",
            "weight": 0.4,
        },
        {
            "rule_id": "R013_beneficiary_fanout_90d",
            "type": "soft",
            "condition": "customer_distinct_bene_90d > 15",
            "weight": 0.35,
        },
        {
            "rule_id": "R014_repeat_declines_24h",
            "type": "soft",
            "condition": "customer_declines_24h >= 3",
            "weight": 0.45,
        },
        {
            "rule_id": "R015_beneficiary_fanout_24h",
            "type": "soft",
            "condition": "customer_distinct_bene_24h > 8",
            "weight": 0.35,
        },
        {
            "rule_id": "R016_mule_fan_in_90d",
            "type": "soft",
            "condition": "bene_distinct_senders_90d > 30",
            "weight": 0.4,
        },
        {
            "rule_id": "R017_velocity_burst_5m",
            "type": "soft",
            "condition": "customer_txn_rate_5m > 5",
            "weight": 0.4,
        },
        {
            "rule_id": "R018_amount_anomaly_90d",
            "type": "soft",
            "condition": "amount_zscore_90d > 3",
            "weight": 0.4,
        },
        {
            "rule_id": "R019_velocity_spike_vs_baseline",
            "type": "soft",
            "condition": "velocity_ratio_1h > 8",
            "weight": 0.45,
        },
        {
            "rule_id": "R020_sustained_elevation",
            "type": "soft",
            "condition": "velocity_elevated_hours >= 4",
            "weight": 0.4,
        },
        {
            "rule_id": "R021_offhour_activity",
            "type": "soft",
            "condition": "hod_share_now < 0.01",
            "weight": 0.3,
        },
        {
            "rule_id": "R022_machine_cadence",
            "type": "soft",
            "condition": "interarrival_cv < 0.15",
            "weight": 0.4,
        },
        {
            "rule_id": "R023_amount_bustout",
            "type": "soft",
            "condition": "amount_trend > 0.15",
            "weight": 0.45,
        },
        {
            "rule_id": "R024_dormant_reactivation",
            "type": "soft",
            "condition": "dormancy_days > 60",
            "weight": 0.35,
        },
        {
            "rule_id": "R025_device_surge",
            "type": "soft",
            "condition": "device_surge > 5",
            "weight": 0.4,
        },
        {
            "rule_id": "R026_bene_inbound_surge",
            "type": "soft",
            "condition": "bene_surge > 5",
            "weight": 0.4,
        },
    ]


# ---------------------------------------------------------------------------
# Historical backfill (10 days, light rate) + injected scenarios
# ---------------------------------------------------------------------------

def new_txn(customer, bene_id, amount, ts, tpp, txn_id):
    return {
        "transaction_id": txn_id,
        "timestamp": ts.isoformat(),
        "customer_id": customer["customer_id"],
        "customer_portfolio_country": customer["customer_portfolio_country"],
        "receiver_transaction_bank_account_number": bene_id,
        "tpp_name_ud": tpp,
        "device_fingerprint": customer["device_fingerprint"],
        "ip_address": customer["ip_address"],
        "amount": round(amount, 2),
        "currency": "GBP",
        "amount_base": round(amount, 2),
    }


def gen_backfill_and_samples(customers, benes, tpps):
    by_id = {c["customer_id"]: c for c in customers}
    tpp_names = [t["tpp_name_ud"] for t in tpps]
    normal_benes = [b["receiver_transaction_bank_account_number"] for b in benes if not b["is_mule_pattern"]]

    txns = []
    seq = [0]

    def add(customer_id, bene_id, amount, ts, tpp=None):
        seq[0] += 1
        txns.append(new_txn(
            by_id[customer_id], bene_id, amount, ts,
            tpp or random.choice(tpp_names),
            f"bf_{seq[0]:06d}",
        ))

    # --- light background traffic: 10 days, ~1-3 txns/day for most customers ---
    for c in customers:
        cid = c["customer_id"]
        lo, hi = c["typical_amount_range_usd"]
        for day in range(10, 0, -1):
            if random.random() < 0.6:  # not every customer transacts every day
                for _ in range(random.randint(1, 2)):
                    ts = REFERENCE_NOW - timedelta(days=day, hours=random.randint(0, 23), minutes=random.randint(0, 59))
                    add(cid, random.choice(normal_benes), random.uniform(lo, hi), ts)

    # --- injected scenario 1: velocity burst — cust_012, 9 txns in 12 minutes, ~30 min ago ---
    burst_start = REFERENCE_NOW - timedelta(minutes=42)
    for i in range(9):
        ts = burst_start + timedelta(minutes=i * 1.4)
        add("cust_012", "bene_005", random.uniform(30, 60), ts)

    # --- injected scenario 2: mule fan-in — bene_018 paid by 14 distinct customers in last 24h ---
    for i, cid in enumerate([f"cust_{n:03d}" for n in range(1, 15)]):
        ts = REFERENCE_NOW - timedelta(hours=random.uniform(0.5, 20))
        add(cid, "bene_018", random.uniform(80, 150), ts)

    # --- established pair history for cust_030 -> bene_017 baseline (used by scenario 4 as "not new payee") ---
    for day in (60, 45, 30):
        ts = REFERENCE_NOW - timedelta(days=day)
        add("cust_030", "bene_017", random.uniform(100, 200), ts)

    write_jsonl(os.path.join(OUT_DIR, "transactions", "historical_backfill.jsonl"), txns)

    # --- sample_test_requests.json: crafted requests to POST *after* the backfill is loaded ---
    samples = [
        {
            "label": "baseline_clean_txn",
            "expected_decision": "approve",
            "expected_trigger": None,
            "note": "Ordinary transaction, no rule should fire.",
            "request": new_txn(by_id["cust_005"], "bene_003", 65.00, REFERENCE_NOW, "tpp_Alpha", "req_001"),
        },
        {
            "label": "vip_fast_track",
            "expected_decision": "approve",
            "expected_trigger": "R003_vip_low_amount_fast_track",
            "note": "VIP customer, small amount — hard-allow rule.",
            "request": new_txn(by_id["cust_046"], "bene_002", 40.00, REFERENCE_NOW, "tpp_Beta", "req_002"),
        },
        {
            "label": "blacklisted_beneficiary",
            "expected_decision": "decline",
            "expected_trigger": "R001_blacklisted_beneficiary",
            "note": "Beneficiary bene_020 is on the blacklist — hard block.",
            "request": new_txn(by_id["cust_038"], "bene_020", 250.00, REFERENCE_NOW, "tpp_Gamma", "req_003"),
        },
        {
            "label": "blacklisted_device",
            "expected_decision": "decline",
            "expected_trigger": "R002_blacklisted_device",
            "note": "device_099 is on the blacklist — hard block.",
            "request": {**new_txn(by_id["cust_035"], "bene_004", 90.00, REFERENCE_NOW, "tpp_Delta", "req_004"),
                        "device_fingerprint": "device_099"},
        },
        {
            "label": "velocity_burst_followup",
            "expected_decision": "review",
            "expected_trigger": "R004_velocity_burst",
            "note": "cust_012 already made 9 txns in the last hour (see backfill) — one more should trip velocity.",
            "request": new_txn(by_id["cust_012"], "bene_005", 45.00, REFERENCE_NOW, "tpp_Alpha", "req_005"),
        },
        {
            "label": "mule_fan_in_followup",
            "expected_decision": "review",
            "expected_trigger": "R005_mule_fan_in",
            "note": "bene_018 already received from 14 distinct customers in 24h (see backfill).",
            "request": new_txn(by_id["cust_020"], "bene_018", 95.00, REFERENCE_NOW, "tpp_Beta", "req_006"),
        },
        {
            "label": "new_payee_high_amount",
            "expected_decision": "review",
            "expected_trigger": "R006_new_payee_high_amount",
            "note": "cust_020 has no prior history paying bene_015, and amount is large.",
            "request": new_txn(by_id["cust_020"], "bene_015", 4800.00, REFERENCE_NOW, "tpp_Gamma", "req_007"),
        },
        {
            "label": "established_payee_high_amount",
            "expected_decision": "approve",
            "expected_trigger": None,
            "note": "cust_030 has 90 days of history paying bene_017 (see backfill) — large amount alone shouldn't trip R006.",
            "request": new_txn(by_id["cust_030"], "bene_017", 4800.00, REFERENCE_NOW, "tpp_Delta", "req_008"),
        },
        {
            "label": "cross_border_off_hours",
            "expected_decision": "review",
            "expected_trigger": "R007_cross_border_off_hours",
            "note": "cust_030 is US, bene_017 assumed non-US; timestamp set to 03:14 local/UTC.",
            "request": new_txn(by_id["cust_030"], "bene_017",
                                2200.00,
                                REFERENCE_NOW.replace(hour=3, minute=14),
                                "tpp_Epsilon", "req_009"),
        },
        {
            "label": "watchlist_customer",
            "expected_decision": "review",
            "expected_trigger": "R008_watchlist_customer",
            "note": "cust_049 is on the watchlist membership list.",
            "request": new_txn(by_id["cust_049"], "bene_006", 150.00, REFERENCE_NOW, "tpp_Alpha", "req_010"),
        },
        {
            "label": "baseline_clean_txn_2",
            "expected_decision": "approve",
            "expected_trigger": None,
            "note": "Second ordinary control-group transaction.",
            "request": new_txn(by_id["cust_022"], "bene_009", 120.00, REFERENCE_NOW, "tpp_Beta", "req_011"),
        },
    ]
    write_json(os.path.join(OUT_DIR, "transactions", "sample_test_requests.json"), samples)

    return txns, samples


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------

def write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


def write_jsonl(path, records):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r, default=str) + "\n")


def main():
    customers = gen_customers()
    benes = gen_beneficiaries()
    tpps = gen_tpps()

    write_json(os.path.join(OUT_DIR, "reference", "customers.json"), customers)
    write_json(os.path.join(OUT_DIR, "reference", "beneficiaries.json"), benes)
    write_json(os.path.join(OUT_DIR, "reference", "tpps.json"), tpps)
    write_json(os.path.join(OUT_DIR, "reference", "blacklists.json"), gen_blacklists())
    write_json(os.path.join(OUT_DIR, "reference", "membership_lists.json"), gen_membership_lists())
    write_json(os.path.join(OUT_DIR, "reference", "ip_geo.json"), gen_network())

    write_json(os.path.join(OUT_DIR, "config", "windows.json"), gen_windows_config())
    write_json(os.path.join(OUT_DIR, "config", "metrics.json"), gen_metrics_config())
    write_json(os.path.join(OUT_DIR, "config", "rules.json"), gen_rules_config())

    txns, samples = gen_backfill_and_samples(customers, benes, tpps)

    print(f"customers: {len(customers)}")
    print(f"beneficiaries: {len(benes)}")
    print(f"tpps: {len(tpps)}")
    print(f"backfill transactions: {len(txns)}")
    print(f"sample test requests: {len(samples)}")
    print(f"output dir: {OUT_DIR}")


if __name__ == "__main__":
    main()
