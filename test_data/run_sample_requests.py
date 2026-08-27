#!/usr/bin/env python3
"""
POSTs each request in test_data/transactions/sample_test_requests.json to a
running Scoring Engine and checks the returned decision against the expected
one (§13.3's "correctness under load" idea, applied as a plain functional
check rather than a load test). Stdlib only — no extra pip installs needed.

Usage:
  python3 run_sample_requests.py [--base-url http://localhost:8080] [--path /v1/transactions/score]

Run load_redis.py first so the backfill (velocity burst, mule fan-in, etc.)
is actually in Redis for these requests to trigger against.

Expects the endpoint to return JSON containing at least a "decision" field
(approve/review/decline, per §4.2 of the design doc). Adjust `extract_decision`
below if your endpoint's response shape differs.
"""
import argparse
import json
import os
import sys
import urllib.request
import urllib.error

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def extract_decision(response_json):
    # Adjust this if your Scoring Engine's response shape differs from §4.2.
    return response_json.get("decision")


def post(url, payload, timeout):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://localhost:8080")
    ap.add_argument("--path", default="/v1/transactions/score")
    ap.add_argument("--timeout", type=float, default=5.0)
    args = ap.parse_args()

    with open(os.path.join(BASE_DIR, "transactions", "sample_test_requests.json")) as f:
        samples = json.load(f)

    url = args.base_url.rstrip("/") + args.path
    results = []
    for s in samples:
        try:
            resp = post(url, s["request"], args.timeout)
            actual = extract_decision(resp)
            ok = actual == s["expected_decision"]
            results.append((s["label"], s["expected_decision"], actual, ok, None))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            results.append((s["label"], s["expected_decision"], None, False, str(e)))

    print(f"{'label':<28} {'expected':<10} {'actual':<10} {'result'}")
    print("-" * 60)
    n_pass = 0
    for label, expected, actual, ok, err in results:
        status = "PASS" if ok else "FAIL"
        n_pass += ok
        print(f"{label:<28} {expected:<10} {str(actual):<10} {status}" + (f"  ({err})" if err else ""))

    print("-" * 60)
    print(f"{n_pass}/{len(results)} passed")
    sys.exit(0 if n_pass == len(results) else 1)


if __name__ == "__main__":
    main()
