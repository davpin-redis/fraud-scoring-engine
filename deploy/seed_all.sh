#!/usr/bin/env bash
# Fan-out seeder — run on ONE box (the gatling VM, or your laptop) to seed ALL engine VMs in
# parallel, each writing its own customer-id shard directly to Redis. This is option 2: one
# command → each engine seeds directly, over `gcloud ssh` (IAP). The load balancer is NOT
# involved (seeding needs deterministic per-engine sharding, the opposite of LB spreading),
# and no seed endpoint is added to the engine.
#
# Prereqs on the box you run this from:
#   - gcloud SDK, authenticated as a principal (the gatling VM's service account, or you) with
#     compute.instances.list/get, IAP tunneling (roles/iap.tunnelResourceAccessor), and SSH to
#     the engine VMs (OS Login or metadata SSH). If run from the gatling VM, grant its SA these.
#
# Config (env):
#   PROJECT      GCP project                     (default: gcloud config's project)
#   NAME_FILTER  engine instance-name substring  (default: "engine")
#   ENGINES      explicit "name:zone name:zone …" to skip auto-discovery
#   APP_DIR      dir on the engine VM holding deploy/seed.sh (default: auto-locate via find)
#   SHARDS       number of shards                (default: number of engines found)
#   TOTAL DAYS FLAGGED0   forwarded to seed.sh    (defaults 5000000 / 30 / 50000)
set -euo pipefail

command -v gcloud >/dev/null || { echo "gcloud not found on this box — run from a VM/laptop with the Cloud SDK"; exit 1; }
PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
NAME_FILTER="${NAME_FILTER:-engine}"

# 1. resolve engine VMs as "name zone" pairs (sorted → stable shard assignment; first = shard 0)
names=(); zones=()
if [ -n "${ENGINES:-}" ]; then
  for e in $ENGINES; do names+=("${e%%:*}"); zones+=("${e##*:}"); done
else
  while read -r n z; do [ -n "$n" ] && { names+=("$n"); zones+=("$z"); }; done < <(
    gcloud compute instances list --project "$PROJECT" \
      --filter="name~${NAME_FILTER}" --format="value(name,zone)" | sort)
fi
N="${#names[@]}"; SHARDS="${SHARDS:-$N}"
[ "$N" -gt 0 ] || { echo "no engine VMs matched NAME_FILTER=$NAME_FILTER (set ENGINES=... to override)"; exit 1; }
echo "[seed_all] project=$PROJECT  $N engine VM(s), SHARDS=$SHARDS"

# 2. per-VM remote command: cd to the app dir (auto-located if APP_DIR unset) and seed this shard
remote_cmd() {  # $1 = shard index
  cat <<EOF
set -e
D="${APP_DIR:-}"
[ -z "\$D" ] && D="\$(dirname "\$(find / -maxdepth 8 -path '*/deploy/seed.sh' 2>/dev/null | head -1)")/.."
cd "\$D"
SHARD=$1 SHARDS=$SHARDS TOTAL="${TOTAL:-5000000}" DAYS="${DAYS:-30}" FLAGGED0="${FLAGGED0:-50000}" bash deploy/seed.sh
EOF
}

# 3. fan out — one shard per VM, in parallel
pids=()
for i in "${!names[@]}"; do
  echo "[seed_all] shard $i -> ${names[$i]} (${zones[$i]})   log: seed-${names[$i]}.log"
  gcloud compute ssh "${names[$i]}" --project "$PROJECT" --zone "${zones[$i]}" --tunnel-through-iap \
    --command "$(remote_cmd "$i")" >"seed-${names[$i]}.log" 2>&1 &
  pids+=("$!")
done

# 4. wait + report
rc=0
for i in "${!pids[@]}"; do
  if wait "${pids[$i]}"; then echo "[seed_all] shard $i (${names[$i]}) OK"
  else echo "[seed_all] shard $i (${names[$i]}) FAILED — see seed-${names[$i]}.log"; rc=1; fi
done
[ "$rc" = 0 ] && echo "[seed_all] all shards done" || echo "[seed_all] some shards failed"
exit $rc
