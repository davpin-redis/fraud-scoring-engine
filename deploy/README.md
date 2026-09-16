# GCP deploy artifacts (redis-ent-wizard `fraud-fbloop` design)

The wizard design (`fraud-fbloop-default`, project `central-beach-194106`, region **europe-west1**)
defines the Redis Enterprise cluster, a GCS bucket, an internal LB, and three VM apps
(`engine` ×5, `pipeline` ×1, `gatling` ×1). Artifacts are staged via the wizard's
`upload_artifact` — no git clone.

## Artifacts (uploaded via `upload_artifact`) — already staged in the design

| App | Artifact (contents) | type | artifact id | Notes |
|---|---|---|---|---|
| engine   | `engine.tar.gz` = **locally-built `app.jar`** + `test_data/` + `deploy/{run_engine.sh,seed.sh}` | `binary` | `9addca8a-3137-4aa6-bb65-f6af5acddda2` | no VM build (jar prebuilt); `test_data/` rides along so the VM can seed |
| pipeline | `pipeline.tar.gz` (`pipeline/` + `deploy/run_pipeline.sh`) | `binary` | `5916d7ac-1e3f-42f5-add1-7fe6e1f76c4f` | `pip install -r requirements.txt` at boot |
| gatling  | `gatling.tar.gz` (`loadtest/` + `deploy/{run_gatling.sh,seed_all.sh}`) | `binary` | `49ab7b01-a460-4311-b1e2-5d039819c456` | **staged only** — load + fan-out seed started manually (see below) |

Rebuild + re-upload when source changes:
```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/openjdk.jdk/Contents/Home
(cd engine && mvn -o -q -DskipTests clean package)
OUT=$TMPDIR/fraud-artifacts; STAGE=$TMPDIR/engine-bundle; mkdir -p "$OUT" "$STAGE/deploy"; export COPYFILE_DISABLE=1
# engine bundle = prebuilt jar + seeders + wrappers
cp engine/target/fraud-scoring-engine-*.jar "$STAGE/app.jar"; cp -R test_data "$STAGE/test_data"
cp deploy/run_engine.sh deploy/seed.sh "$STAGE/deploy/"
tar --no-xattrs -czf "$OUT/engine.tar.gz" -C "$STAGE" .
# pipeline / gatling source tarballs
tar --no-xattrs -czf "$OUT/pipeline.tar.gz" --exclude='__pycache__' --exclude='pipeline/data' pipeline deploy/run_pipeline.sh
tar --no-xattrs -czf "$OUT/gatling.tar.gz"  --exclude='loadtest/target' loadtest deploy/run_gatling.sh
# upload_artifact(path, "binary") for each; set the returned id as the app's artifact.ref
```
Each app's command is `tar xzf <name>.tar.gz 2>/dev/null; bash deploy/run_<app>.sh` (gatling stages
then idles instead) — self-healing whether or not the wizard auto-extracts the uploaded tarball.
**Verify the extraction behaviour in the review UI** (the one artifact-handling assumption).

## Wiring the apps get (injected env → flags, via the wrappers)
- engine / pipeline: `REDIS_FRAUD_ENDPOINT` (Redis DB host:port), `GCS_FRAUD_DATA_URL` (bucket).
  `run_engine.sh` also writes `redis.env` so `seed.sh` can reach Redis when run by hand.
- gatling: `LB_ENGINE_LB_ENDPOINT` — persisted to `gatling.env` at boot for the manual launcher.

## Seeding (manual, on the engine VMs — they're idle pre-traffic)
The engine VMs write their own shard **directly to Redis** (the LB is not involved; seeding needs
deterministic per-engine sharding). Two ways to drive it:

**A. One command, fanned out from the gatling VM (or your laptop)** — `deploy/seed_all.sh` is
bundled on the gatling VM. It discovers the engine VMs (`NAME_FILTER=engine`), then `gcloud ssh`es
into each (IAP) and runs its shard in parallel; the first VM (shard 0) also loads cfg + reference
data + flagged ids:
```bash
bash deploy/seed_all.sh                    # knobs: PROJECT NAME_FILTER ENGINES SHARDS TOTAL DAYS FLAGGED0
```
Prereq: the box you run it from needs the Cloud SDK and rights to list the engines, IAP-tunnel,
and SSH to them. If you run it **on the gatling VM**, grant that VM's service account
`roles/compute.viewer` + `roles/iap.tunnelResourceAccessor` + SSH (OS Login) to the engines —
the wizard's `connect*` wiring does **not** grant cross-VM SSH. Running from your laptop uses your
own gcloud creds instead.

**B. Per VM, by hand** — SSH to each engine VM and, from the extracted app dir:
```bash
SHARD=0 SHARDS=5 bash deploy/seed.sh       # engine VM 0 (also loads cfg/fixtures, FLAGGED0=50000)
SHARD=1 SHARDS=5 bash deploy/seed.sh       # engine VM 1 … through SHARD=4.  Knobs: TOTAL(5000000) DAYS(30)
```

## Gatling (manual load driver)
The gatling VM stages the code and idles. To drive load, SSH in and from the app dir:
```bash
source gatling.env && bash deploy/run_gatling.sh    # knobs: SCENARIO(steady) TPS(1000) CUSTOMERS(5000000) DURATION(1800)
```

## Shared POSIX mount (`$SHARED_DATA`, default `/data`) — operator-provided
The Parquet writer uses atomic temp+rename and the engines read the ONNX model from a shared
path, so a real filesystem is required. The wizard has **no Filestore component** — create one
manually and mount it on the pipeline VM (read-write) and all engine VMs (read-only) at `/data`:
```bash
# once: gcloud filestore instances create fraud-fs --tier=BASIC_SSD --file-share=name=share,capacity=1TB --network=name=default --zone=europe-west1-b
# each VM: sudo mount -t nfs <FS_IP>:/share /data
```
Alternatively `gcsfuse <bucket> /data` (add `--implicit-dirs`); the GCS bucket in the design is
still useful as the durable archive DuckDB can read directly.

## Manual steps the tool cannot do
- `terraform apply` from the wizard review UI (credential/project/region are already set).
- Create/mount the shared `/data` filesystem (above).
- Seed once before traffic — one command `deploy/seed_all.sh` (fan-out) or `deploy/seed.sh` per VM (above).
- Start Gatling manually when ready (`deploy/run_gatling.sh`, above).
- Reach the dashboard over an IAP tunnel: `gcloud compute ssh <pipeline-vm> --tunnel-through-iap -- -L 8090:localhost:8090`.
- ONNX hot-reload across the run still needs the fetch-on-`model:invalidate` hook (or the shared
  mount above, which sidesteps it); until the first model lands, engines run rules-only.
