#!/usr/bin/env bash
# MANUAL launcher — run this on the gatling VM (over SSH) when you want to drive load.
# The app does NOT start Gatling at boot; boot only stages the code and records the LB
# endpoint into ./gatling.env (see the app command in the wizard design).
#
# Usage on the VM:   cd <app dir> && source gatling.env && bash deploy/run_gatling.sh
# Overridable knobs: SCENARIO (steady) TPS (1000) CUSTOMERS (5000000) DURATION (1800)
set -euo pipefail
cd "$(dirname "$0")/.."                       # app dir (contains loadtest/ and gatling.env)
[ -f gatling.env ] && source gatling.env      # LB_ENGINE_LB_ENDPOINT persisted at boot

: "${LB_ENGINE_LB_ENDPOINT:?not set — run from the app dir where boot wrote gatling.env, or export it}"
SCENARIO="${SCENARIO:-steady}"; TPS="${TPS:-1000}"; CUSTOMERS="${CUSTOMERS:-5000000}"; DURATION="${DURATION:-1800}"

cd loadtest
exec mvn -q gatling:test \
  -Dgatling.simulationClass=fraud.FeedbackLoopSimulation \
  -DbaseUrl="http://${LB_ENGINE_LB_ENDPOINT}:8080" \
  -Dscenario="$SCENARIO" -Dtps="$TPS" -Dcustomers="$CUSTOMERS" -DdurationSec="$DURATION"
