#!/bin/bash
# Run scripts/bench-shared-state.py under the machine-wide benchmark lock and the performance
# CPU governor, as the v3 harness run_ab.sh does. Builds and tests take the lock shared, so the
# benchmark waits for them, and they wait for it.
#
# usage: BENCH_LOCKS=<dir with .bench.gate and .bench.lock> run-shared-bench.sh <script args...>
set -euo pipefail
LOCKS=${BENCH_LOCKS:?set BENCH_LOCKS to the directory of the shared benchmark lock}
SCRIPT=$(cd "$(dirname "$0")" && pwd)/bench-shared-state.py
RUNNER=$(id -un)
PREV_GOV=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
restore() {
  echo "$PREV_GOV" | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor >/dev/null
  echo "governor restored to $PREV_GOV $(date -Is)"
}
trap restore EXIT
ARGS=$(printf '%q ' "$@")
echo "start $(date -Is) args: $*"
sudo flock -x "$LOCKS/.bench.gate" flock -x "$LOCKS/.bench.lock" bash -c \
  "for f in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo performance > \$f; done; \
   sudo -u $RUNNER env HOME=$HOME PATH=$PATH python3 $SCRIPT $ARGS"
echo "done $(date -Is)"
