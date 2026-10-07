#!/usr/bin/env bash
# Run a HammerDB TPROC-C timed run and a tplytics concurrent run at the same time.
#
# Usage: scripts/with-hammerdb.sh HAMMERDB_DIR [tplytics concurrent options ...]
#
# HammerDB is configured with the HDB_* variables described in
# scripts/hammerdb/common.tcl (HDB_DB, HDB_VU, HDB_RAMPUP, HDB_DURATION, ...).
# tplytics starts when the HammerDB rampup is over and runs for the rest of
# the HammerDB run unless --duration is given. Both logs land in ./results/.
set -euo pipefail
[[ $# -ge 1 ]] || { sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }
HDB_HOME=$(cd "$1" && pwd); shift
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(dirname "$HERE")
OUT=${OUT:-results/$(date +%Y%m%d-%H%M%S)}
mkdir -p "$OUT"
RAMPUP=${HDB_RAMPUP:-1}; DURATION=${HDB_DURATION:-10}
export HDB_RAMPUP=$RAMPUP HDB_DURATION=$DURATION

echo "HammerDB: ${HDB_VU:-4} VU, rampup ${RAMPUP} min, ${DURATION} min timed run -> $OUT/hammerdb.log"
( cd "$HDB_HOME" && ./hammerdbcli auto "$HERE/hammerdb/run.tcl" ) > "$OUT/hammerdb.log" 2>&1 &
HPID=$!
trap 'kill $HPID 2>/dev/null || true' EXIT

sleep $((RAMPUP * 60 + 10))
EXTRA=()
[[ " $* " == *" --duration "* || " $* " == *" -D "* ]] || EXTRA=(--duration $((DURATION * 60 - 20)))
python3 "$ROOT/tplytics.py" concurrent "${EXTRA[@]}" --json "$OUT/tplytics.json" --raw "$OUT/tplytics.csv" "$@" \
    | tee "$OUT/tplytics.log"
wait $HPID || true
trap - EXIT
grep -E 'TEST RESULT|NOPM' "$OUT/hammerdb.log" | tail -2 || true
echo "results in $OUT"
