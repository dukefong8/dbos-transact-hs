#!/bin/sh
# THROWAWAY runner: positives must build+run and agree; replay must not
# re-execute effects; negatives must fail to typecheck.
set -u
cd "$(dirname "$0")"

pass=0; fail=0
ok()   { pass=$((pass+1)); echo "PASS: $1"; }
bad()  { fail=$((fail+1)); echo "FAIL: $1"; }

echo "=== build model + positive flows ==="
cabal build lib:dbos-steps-proto exe:proto-io exe:proto-sim 2>&1 | tail -2

echo "=== positive flow under IO ==="
io_out=$(cabal run proto-io 2>/dev/null) || bad "proto-io runs"
echo "$io_out"

echo "=== positive flow under IOSim ==="
sim_out=$(cabal run proto-sim 2>/dev/null) || bad "proto-sim runs"
echo "$sim_out"

echo "=== IO/Sim agreement (recorded slots only) ==="
echo "$io_out" | grep -E "^(STEP|OUTCOME)" | sort > /tmp/steps-io.txt
echo "$sim_out" | grep -E "^(STEP|OUTCOME)" | sort > /tmp/steps-sim.txt
if diff -q /tmp/steps-io.txt /tmp/steps-sim.txt >/dev/null; then
  ok "IO/Sim step agreement"
else
  bad "IO/Sim step agreement"; diff /tmp/steps-io.txt /tmp/steps-sim.txt | head -6
fi

echo "=== replay determinism (no effect re-execution) ==="
for mark in "CALLS n=4" "REPLAY-OUTCOME total=5700 lane=express" \
            "REPLAY-MATCH True" "REPLAY-CALLS n=4"; do
  if echo "$io_out" | grep -q "$mark"; then ok "io: $mark";
  else bad "io: $mark"; fi
  if echo "$sim_out" | grep -q "$mark"; then ok "sim: $mark";
  else bad "sim: $mark"; fi
done

echo "=== negative compile tests (each MUST fail) ==="
for t in neg-op-in-workflow neg-raw-io neg-cross-exec; do
  err=$(cabal build exe:$t 2>&1)
  if echo "$err" | grep -q "Couldn't match\|would escape\|No instance\|rigid"; then
    ok "$t rejected"
    echo "$err" | grep -m1 "Couldn't match\|would escape\|No instance\|rigid"
  else
    bad "$t rejected (it BUILT — enforcement hole!)"
  fi
done

echo "=== witness controls (must BUILD CLEAN — else the negative is vacuous) ==="
for w in w-op-in-step w-shared-body w-same-exec; do
  if cabal build exe:$w >/dev/null 2>&1; then
    ok "witness $w builds"
  else
    bad "witness $w does not build — its negative is suspect"
    cabal build exe:$w 2>&1 | grep -m1 "error" || true
  fi
done
echo "=== witness W2 runs (shared body under IOSim) ==="
if [ "$(cabal run w-shared-body 2>/dev/null)" = "8" ]; then
  ok "witness w-shared-body runs (7 + 1)"
else
  bad "witness w-shared-body did not produce the expected result"
fi

echo "=== $pass passed, $fail failed ==="
test "$fail" -eq 0
