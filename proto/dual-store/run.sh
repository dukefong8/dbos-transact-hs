#!/bin/sh
# THROWAWAY runner: same workflow source, two transaction handlers.
# Postgres under IO, STM under IOSim; identical invariant lines either
# way; the cross-stack misuse must fail to typecheck.
set -u
cd "$(dirname "$0")"

pass=0; fail=0
ok()   { pass=$((pass+1)); echo "PASS: $1"; }
bad()  { fail=$((fail+1)); echo "FAIL: $1"; }

echo "=== build model + positive flows ==="
cabal build lib:dual-store-proto exe:proto-io exe:proto-sim 2>&1 | tail -2

if [ -z "${DBOS_DATABASE_URL:-}" ]; then
  echo "SKIP: proto-io needs DBOS_DATABASE_URL (live Postgres); running sim + negatives only"
  io_out=""
else
  echo "=== widget flow over Postgres transactions (IO) ==="
  io_out=$(cabal run proto-io 2>/dev/null) || bad "proto-io runs"
  echo "$io_out"
fi

echo "=== widget flow over STM (IOSim) ==="
sim_out=$(cabal run proto-sim 2>/dev/null) || bad "proto-sim runs"
echo "$sim_out"

echo "=== invariant agreement ==="
for mark in "INSUFFICIENT-OK refused=True stock=10 orders=0 audits=1" \
            "ROLLBACK-OK threw=True stock=10 orders=0 audits=0" \
            "RACE winners=3 losers=5 stock=1 orders=3 audits=11"; do
  if echo "$sim_out" | grep -q "$mark"; then ok "sim: $mark";
  else bad "sim: $mark"; fi
  if [ -n "$io_out" ]; then
    if echo "$io_out" | grep -q "$mark"; then ok "io: $mark";
    else bad "io: $mark"; fi
  fi
done

echo "=== negative compile tests (each MUST fail) ==="
for t in neg-step-cross-stack; do
  err=$(cabal build exe:$t 2>&1)
  if echo "$err" | grep -q "Couldn't match\|rigid"; then
    ok "$t rejected"
    echo "$err" | grep -m1 "Couldn't match\|rigid"
  else
    bad "$t rejected (it BUILT — enforcement hole!)"
  fi
done

echo "=== witness control (must BUILD CLEAN — else the negative is vacuous) ==="
if cabal build exe:w-sim-scope >/dev/null 2>&1; then
  ok "witness w-sim-scope builds"
else
  bad "witness w-sim-scope does not build — its negative is suspect"
  cabal build exe:w-sim-scope 2>&1 | grep -m1 "error" || true
fi

echo "=== $pass passed, $fail failed ==="
test "$fail" -eq 0
