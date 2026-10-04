#!/bin/sh
# THROWAWAY runner: positives must build+run, negatives must fail to typecheck.
set -u
cd "$(dirname "$0")"

pass=0; fail=0
ok()   { pass=$((pass+1)); echo "PASS: $1"; }
bad()  { fail=$((fail+1)); echo "FAIL: $1"; }

echo "=== build model + positive flows ==="
cabal build lib:dbos-scope-proto exe:proto-io exe:proto-sim exe:proto-backstop 2>&1 | tail -2

echo "=== positive flow under IO ==="
if cabal run proto-io 2>/dev/null; then ok "proto-io runs"; else bad "proto-io runs"; fi

echo "=== positive flow under IOSim ==="
if cabal run proto-sim 2>/dev/null; then ok "proto-sim runs"; else bad "proto-sim runs"; fi

echo "=== negative compile tests (each MUST fail) ==="
for t in neg-n2-step-start neg-n4-nextid-on-step neg-n5-cross-exec-drive \
         neg-n6-cross-run neg-n7-pin-cross-exec; do
  err=$(cabal build exe:$t 2>&1)
  if echo "$err" | grep -q "Couldn't match\|would escape\|Ambiguous\|Not in scope\|could not deduce"; then
    ok "$t rejected"
    echo "$err" | grep -m1 "Couldn't match\|would escape\|Ambiguous\|Not in scope\|could not deduce"
  else
    bad "$t rejected (it BUILT — enforcement hole!)"
  fi
done

echo "=== witness controls (must BUILD CLEAN — else the negative is vacuous) ==="
for w in w-n2-start-with-ctx w-n4-nextid-on-ctx w-n5-same-exec-drive \
         w-n6-same-run w-n7-same-exec-pin; do
  if cabal build exe:$w >/dev/null 2>&1; then
    ok "witness $w builds"
  else
    bad "witness $w does not build — its negative is suspect"
    cabal build exe:$w 2>&1 | grep -m1 "error" || true
  fi
done

echo "=== runtime backstop (capture compiles; refusal must fire) ==="
out=$(cabal run proto-backstop 2>/dev/null)
echo "$out"
for mark in "cross-instance refused" "capture-start refused" "capture-place refused" \
            "depth restored after success" "depth restored after throw" \
            "markers distinct" "tokens per-attempt" "in-step pin refused" \
            "pool cap respected" "raw path exceeds" "use-after-release refused" \
            "pin released on throw"; do
  if echo "$out" | grep -q "backstop: $mark"; then ok "backstop: $mark";
  else bad "backstop: $mark"; fi
done

echo "=== $pass passed, $fail failed ==="
test "$fail" -eq 0
