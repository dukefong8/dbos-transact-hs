#!/bin/sh
# THROWAWAY runner: positives must build+run, negatives must fail to typecheck.
set -u
cd "$(dirname "$0")"

pass=0; fail=0
ok()   { pass=$((pass+1)); echo "PASS: $1"; }
bad()  { fail=$((fail+1)); echo "FAIL: $1"; }

echo "=== build model + positive flows ==="
cabal build lib:dbos-scope-proto exe:proto-io exe:proto-sim 2>&1 | tail -2

echo "=== positive flow under IO ==="
if cabal run proto-io 2>/dev/null; then ok "proto-io runs"; else bad "proto-io runs"; fi

echo "=== positive flow under IOSim ==="
if cabal run proto-sim 2>/dev/null; then ok "proto-sim runs"; else bad "proto-sim runs"; fi

echo "=== negative compile tests (each MUST fail) ==="
for t in neg-n1-mix-instances neg-n2-step-start neg-n3-escape-handle \
         neg-n4-nextid-on-step neg-n5-cross-exec-drive neg-n6-cross-run; do
  err=$(cabal build exe:$t 2>&1)
  if echo "$err" | grep -q "Couldn't match\|would escape\|Ambiguous\|Not in scope\|could not deduce"; then
    ok "$t rejected"
    echo "$err" | grep -m1 "Couldn't match\|would escape\|Ambiguous\|Not in scope\|could not deduce"
  else
    bad "$t rejected (it BUILT — enforcement hole!)"
  fi
done

echo "=== residual hole exhibit (MUST build: the hole is real) ==="
if cabal build exe:residual-capture >/dev/null 2>&1; then
  ok "residual-capture builds (parent-capture bypasses the split)"
else
  bad "residual-capture builds"
fi

echo "=== $pass passed, $fail failed ==="
test "$fail" -eq 0
