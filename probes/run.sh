#!/bin/sh
# Permanent exec-brand probes for the real tree (docs/invariant-gates.md §5):
# every negative MUST fail to typecheck with the expected error class, and its
# witness twin — the negative with the one illegal token swapped for the legal
# one — MUST build clean. A witness that fails makes its negative suspect.
#
# Run through `make probes`; the library must be built first.
set -u
cd "$(dirname "$0")/.."

pass=0
fail=0
ok()  { pass=$((pass+1)); echo "PASS: $1"; }
bad() { fail=$((fail+1)); echo "FAIL: $1"; }

probe() {
  # $1 = file, $2 = must-fail | must-pass
  err=$(cabal exec -- ghc -fno-code -fno-write-interface "$1" 2>&1)
  if [ "$2" = "must-fail" ]; then
    if echo "$err" | grep -q "Couldn't match"; then
      ok "$1 rejected"
      echo "$err" | grep -m1 "Couldn't match"
    else
      bad "$1 built clean or failed for the wrong reason"
      echo "$err" | grep -m1 "error" || true
    fi
  else
    if echo "$err" | grep -q "error"; then
      bad "$1 does not build — its negative is suspect"
      echo "$err" | grep -m1 "error" || true
    else
      ok "$1 builds clean"
    fi
  fi
}

echo "=== negative compile probes (each MUST fail) ==="
probe probes/neg-child-start-step.hs must-fail
probe probes/neg-alloc-on-step.hs must-fail
probe probes/neg-nested-on-workflow.hs must-fail
probe probes/neg-cross-exec-arms.hs must-fail

echo "=== witness controls (each MUST build clean) ==="
probe probes/w-child-start-workflow.hs must-pass
probe probes/w-alloc-on-workflow.hs must-pass
probe probes/w-nested-on-step.hs must-pass
probe probes/w-cross-exec-arms.hs must-pass

echo "=== $pass passed, $fail failed ==="
test "$fail" -eq 0
