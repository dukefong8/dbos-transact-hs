SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

GHC  ?= 9.12
PACKAGE ?= dbos-transact-hs

.PHONY: build dev env hie pg test neg db-migrate widget-db

dev:
	ghciwatch --no-interrupt-reloads \
		--command ghci-$(GHC) \
		--error-file ghcid.txt \
		--restart-glob Makefile \
		--restart-glob .ghc.environment.* \
		--restart-glob '!dist-newstyle/**/*.cabal' \
		--reload-glob  '!dist-newstyle/**/*.hs' \
		--enable-eval \
		--watch src \
		--watch test


build:
	cabal build all


test:
	cabal test all


# Regression gate for the ctx invariants (ADR-0029): every negative must
# FAIL with the expected error class (a misuse that compiles is a hole),
# every witness twin must BUILD clean (a broken negative fails for the
# wrong reason). negative/ sits outside the cabal stanzas and the
# ghciwatch globs, so the corpus never enters a build or a reload.
neg: build
	for f in negative/neg_*.hs; do \
		echo "== $$f (must fail)"; \
		out=$$(cabal exec -- ghc -fno-code $$f 2>&1) || true; \
		[ "$$(printf '%s\n' "$$out" | grep -c "Couldn't match\|does not export" || true)" -gt 0 ] || { echo "GATE RED: $$f built or wrong error class"; exit 1; }; \
	done; \
	for f in negative/w_*.hs; do \
		echo "== $$f (must build)"; \
		cabal exec -- ghc -fno-code $$f > /dev/null || { echo "GATE RED: witness $$f failed"; exit 1; }; \
	done; \
	echo "negative gate green"


db-migrate:
	cargo run --quiet --manifest-path rust-migrate/Cargo.toml


# Bootstrap the widget store's app schema before the first typedSql build.
# The demo app also creates it at startup; this is only what the compile-time
# describe needs.
widget-db:
	psql "$${DATABASE_URL:-$${DBOS_DATABASE_URL}}" -v ON_ERROR_STOP=1 -f demo-apps/dbos-hs-widget-store/schema.sql

env:
	rm .ghc.environment.*$(GHC)* || true
	cabal install -w ghc-$(GHC) --enable-documentation \
		--package-env . --lib \
		base containers foldl stm-containers unordered-containers vector template-haskell \
		aeson bytestring text rerefined safe-wild-cards strict-wrapper time uuid \
		contra-tracer contravariant fast-logger io-sim io-classes mtl \
		hasql ihp-typed-sql hasql-pool hasql-transaction hasql-postgresql-types \
		ihp-hsx ihp-router lucid2 wai warp http-types unix \
		breakpoint nothunks rapid silently tasty tasty-hunit tasty-golden
