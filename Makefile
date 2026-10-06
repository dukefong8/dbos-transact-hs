SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

GHC  ?= 9.12
PACKAGE ?= dbos-transact-hs

.PHONY: build dev env hie pg test db-migrate widget-db probes

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


# Exec-brand compile probes (docs/invariant-gates.md §5): negatives must
# fail, witness twins must build clean. Builds the library first so the
# probe compiles see a registered package.
probes:
	cabal build lib:dbos-transact-hs
	./probes/run.sh


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
		base containers stm-containers unordered-containers vector template-haskell \
		aeson bytestring text rerefined safe-wild-cards strict-wrapper time uuid \
		contra-tracer contravariant fast-logger io-sim io-classes mtl \
		hasql ihp-typed-sql hasql-pool hasql-transaction hasql-postgresql-types \
		ihp-hsx ihp-router lucid2 wai warp http-types unix \
		breakpoint nothunks rapid silently tasty tasty-hunit tasty-golden
