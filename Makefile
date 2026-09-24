SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

GHC  ?= 9.12
PACKAGE ?= dbos-transact-hs

.PHONY: build dev env hie pg test db-migrate

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


db-migrate:
	cargo run --quiet --manifest-path rust-migrate/Cargo.toml

env:
	rm .ghc.environment.*$(GHC)* || true
	cabal install -w ghc-$(GHC) --enable-documentation \
		--package-env . --lib \
		base template-haskell bytestring text containers vector stm-containers \
		aeson safe-wild-cards strict-wrapper time uuid \
		bluefin co-log co-log-core fast-logger io-classes io-classes:strict-stm io-classes:strict-mvar io-classes:si-timers io-classes:mtl io-sim mtl \
		hasql ihp-typed-sql hasql-pool hasql-postgresql-types postgresql-types \
		breakpoint nothunks rapid silently tasty tasty-hunit tasty-golden
