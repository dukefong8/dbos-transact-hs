SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

GHC  ?= 9.12
PACKAGE ?= dbos-transact-hs

.PHONY: build dev env hie pg test db-migrate

dev:
	ghciwatch --clear --no-interrupt-reloads \
		--command ghci-$(GHC) \
		--error-file .ghcid.txt \
		--restart-glob Makefile \
		--restart-glob .ghc.environment.* \
		--restart-glob '!dist-newstyle/**/*.cabal' \
		--reload-glob  '!dist-newstyle/**/*.hs' \
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
		base containers stm-containers text vector template-haskell \
		bytestring time uuid exceptions base64-bytestring \
		aeson generic-data safe-wild-cards strict-wrapper \
		hasql ihp-typed-sql hasql-pool hasql-postgresql-types postgresql-types \
		async bluefin co-log co-log-core fast-logger mtl io-classes io-sim stm \
		breakpoint nothunks rapid hedgehog tasty tasty-hunit tasty-hedgehog tasty-golden
