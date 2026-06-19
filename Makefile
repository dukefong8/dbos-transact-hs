SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

GHC  ?= 9.12
PACKAGE ?= dbos-transact-hs

.PHONY: build dev env hie pg test

dev:
	ghciwatch --clear --no-interrupt-reloads \
		--command ghci-$(GHC) \
		--error-file ghcid.txt \
		--restart-glob Makefile \
		--restart-glob .ghc.environment.* \
		--enable-eval --watch .


build:
	cabal build all


test:
	cabal test all

env:
	rm .ghc.environment.*$(GHC)* || true
	cabal install -w ghc-$(GHC) --enable-documentation \
		--package-env . --lib \
		base containers stm-containers text vector template-haskell \
		bytestring time uuid exceptions base64-bytestring \
		aeson generic-data optics witch safe-wild-cards strict-wrapper \
		hasql hasql-th hasql-dynamic-statements hasql-pool hasql-postgresql-types postgresql-types \
		async bluefin co-log fast-logger mtl io-classes io-sim stm \
		breakpoint nothunks rapid hedgehog tasty tasty-hunit tasty-hedgehog tasty-golden
