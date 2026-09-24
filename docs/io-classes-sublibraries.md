# io-classes sublibraries: what this repo actually needs

Question: `dbos-transact-hs` adopted `io-classes` for the retry core and the
`io-sim` tests, and `dbos-transact-hs.cabal:30-34` lists four io-classes
build-deps plus `io-sim`. Which of the package's public sublibraries are
needed, which are dead weight, and what does each add over the main library?
Sources read directly: the installed store confs (authoritative for what is
actually built), the Hackage tarball `io-classes-1.11.0.0`, the resolved
`plan.json`, and the repo itself. Every claim cites `file:line` or exact
command output; commands are in [Evidence](#evidence). No source or cabal
files were changed.

Installed: **io-classes-1.11.0.0** (all sublibraries) and **io-sim-1.11.0.0**,
GHC 9.12.4. The store holds 18 `-clsss-*.conf` units: 3 main, 3 `strict-stm`,
3 `si-timers`, 3 `mtl`, 2 `strict-mvar`, 4 `testlib` — the duplicates are
different dependency hashes, not different module sets.

## Summary verdict

| Sublibrary | Exposed modules | Imported by repo? | Verdict |
|---|---|---|---|
| `io-classes` (main) | 21 modules: `Control.Concurrent.Class.MonadMVar`, `...MonadSTM[.TVar/.TMVar/.TChan/.TQueue/.TBQueue/.TArray/.TSem]`, `Control.Monad.Class.{MonadAsync,MonadEventlog,MonadFork,MonadST,MonadSTM,MonadSTM.Internal,MonadThrow,MonadTime,MonadTimer,MonadSay,MonadTest,MonadUnique}` | **Yes** — 5 files, 4 distinct modules | **Keep** |
| `io-classes:strict-stm` | 7 `Control.Concurrent.Class.MonadSTM.Strict*` modules | No (0 hits) | **Drop** from build-depends; `io-sim` still pulls it transitively. Re-add when `Control.Concurrent.Class.MonadSTM.Strict` is imported |
| `io-classes:si-timers` | `Control.Monad.Class.MonadTime.SI`, `Control.Monad.Class.MonadTimer.SI` | No (0 hits) | **Drop** from build-depends; `io-sim` still pulls it transitively. Re-add when `MonadTimer.SI`/`MonadTime.SI` is imported |
| `io-classes:mtl` | `Control.Monad.Class.Trans` + 10 `*.Trans` orphan-instance modules | No (0 hits) | **Drop** |
| `io-classes:strict-mvar` | `Control.Concurrent.Class.MonadMVar.Strict` | No — not even in build-depends | **Unnecessary, do not add** |
| `io-classes:testlib` (bonus) | `Test.Control.Concurrent.Class.MonadMVar.Strict.WHNF` | No — not a dep | **Not needed** (only io-sim's own test-suite uses it) |

`io-classes:mtl` and `io-classes:strict-mvar` should be dropped/not added:
nothing in `src/`, `test/`, or `app/` imports any of their modules.
`si-timers` and `strict-stm` are also unused by direct import, so the direct
edges are removable; the modules that would justify re-adding them are named
in their sections.

## Main library `io-classes`

The main library's own description (`io-classes.cabal:4`): "Type classes for
concurrency with STM, ST and timing". The tarball's `library` stanza
(`io-classes.cabal:64-103`) exposes 21 modules, matching the installed conf
`-clsss-1.11.0.0-38394964` exactly (`name: io-classes`, no `lib-name:` field,
so it is the main unit).

Repo imports (from `rg -n 'Control\.(Monad|Concurrent)\.(Class|IOSim)' src/ test/ app/`):

| Import | File |
|---|---|
| `Control.Monad.Class.MonadTimer (MonadDelay, threadDelay)` | `src/DBOS/SystemDB/Retry.hs:24` |
| `Control.Concurrent.Class.MonadSTM (MonadSTM (..))` | `test/DBOS/SimDB.hs:16`, `test/DBOS/SimTest.hs:14` |
| `Control.Concurrent.Class.MonadSTM (atomically, newTVarIO, readTVarIO, writeTVar)` | `test/DBOS/SystemDB/RetryTest.hs:11` |
| `Control.Monad.Class.MonadSay (MonadSay (..))` / `(say)` | `test/DBOS/SimTest.hs:15`, `test/DBOS/SystemDB/RetryTest.hs:12` |
| `Control.Monad.IOSim (...)` (io-sim, not io-classes) | `test/DBOS/SimTest.hs:18`, `test/DBOS/SystemDB/RetryTest.hs:13` |

### (a) `MonadTimer` / `MonadDelay` is in the main library

`Control.Monad.Class.MonadTimer` appears in the main conf's
`exposed-modules:` list, and in `io-classes.cabal:88` inside the `library`
stanza — not in any sublibrary stanza. The class is defined there:

- `io-classes/Control/Monad/Class/MonadTimer.hs:23` — `class Monad m => MonadDelay m where`
- `:29` — `threadDelay :: Int -> m ()`
- `:32` — `class (MonadDelay m, MonadSTM m) => MonadTimer m` (so main is self-sufficient)

The same-named `MonadDelay` in `si-timers` is a *different* class
(`DiffTime -> m ()`, `si-timers/src/Control/Monad/Class/MonadTimer/SI.hs:71-78`);
the repo imports the main one.

### (b) The main library alone covers every io-classes import in the repo

All four imported modules are in the main conf's exposed list:
`Control.Monad.Class.MonadTimer`, `Control.Concurrent.Class.MonadSTM`,
`Control.Monad.Class.MonadSay`, and (available but not yet imported)
`Control.Monad.Class.MonadThrow`, `Control.Monad.Class.MonadAsync`,
`Control.Monad.Class.MonadFork`, `Control.Monad.Class.MonadTime`.

- `Control.Concurrent.Class.MonadSTM` is main (`io-classes.cabal:71`,
  conf `38394964`); it re-exports `Control.Concurrent.Class.MonadSTM.TVar`
  (`io-classes/Control/Concurrent/Class/MonadSTM.hs:3-10`), where
  `newTVarIO`/`readTVarIO`/`writeTVar` are class methods
  (`MonadSTM/Internal.hs:240-241`); `atomically` is a method of `MonadSTM`
  (`MonadSTM/Internal.hs:144`).
- `Control.Monad.Class.MonadSay` is main (`io-classes.cabal:82`).
- `Control.Monad.Class.MonadTime` is main (`io-classes.cabal:87`); the
  `Time.SI` variant is the `si-timers` one and is not imported.
- `Control.Monad.IOSim` is `io-sim`, not an io-classes sublibrary
  (`io-sim-1.11.0.0/io-sim.cabal:51-52`).

Note: `test/DBOS/SimTest.hs:17` imports `MonadThrow` from
`Control.Monad.Catch` (the `exceptions` package), not from io-classes; the
io-classes `MonadThrow` is available in main if the engine ever switches.

## `io-classes:strict-stm`

Adds, over main, seven exposed modules
(`io-classes.cabal:112-118`): `Control.Concurrent.Class.MonadSTM.Strict` and
its `.TArray`, `.TBQueue`, `.TChan`, `.TMVar`, `.TQueue`, `.TVar` children,
plus the re-export `Control.Concurrent.Class.MonadSTM.TSem as
Control.Concurrent.Class.MonadSTM.Strict.TSem` (`io-classes.cabal:119`). It
depends only on `base`, `array`, `io-classes:io-classes`
(`io-classes.cabal:124-127`, conf `-clsss-1.11.0.0-a525cffd`).

What it adds module-by-module: the strict `TVar`/`TMVar`/`TChan`/`TQueue`/
`TBQueue`/`TArray` newtypes that force `NFData`/`NoThunks` on write
(`strict-stm/README.md`), and it re-exports the rest of main's
`MonadSTM` API. The repo has **zero** imports of any `...MonadSTM.Strict`
module; its only TVar use is the lazy API from main
(`test/DBOS/SimDB.hs:16`, `test/DBOS/SystemDB/RetryTest.hs:11`).

`io-sim` itself depends on this sublibrary
(`io-sim-1.11.0.0/io-sim.cabal:68`: `io-classes:{io-classes,strict-stm,si-timers}`),
so dropping the direct edge does not remove it from the build plan.

## `io-classes:si-timers`

Adds two exposed modules plus one hidden module
(`io-classes.cabal:149-151`): `Control.Monad.Class.MonadTime.SI` (the SI
`newtype Time = Time DiffTime` at `MonadTime/SI.hs:42` and class
`MonadMonotonicTime` at `:60`) and `Control.Monad.Class.MonadTimer.SI` (its
own `MonadDelay` with `threadDelay :: DiffTime -> m ()` at
`MonadTimer/SI.hs:71-78`, its own `MonadTimer` with `registerDelay`,
`registerDelayCancellable` and `timeout` at `:130-142`, plus
`diffTimeToMicrosecondsAsInt`/`microsecondsAsIntToDiffTime`/
`roundDiffTimeToMicroseconds`); hidden `MonadTimer.NonStandard`. Depends on
`base`, `deepseq`, `mtl`, `nothunks`, `stm`, `time`,
`io-classes:io-classes` (`io-classes.cabal:154-161`, conf
`-clsss-1.11.0.0-4eade525`).

The repo imports none of it: `src/DBOS/SystemDB/Retry.hs:24` takes the
`Int`-microsecond `threadDelay` from main. The sibling note already left this
as an open question ("Whether to depend on `io-classes:si-timers` for
`DiffTime`-typed delays and `registerDelayCancellable`",
`docs/io-classes-impure-constraints.md:217-219`). `io-sim` also depends on it
(`io-sim.cabal:68`), so the unit stays in the plan either way. Re-add the
direct edge only when `Control.Monad.Class.MonadTimer.SI` or
`Control.Monad.Class.MonadTime.SI` is actually imported — e.g. if retry
jitter moves to sub-millisecond `DiffTime` or virtual-time assertions need SI
timestamps.

## `io-classes:mtl`

Eleven exposed modules (`io-classes.cabal:168-178`): `Control.Monad.Class.Trans`
plus `MonadEventlog.Trans`, `MonadSay.Trans`, `MonadST.Trans`,
`MonadSTM.Trans`, `MonadThrow.Trans`, `MonadTime.Trans`,
`MonadTime.SI.Trans`, `MonadTimer.Trans`, `MonadTimer.SI.Trans`,
`MonadUnique.Trans`. Every one is orphan transformer instances for
`ContT`/`ExceptT`/`RWST`/`StateT`/`WriterT`, compiled with
`-Wno-orphans`; `Control.Monad.Class.Trans` is nothing but
"Export all orphaned instances" (`mtl/Control/Monad/Class/Trans.hs:1-4`).
`MonadSTM.Trans` even defines a bespoke `ContTSTM` newtype and carries 34
instances. The package description flags this directly
(`io-classes.cabal:26`: "`io-classes:mtl` - MTL instances, some of which are
experiemental" [sic]); the CHANGELOG's mtl entries are
`CHANGELOG.md:39` (new `MonadUnique.Trans`), `:59` ("instances support the
extended `MonadMask` instance") and `:71-78` (renames/moves).

It depends on `io-classes:{io-classes,si-timers}` and `mtl`
(`io-classes.cabal:179-183`, conf `-clsss-1.11.0.0-b99a1fa7`), so it is what
drags `si-timers` into the plan alongside `io-sim`. The repo imports **none**
of these modules, and note `dbos-transact-hs.cabal:35`'s `mtl` is the
standard `mtl-2.3.2` package (`plan.json` id `mtl-2.3.2-8089`), unrelated to
`io-classes:mtl`.

## `io-classes:strict-mvar`

One exposed module, `Control.Concurrent.Class.MonadMVar.Strict`
(`io-classes.cabal:137`, conf `-clsss-1.11.0.0-496e87dd`), plus its README on
space-leak elimination. It is **not** in this repo's `build-depends`
(`dbos-transact-hs.cabal:30-34` has no `strict-mvar`), and no repo module
imports `MonadMVar` at all. It appears in the store only as a dependency of
`io-classes:testlib` (conf `-clsss-1.11.0.0-bffcb317` depends on
`-clsss-1.11.0.0-496e87dd`), and `testlib` is used by io-sim's own test-suite
(`io-sim.cabal:104`); `plan.json` shows no component of this project
depending on either. Nothing to drop — just never add it.

## Open questions

- `dbos-transact-hs.cabal:32-33` will list unused direct deps after the
  drop; if `-Wunused-packages` is ever enabled the repo will need this audit
  re-run (currently only `io-classes.cabal:60` enables that warning for
  io-classes itself).
- Whether the retry loop should move from main's `Int`-microsecond
  `threadDelay` to `si-timers`' `DiffTime` API when jitter goes sub-second —
  this is the same question as
  `docs/io-classes-impure-constraints.md:217-219`, still open.
- Dropping the direct `strict-stm`/`si-timers` edges does not remove their
  units from `plan.json` while `io-sim` depends on them
  (`io-sim.cabal:68`); only the direct dependency declaration changes.
- `io-classes:testlib` (`Test.Control.Concurrent.Class.MonadMVar.Strict.WHNF`)
  could be useful if this repo ever wants `nothunks`-based WHNF checks on
  strict MVars; today it is not a dep and there is no strict MVar.

## Evidence

All commands run from `/Users/duke/dev/dbos-transact-hs` unless noted.

1. Installed version and units. The store holds 18 `-clsss-*.conf` files:

```
$ ls /Users/duke/.cabal/store/ghc-9.12.4-6f4d/package.db/ | grep -i -E 'clsss|io-sim'
-clsss-1.11.0.0-17816f99.conf   -clsss-1.11.0.0-4eade525.conf
-clsss-1.11.0.0-2b08e4c8.conf   -clsss-1.11.0.0-500a734f.conf
-clsss-1.11.0.0-35dfb2fa.conf   -clsss-1.11.0.0-6bab6511.conf
-clsss-1.11.0.0-38394964.conf   -clsss-1.11.0.0-998809c3.conf
-clsss-1.11.0.0-496e87dd.conf   -clsss-1.11.0.0-a525cffd.conf
                                -clsss-1.11.0.0-b21adc2c.conf
-clsss-1.11.0.0-b99a1fa7.conf   -clsss-1.11.0.0-bd9abe83.conf
-clsss-1.11.0.0-bffcb317.conf   -clsss-1.11.0.0-c0b1697d.conf
-clsss-1.11.0.0-cc28e6da.conf   -clsss-1.11.0.0-cf435c31.conf
-clsss-1.11.0.0-f1779c3b.conf
```

   `grep -E '^(name|lib-name):'` over every conf yields:

| lib-name (absent = main) | Conf hashes | `name:` |
|---|---|---|
| — (main) | `38394964`, `500a734f`, `f1779c3b` | `io-classes` |
| `strict-stm` | `17816f99`, `35dfb2fa`, `a525cffd` | `z-io-classes-z-strict-stm` |
| `si-timers` | `4eade525`, `6bab6511`, `bd9abe83` | `z-io-classes-z-si-timers` |
| `mtl` | `2b08e4c8`, `998809c3`, `b99a1fa7` | `z-io-classes-z-mtl` |
| `strict-mvar` | `496e87dd`, `b21adc2c` | `z-io-classes-z-strict-mvar` |
| `testlib` | `bffcb317`, `c0b1697d`, `cc28e6da`, `cf435c31` | `z-io-classes-z-testlib` |

   Full `exposed-modules:`/`hidden-modules:` per lib-name (identical across
   that lib-name's hashes, verified in the raw conf output):

   - main (`-clsss-1.11.0.0-38394964`, also `500a734f`/`f1779c3b`):
     `Control.Concurrent.Class.MonadMVar`,
     `Control.Concurrent.Class.MonadSTM`,
     `Control.Concurrent.Class.MonadSTM.TArray`,
     `Control.Concurrent.Class.MonadSTM.TBQueue`,
     `Control.Concurrent.Class.MonadSTM.TChan`,
     `Control.Concurrent.Class.MonadSTM.TMVar`,
     `Control.Concurrent.Class.MonadSTM.TQueue`,
     `Control.Concurrent.Class.MonadSTM.TSem`,
     `Control.Concurrent.Class.MonadSTM.TVar`,
     `Control.Monad.Class.MonadAsync`,
     `Control.Monad.Class.MonadEventlog`,
     `Control.Monad.Class.MonadFork`,
     `Control.Monad.Class.MonadST`,
     `Control.Monad.Class.MonadSTM`,
     `Control.Monad.Class.MonadSTM.Internal`,
     `Control.Monad.Class.MonadSay`,
     `Control.Monad.Class.MonadTest`,
     `Control.Monad.Class.MonadThrow`,
     `Control.Monad.Class.MonadTime`,
     `Control.Monad.Class.MonadTimer`,
     `Control.Monad.Class.MonadUnique`; hidden: none.
   - `strict-stm` (`a525cffd`, etc.):
     `Control.Concurrent.Class.MonadSTM.Strict`,
     `Control.Concurrent.Class.MonadSTM.Strict.TArray`,
     `Control.Concurrent.Class.MonadSTM.Strict.TBQueue`,
     `Control.Concurrent.Class.MonadSTM.Strict.TChan`,
     `Control.Concurrent.Class.MonadSTM.Strict.TMVar`,
     `Control.Concurrent.Class.MonadSTM.Strict.TQueue`,
     `Control.Concurrent.Class.MonadSTM.Strict.TVar`, and the re-export
     `Control.Concurrent.Class.MonadSTM.Strict.TSem from
     -clsss-1.11.0.0-38394964:Control.Concurrent.Class.MonadSTM.TSem`;
     hidden: none.
   - `si-timers` (`4eade525`, etc.):
     `Control.Monad.Class.MonadTime.SI`,
     `Control.Monad.Class.MonadTimer.SI`; hidden:
     `Control.Monad.Class.MonadTimer.NonStandard`.
   - `mtl` (`b99a1fa7`, etc.): `Control.Monad.Class.MonadEventlog.Trans`,
     `Control.Monad.Class.MonadST.Trans`,
     `Control.Monad.Class.MonadSTM.Trans`,
     `Control.Monad.Class.MonadSay.Trans`,
     `Control.Monad.Class.MonadThrow.Trans`,
     `Control.Monad.Class.MonadTime.SI.Trans`,
     `Control.Monad.Class.MonadTime.Trans`,
     `Control.Monad.Class.MonadTimer.SI.Trans`,
     `Control.Monad.Class.MonadTimer.Trans`,
     `Control.Monad.Class.MonadUnique.Trans`,
     `Control.Monad.Class.Trans`; hidden: none.
   - `strict-mvar` (`496e87dd`): `Control.Concurrent.Class.MonadMVar.Strict`;
     hidden: none. Depends: `base-4.21.2.0-fc24
     -clsss-1.11.0.0-500a734f`.
   - `testlib` (`bffcb317`): `Test.Control.Concurrent.Class.MonadMVar.Strict.WHNF`;
     hidden: none. Depends: `QckChck-2.19.0.0-3b6348ca base-4.21.2.0-fc24
     -clsss-1.11.0.0-496e87dd nthnks-0.3.2-3fc9142a`.

   Dependency edges relevant to the verdict (from conf `depends:`):
   main `38394964` → `array, async, base, bytestring, ghc-internal, mtl,
   primitive, stm, time`; `strict-stm a525cffd` → `array, base,
   38394964`; `si-timers 4eade525` → `base, deepseq, 38394964, mtl,
   nothunks, stm, time`; `mtl b99a1fa7` → `array, base, 38394964,
   4eade525, mtl`; `io-sim 49bec4e6` → `... 38394964, 4eade525,
   a525cffd ...` (i.e. main + si-timers + strict-stm).

2. Resolved plan (which units this project actually uses):

```
$ python3 -c "import json; p=json.load(open('dist-newstyle/cache/plan.json')); ..."
io-classes 1.11.0.0 component: lib          # 38394964
io-classes 1.11.0.0 component: lib:si-timers # 4eade525
io-classes 1.11.0.0 component: lib:testlib   # 68ffc9e8
io-classes 1.11.0.0 component: lib:strict-mvar # 9f0014f7
io-classes 1.11.0.0 component: lib:strict-stm  # a525cffd
io-classes 1.11.0.0 component: lib:mtl         # b99a1fa7
io-sim 1.11.0.0 component: lib               # 49bec4e6
```

   The `dbos-transact-hs` lib component's `depends:` includes exactly
   `-clsss-1.11.0.0-38394964` (main), `-clsss-1.11.0.0-b99a1fa7` (mtl),
   `-clsss-1.11.0.0-4eade525` (si-timers),
   `-clsss-1.11.0.0-a525cffd` (strict-stm) and `-sm-1.11.0.0-49bec4e6`
   (io-sim). No component in the plan references `9f0014f7` (strict-mvar) or
   `68ffc9e8`/`cc28e6da`/etc. (testlib); those units are store/plan
   leftovers, not repo dependencies.

3. Upstream cabal (`tar -xzf ~/.cabal/packages/hackage.haskell.org/io-classes/1.11.0.0/io-classes-1.11.0.0.tar.gz -C /var/folders/.../opencode/ioclasses-audit`):
   `library` at `io-classes.cabal:64-103` (21 modules, deps at `:93-100`),
   `library strict-stm` at `:107-130`, `library strict-mvar` at `:132-143`,
   `library si-timers` at `:145-163`, `library mtl` at `:165-187`, `library
   testlib` at `:189-201`. Sublibrary summary is also in the package
   description, `io-classes.cabal:21-26`, and in `README.md` ("We provide
   also non-standard extensions of this API in **sublibraries**"). No other
   `library <name>` stanzas exist. `io-sim-1.11.0.0/io-sim.cabal:67-69`:
   `io-classes:{io-classes,strict-stm,si-timers} ^>=1.11`.

4. Repo greps (all zero for sublibrary modules):

```
$ rg -n 'Control\.Monad\.Class\.MonadTimer\.SI|Control\.Monad\.Class\.MonadTime\.SI|Control\.Concurrent\.Class\.MonadSTM\.Strict|Control\.Concurrent\.Class\.MonadMVar\.Strict|Control\.Monad\.Class\.MonadTimer\.Trans|Control\.Monad\.Class\.MonadSTM\.Trans|Control\.Monad\.Class\.Trans' src/ test/ app/
NO HITS

$ rg -n 'MonadThrow|MonadAsync|MonadFork|MonadTime|MonadCatch|MonadMask|MonadEventlog|MonadST\b|MonadUnique' src/ test/ app/
test/DBOS/SimTest.hs:17:import Control.Monad.Catch (MonadThrow (..), try)
test/DBOS/SimTest.hs:53:  (MonadSTM m, MonadSay m, MonadThrow m) =>
src/DBOS/SystemDB/Retry.hs:24:import Control.Monad.Class.MonadTimer (MonadDelay, threadDelay)
```

   (The `MonadThrow` hits are the `exceptions` package, not io-classes.)
