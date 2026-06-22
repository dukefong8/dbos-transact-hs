# Bluefin 0.7.0.0 — Research Context for Design

> Handoff document for an agent designing a Haskell library that may use or integrate with Bluefin.
> Source: https://hackage.haskell.org/package/bluefin-0.7.0.0/docs/
> Fetched: 2026-06-21

---

## 1. What Bluefin Is

Bluefin is an **analytic effect system** for Haskell. "Analytic" means effects take place in a monad (`Eff`) that is a lightweight wrapper around `IO`, with a phantom type parameter to track effects.

```
newtype Eff (es :: Effects) a = UnsafeMkEff (IO a)
```

This gives Bluefin **predictable performance** (no fragile inlining required) and **resource safety** (bracketing works naturally, unlike synthetic systems like `fused-effects`/`polysemy`).

### Comparison with other systems

| Property | IO | ST | MTL/fused-effects/Polysemy | Bluefin/effectful |
|---|---|---|---|---|
| Mixing effects | ✅ | ❌ (state only) | ✅ | ✅ |
| Fine-grained effects | ❌ | ✅ (state only) | ✅ | ✅ |
| Encapsulation | ❌ | ✅ | ✅ | ✅ |
| Resource safety | ✅ | ❌ | ❌ | ✅ |
| Predictable perf | ✅ | ✅ | ❌ | ✅ |
| Multishot continuations | ❌ | ❌ | ✅ | ❌ |

---

## 2. Core Concept: Value-Level Capabilities

Bluefin's distinctive design: **effects are accessed through value-level capabilities** passed as function arguments, not through type-level constraints alone.

```haskell
example1 :: Int -> Int
example1 n = runPureEff $
  evalModify n $ \sn -> do
    n' <- get sn
    when (n' < 10) $ modify sn (+ 10)
    get sn
```

- `evalModify n $ \sn -> ...` introduces a state capability `sn` with initial value `n`
- `sn` is a **first-class value** — multiple effects of the same type are trivially disambiguated by different variable names

### Key insight for library design

Because capabilities are value-level, you can have **two `Modify Int` effects in scope simultaneously** and disambiguate by which value you pass:

```haskell
example2 (m, n) = runPureEff $
  evalModify m $ \sm ->
    evalModify n $ \sn -> do
      n' <- get sn
      m' <- get sm
      if n' < m'
        then modify sn (+ 10)
        else modify sm (+ 10)
      ...
```

This is much simpler than type-level disambiguation in MTL or other effect systems.

### Relationship to the Handle Pattern

Bluefin's value-level capabilities are a well-typed implementation of the [Handle Pattern](https://jaspervdj.be/posts/2018-03-08-handle-pattern.html). In the Handle Pattern, dependencies are passed as explicit function arguments (handles). Bluefin lifts this to the effect level:

| Handle Pattern | Bluefin equivalent |
|---|---|
| `data Logger = Logger { logMsg :: String -> IO () }` | `type Logger e = capability` (wraps `IOE`) |
| `writeUserData :: Logger -> IO ()` | `writeUserData :: IOE e -> Eff es ()` plus `<: es` |
| `withStdoutLogger :: (Logger -> IO r) -> IO r` | `runEff :: (forall e. IOE e -> Eff e a) -> IO a` |

Bluefin adds type-level effect tracking on top of the Handle Pattern, giving encapsulation (you can see from the type what effects a function may use) while keeping the simplicity of explicit value-level dependency passing.

---

## 3. The Fork-Fragility Problem (Why IO-Based Effects Are Fragile)

Haskell's ecosystem is full of "reader-like" operations implemented in `IO` using mutable references — operations like `local`, `withArgs`, logging context overrides, etc. These are **fork-fragile**: they break in the presence of concurrency. Understanding why is essential to appreciating Bluefin's design.

### Two Implementation Strategies, Two Failure Modes

**Strategy 1: Global mutable reference**

```haskell
ambientState :: IORef StateType
ambientState = unsafePerformIO (newIORef initialValue)

ask :: IO StateType
ask = readIORef ambientState

local :: (StateType -> StateType) -> IO r -> IO r
local f body = bracket_
  (modifyIORef ambientState f)
  (writeIORef ambientState orig)
  body
```

*Symptom 1:* Concurrent threads observe each other's supposedly "local" modifications.

```haskell
concurrently_
  ( local f $ do ... )     -- modifies ambientState for "this thread only"
  ( do
      s <- ask             -- BUG: may see f's modification from sibling thread!
      ...
  )
```

**Strategy 2: Per-thread mutable reference**

```haskell
ambientState :: IORef (Map ThreadId StateType)

ask :: IO StateType
ask = do m <- readIORef ambientState
         t <- myThreadId
         pure (fromJust (Map.lookup t m))

local :: (StateType -> StateType) -> IO r -> IO r
local f body = bracket_
  (modifyIORef' ambientState (Map.adjust f t))
  (modifyIORef' ambientState (Map.insert t orig))
  body
```

*Symptom 2:* Child threads don't inherit the parent's ambient state.

```haskell
local g $ do
  forkIO $ do
    s <- ask   -- BUG: doesn't see g's modification!
    ...
```

### The Only Exception: `mask_` / `uninterruptibleMask_`

GHC's masking operations (`mask_`, `uninterruptibleMask_`) are reader-like operations in `IO` that are **not** fork-fragile. They work because they have a special implementation in GHC's RTS. Unfortunately, this RTS mechanism is not exposed to users for defining their own scoped state.

### Catalogue of Fork-Fragile Operations in the Ecosystem

| Package | Operation | Symptom |
|---|---|---|
| `base` | `withArgs`, `withProgName` | Symptom 1 (global argv table) |
| `QuickCheck`, `hspec-core` | `withBuffering`, `withLineBuffering` | Symptom 1 (global Handle state) |
| `with-utf8` | `withUtf8`, `withStdTerminalHandles` | Symptom 1 (global Handle state) |
| `context` | `adjust` (per-thread `Store`) | Symptom 2 (child threads) |
| `hs-opentelemetry-api` | `inSpan` (per-thread span stack) | Symptom 2 (child threads) |
| `heavy-logger` | `withLoggingIO` | Symptom 2 (child threads) |
| `logging`, `simple-logger` | `withStdoutLogging`, `withGlobalLogging` | Symptom 1 (global ref) |
| `io-storage` | `withStore` | Symptom 1 (global ref) |

### How Bluefin Avoids Both Symptoms

Bluefin avoids the fork-fragility problem **entirely** because effects are not implemented via global or per-thread mutable state. Instead:

1. **Effects are scoped by the type system**: A capability's effect tag `e` is existentially quantified and cannot escape its handler's scope. This is the same trick `ST` uses.
2. **No implicit state**: The state of an effect (e.g., the current value of a `Modify` capability) is stored in the capability value itself (which wraps an `IORef`), and the capability's scope is enforced by the type system.
3. **`local` is typed and explicit**: In `Bluefin.Capability.Ask`, `local` takes a `Reader r e1` argument — you can't accidentally use the wrong capability, and there's no global mutable state to race on.
4. **Handlers remove effects**: Once a handler has run, the effect is gone — both at the type level and at runtime. There's no lingering global state.

In short: analytic effect systems like Bluefin achieve what `IOScopedRef` promises (scoped, fork-safe mutable state) through the type system rather than through new RTS primitives.

---

## 4. IOScopedRef: Haskell's Missing Mutable Reference Type

Tom Ellis has proposed a new mutable reference type for Haskell called `IOScopedRef` ([GHC proposal PR #751](https://github.com/ghc-proposals/ghc-proposals/pull/751)). It fills the gap between `IORef` (global, visible everywhere) and Bluefin-style typed effect tracking.

### The Core API

```haskell
type IOScopedRef :: Type -> Type

-- Create a scoped reference with an initial value
withIOScopedRef :: a -> (IOScopedRef a -> IO r) -> IO r

-- Read the current value (scoped to the thread+block)
readIOScopedRef :: IOScopedRef a -> IO a

-- Modify within a scope; the old value is restored after
modifyIOScopedRef :: (a -> a) -> IOScopedRef a -> IO r -> IO r
```

### IORef vs IOScopedRef

| Property | `IORef` | `IOScopedRef` |
|---|---|---|
| Modifications visible outside scope | ✅ Yes — escape the scope | ❌ No — restored on exit |
| Exception-safe restoration | ❌ Must use `bracket` manually | ✅ Guaranteed by implementation |
| Thread-local modifications | ❌ Visible to other threads | ✅ Invisible to other threads |
| Child thread inheritance | ✅ Inherits current value | ✅ Inherits current value |
| Implementation in GHC | ✅ Already exists | ❌ Requires new RTS primitive |

The critical difference: `IOScopedRef` is like `ReaderT`'s `local`/`runReaderT` but in `IO`. You can't implement it with `IORef` because:
- `bracket`-based save/restore has Symptom 1 (other threads observe the modification during the body)
- Per-thread Map-based approaches have Symptom 2 (child threads don't inherit)

### Relationship to Bluefin

| IOScopedRef concept | Bluefin equivalent |
|---|---|
| `withIOScopedRef :: a -> (IOScopedRef a -> IO r) -> IO r` | `evalModify :: s -> (forall e. Modify s e -> Eff (e :& es) a) -> Eff es a` |
| `readIOScopedRef :: IOScopedRef a -> IO a` | `get :: e <: es => State s e -> Eff es s` |
| `modifyIOScopedRef :: (a -> a) -> IOScopedRef a -> IO r -> IO r` | `local :: ... => Reader r e1 -> (r -> r) -> Eff es a -> Eff es a` |
| `withIOScopedRef` + `modifyIOScopedRef` nesting | `evalModify` mutual nesting (same type) |

Bluefin achieves what `IOScopedRef` promises, but:
- **Bluefin doesn't need new RTS primitives** — the type system enforces scoping
- **Bluefin is more general** — you get exceptions, IO, streams, etc. alongside scoped state
- **Bluefin is opt-in** — you must explicitly pass capabilities; `IOScopedRef` would be implicit

The `context` package provides a partial implementation of `IOScopedRef` (with Symptom 2: fork-fragility). Bluefin's `Ask`/`Reader` capability is a safer alternative.

---

## 5. Core Types

### `Eff` monad

```haskell
data Eff (es :: Effects) a
```

- `es` is a phantom type parameter tracking which **unhandled effects** remain
- Kind `Effects` is a type-level set of effect tags
- Operations are performed within `Eff`

### `Effects` kind

```haskell
data Effects  -- each inhabitant is a set of effect tags
```

### Subset constraint

```haskell
class (es1 :: Effects) :> (es2 :: Effects)
type (<:) = (:>)  -- preferred synonym
-- e <: es  means "e is a subset of es" (effect e is allowed in effect set es)
```

### Union type operator

```haskell
type (:&) = 'Union  -- infixr 9
-- e :& es  = union of effect tag e with effect set es
```

Handler type signatures follow this pattern:

```haskell
(e1 <: es, e2 <: es, ...) => Capability1 e1 -> Capability2 e2 -> ... -> Eff es r
```

Handler signatures:

```haskell
-- handler introduces a fresh effect tag e, wraps its callback with e :& es
(forall e. Capability e -> Eff (e :& es) a) -> Eff es r
```

---

## 6. Running `Eff`

### Pure execution (no unhandled effects)

```haskell
runPureEff :: (forall es. Eff es a) -> a
```

### IO execution

```haskell
runEff :: (forall e. IOE e -> Eff e a) -> IO a
```

`runEff` removes the `Eff` wrapper entirely, returning `IO a`. The callback receives an `IOE` capability for performing IO operations.

---

## 7. Built-in Effect Capabilities

### 7.1 State / Modify

| Module (new) | Module (old, deprecated) |
|---|---|
| `Bluefin.Capability.Modify` | `Bluefin.State` |

**Capability type:**

```haskell
type Modify s e = State s e   -- State is the underlying opaque newtype
```

- `Modify s e` wraps an `IORef s`

**Handlers:**

```haskell
evalModify :: s -> (forall e. Modify s e -> Eff (e :& es) a) -> Eff es a
runModify  :: s -> (forall e. Modify s e -> Eff (e :& es) a) -> Eff es (a, s)
withModify :: s -> (forall e. Modify s e -> Eff (e :& es) a) -> Eff es a
```

- `evalModify` discards final state, returns only the result
- `runModify` returns both result and final state
- `withModify` returns the result and gives a diff function `(s -> a)` to access state after the fact

**Operations:**

```haskell
get    :: e <: es => State s e -> Eff es s
put    :: e <: es => State s e -> s -> Eff es ()
modify :: e <: es => State s e -> (s -> s) -> Eff es ()
```

`modify` forces the new value before writing (strict).

### 7.2 Exceptions / Throw

| Module (new) | Module (old, deprecated) |
|---|---|
| `Bluefin.Capability.Throw` | `Bluefin.Exception` |

**Capability type:**

```haskell
type Throw exn e = Exception exn e
```

**Handlers:**

```haskell
try    :: (forall e. Exception exn e -> Eff (e :& es) a) -> Eff es (Either exn a)
handle :: (exn -> Eff es a) -> (forall e. Exception exn e -> Eff (e :& es) a) -> Eff es a
catch  :: (forall e. Exception exn e -> Eff (e :& es) a) -> (exn -> Eff es a) -> Eff es a
```

- Every Bluefin exception **must** be handled — unhandled exceptions are impossible
- An exception is handled at exactly one place (the handler that introduced the capability)
- This differs from Haskell's normal exceptions (which can be caught by any matching handler on the stack)

**Operations:**

```haskell
throw     :: e <: es => Exception exn e -> exn -> Eff es a
rethrowIO :: (e1 <: es, e2 <: es, Exception ex) =>
             IOE e1 -> Exception ex e2 -> Eff es r -> Eff es r
```

`rethrowIO` wraps an IO action, catching `IOException` and rethrowing as a Bluefin exception.

### 7.3 IO

| Module (new) | Module (old, deprecated) |
|---|---|
| `Bluefin.IO` | — |

**Capability type:**

```haskell
data IOE (e :: Effects)
```

**Handler:**

```haskell
runEff :: (forall e. IOE e -> Eff e a) -> IO a
```

**Operations:**

```haskell
effIO     :: e <: es => IOE e -> IO a -> Eff es a
rethrowIO :: (e1 <: es, e2 <: es, Exception ex) =>
              IOE e1 -> Exception ex e2 -> Eff es r -> Eff es r
```

**Bridging to MonadIO/standard Haskell:**

```haskell
withMonadIO :: e <: es => IOE e -> (forall m. MonadIO m => m r) -> Eff es r
```

`withMonadIO` allows running `MonadIO`-based code inside `Eff`.

**EffReader** (for `IO`-related instances):

```haskell
data EffReader r (es :: Effects) a
effReader     :: (r -> Eff es a) -> EffReader r es a
runEffReader  :: r -> EffReader r es a -> Eff es a
```

### 7.4 Reader / Ask

| Module (new) | Module (old, deprecated) |
|---|---|
| `Bluefin.Capability.Ask` | `Bluefin.Reader` |

**Capability type:**

```haskell
type Ask r e = Reader r e
```

**Handler:**

```haskell
runAsk :: r -> (forall e. Ask r e -> Eff (e :& es) a) -> Eff es a
```

**Operations:**

```haskell
ask   :: e <: es => Reader r e -> Eff es r
asks  :: e <: es => Reader r e -> (r -> a) -> Eff es a
local :: e1 <: es => Reader r e1 -> (r -> r) -> Eff es a -> Eff es a
```

`local` restores the original value on exit (normal or exception). `ask` is referentially transparent — two `ask`s in sequence always return the same value.

**Why Bluefin's `local` is fork-safe (and IO's isn't):**

In `IO`, implementing `local` requires either:
- A **global `IORef`** → other threads observe modifications (Symptom 1)
- A **per-thread `IORef`** → child threads don't inherit value (Symptom 2)

Bluefin's `local` avoids both because:
1. The `Reader` capability is a **value-level handle**, not a global reference — there's no shared mutable state to race on
2. The type system enforces that the capability cannot escape its handler's scope
3. When used within Bluefin's `Eff` monad, `local` naturally scopes modifications to the `Eff` computation, and forking (via `effIO` + `forkIO`) is explicit about what state gets shared

**Comparison with the `context` package:**

The `context` package provides `Store ctx` / `adjust` / `mine` which is a partial implementation of `IOScopedRef`. It suffers from Symptom 2 (fork-fragility): child threads don't inherit scoped modifications unless you use its custom `Context.Concurrent` thread-creation functions. Bluefin's `Ask` capability solves the same problem (scoped ambient state) without fork-fragility and without needing custom thread creation — at the cost of requiring explicit capability passing.

| Feature | `context`'s `Store` | Bluefin's `Ask` |
|---|---|---|
| Read ambient state | `mine store` | `ask reader` |
| Modify within scope | `adjust store f` | `local reader f` |
| Fork-safe? | ❌ (needs custom `concurrently`) | ✅ (type-safe by construction) |
| Requires explicit threading lib | ✅ `Context.Concurrent` | ❌ No special threading needed |
| Type tracks effects? | ❌ | ✅ (via `Eff es`) |

**`HandleReader`** (for passing around a handle as a context):

```haskell
data HandleReader h (e :: Effects)
runHandleReader :: h e -> (HandleReader h e -> Eff es a) -> Eff es a
asksHandle     :: HandleReader h e -> (h e -> Eff es a) -> Eff es a
```

### 7.5 Writer / Tell

| Module (new) | Module (old, deprecated) |
|---|---|
| `Bluefin.Capability.Tell` | `Bluefin.Writer` |

**Capability type:**

```haskell
type Tell w e = Writer w e
```

**Handlers:**

```haskell
runTell :: (forall e. Tell w e -> Eff (e :& es) a) -> Eff es (a, w)
execTell :: Monoid w => (forall e. Tell w e -> Eff (e :& es) a) -> Eff es w
```

**Operations:**

```haskell
tell :: e <: es => Writer w e -> w -> Eff es ()
```

### 7.6 Jump (early return)

| Module (new) | Module (old, deprecated) |
|---|---|
| `Bluefin.Capability.JumpTo` | `Bluefin.Jump` |

**Capability type:**

```haskell
type Jump e   = EarlyReturn () e  -- old (Bluefin.Jump)
type JumpTo e = EarlyReturn () e  -- new (Bluefin.Capability.JumpTo)
```

**Handler:**

```haskell
withJump    :: (forall e. Jump e -> Eff (e :& es) ()) -> Eff es ()
withJumpTo  :: (forall e. JumpTo e -> Eff (e :& es) a) -> Eff es a
```

**Operations:**

```haskell
jumpTo :: e <: es => JumpTo e -> Eff es a
```

`JumpTo` is equivalent to an untyped early return — it's an exception of type `()`.

### 7.7 Stream / Yield

| Module (new) | Module (old, deprecated) |
|---|---|
| `Bluefin.Capability.Yield` | `Bluefin.Stream` |

**Capability type:**

```haskell
type Yield a e = Stream a e  -- which is Coroutine a () e
```

**Handlers:**

```haskell
yieldToList        :: (forall e. Stream a e -> Eff (e :& es) r) -> Eff es ([a], r)
yieldToReverseList :: (forall e. Stream a e -> Eff (e :& es) r) -> Eff es ([a], r)
withYieldToList    :: (forall e. Stream a e -> Eff (e :& es) ([a] -> r)) -> Eff es r
ignoreYield        :: (forall e. Stream a e -> Eff (e :& es) r) -> Eff es r  -- renamed from ignoreStream
```

**Operation:**

```haskell
yield :: e1 <: es => Stream a e1 -> a -> Eff es ()
```

**Stream combinators:**

```haskell
forEach      :: (forall e1. Coroutine a b e1 -> Eff (e1 :& es) r) -> (a -> Eff es b) -> Eff es r
enumerate    :: ... -> Stream (Int, a) e2 -> Eff es r
enumerateFrom :: Int -> ... -> Stream (Int, a) e2 -> Eff es r
mapMaybe     :: (a -> Maybe b) -> ... -> Stream b e2 -> Eff es r
catMaybes    :: ... -> Stream a e2 -> Eff es r
inFoldable   :: Foldable t => t a -> Stream a e1 -> Eff es ()
cycleToYield :: Foldable f => f a -> Yield a e1 -> Eff es ()
takeAwait    :: Int -> Await a e1 -> Yield a e2 -> Eff es ()
```

### 7.8 Consume / Await

| Module (new) | Module (old, deprecated) |
|---|---|
| `Bluefin.Capability.Await` | `Bluefin.Consume` |

**Capability type:**

```haskell
type Await a e = Consume a e  -- which is Coroutine () a e
```

**Operations:**

```haskell
await :: e <: es => Consume a e -> Eff es a
```

**Handlers:**

```haskell
consumeStream :: (forall e. Consume a e -> Eff (e :& es) r) -> (forall e. Stream a e -> Eff (e :& es) r) -> Eff es r
```

### 7.9 Coroutine / Request

| Module (new) | Module (old, deprecated) |
|---|---|
| `Bluefin.Capability.Request` | `Bluefin.Coroutine` |

This is the most general streaming primitive. `Request a b e` lets one yield an `a` and await a `b` in response.

```haskell
type Request a b e = Coroutine a b e

request :: e1 <: es => Request a b e1 -> a -> Eff es b

forEach :: (forall e1. Request a b e1 -> Eff (e1 :& es) r)
        -> (a -> Eff es b)
        -> Eff es r

connectRequests :: (forall e. Request a b e -> Eff (e :& es) r)
                -> (forall e. a -> Request b a e -> Eff (e :& es) r)
                -> Eff es r
```

### 7.10 ReturnEarly

| Module (new) |
|---|
| `Bluefin.Capability.ReturnEarly` |

```haskell
type ReturnEarly a e = EarlyReturn a e
withReturnEarly :: (forall e. ReturnEarly a e -> Eff (e :& es) a) -> Eff es (Maybe a)
```

---

## 8. Resource Management

Bluefin provides `bracket` directly in `Eff` (no need for `IO` bracket):

```haskell
bracket :: Eff es a                    -- acquire
        -> (a -> Eff es ())            -- release
        -> (a -> Eff es b)             -- body
        -> Eff es b

finally :: Eff es b                    -- body
        -> Eff es ()                   -- final (run regardless)
        -> Eff es b
```

Unlike synthetic effect systems, bracketing in Bluefin:
- Works predictably (inherited from `IO`)
- Is type-safe (the type parameter `es` doesn't need to contain exception/IO effects for `bracket` to work)
- Guarantees resource release even across exceptions within `Eff`

For streaming, Bluefin's `Stream`/`Yield`/`Request` computations **finalize promptly**: resources acquired via `bracket` inside a stream are released at the end of the `bracket` scope, not at the end of the resource scope. This is better resource safety than Conduit/Pipes.

---

## 9. Creating Custom Effects

### 9.1 Via `Bluefin.Compound` — The Preferred Way

`Bluefin.Compound` supports defining new effects by composing existing ones.

Key types:

```haskell
class Handle (h :: (Effects -> Type) -> Effects -> Type) where
  useImpl :: h (Eff es) -> Eff es r -> Eff es r

class OneWayCoercible h hs where
  oneWayCoercibleImpl :: h (Eff es) -> hs (Eff es)

-- A simple compound effect that gives access to another capability
newtype OneWayCoercibleHandle f (es :: Effects)
  = OneWayCoercibleHandle (f (Eff es))
```

A new effect is typically defined by:

1. Creating a newtype around an existing capability
2. Deriving `Handle` via `OneWayCoercibleHandle`
3. Providing `OneWayCoercible` instances for effect tag flexibility

### 9.2 Via GADT Effect (for `effectful`/`polysemy` users)

`Bluefin.GadtEffect` provides a `send`/`interpret` style similar to `effectful` and `polysemy`, for defining custom effects by GADT:

```haskell
data Effect (f :: (Type -> Type) -> Type -> Type)
newtype Send (f :: Effect) (e :: Effects)

send       :: e1 <: es => Send f e1 -> f (Eff es) r -> Eff es r
interpret  :: (forall x. f (Eff (e :& es)) x -> Eff es x) -> (forall e'. Send f e' -> Eff (e' :& es) r) -> Eff es r
interpose  :: (Send f es -> EffectHandler f es) -> HandleReader (Send f) e1 -> Eff es r -> Eff es r
passthrough :: ... -> Send f e1 -> f (Eff es) x -> Eff es x
```

Requires boilerplate `OneWayCoercible` and `Handle` instances (no Template Haskell available yet, but it may be contributed).

---

## 10. Streaming / Pipes

`Bluefin.Pipes` provides a full `pipes`-compatible streaming library built on Bluefin coroutines.

```haskell
data Proxy a' a b' b (e :: Effects)

type Producer   a = Proxy Void () () a
type Consumer   a = Proxy () a Void Void
type Pipe    a b = Proxy () a () b
type Effect     = Producer Void

-- Pipeline composition
yield :: ... => Proxy x1 x () a e -> a -> Eff es ()
await :: ... => Proxy () a y' y e -> Eff es a

(>->) :: ... -> Proxy a' a () b e -> Proxy () b c' c e -> Proxy a' a c' c e
(~>)  :: ... production category composition
for   :: ... monadic bind for pipes
cat   :: ... identity pipe
each  :: ... produce from Foldable
next  :: ... ()
```

---

## 11. DSL Builder (Applicative DSL construction)

`Bluefin.DslBuilder` provides an Applicative DSL for building effectful computations:

```haskell
data DslBuilder (es :: Effects) o a

pureEffRead :: Eff es o -> DslBuilder es o ()
mkEff :: ((a -> Eff es o) -> Eff es o) -> DslBuilder es o a
buildEff :: DslBuilder es o a -> Eff es o
```

`Bluefin.DslBuilderEff` integrates this into `Eff` more directly.

---

## 12. Deprecation / Module Migration

Bluefin is transitioning from old MTL-style names to capability-oriented names. New code should use the `Bluefin.Capability.*` modules.

| Old module | New module | Capability name |
|---|---|---|
| `Bluefin.Reader` | `Bluefin.Capability.Ask` | `Ask r e` |
| `Bluefin.HandleReader` | `Bluefin.Capability.AskCapability` | `AskCapability h e` |
| `Bluefin.Consume` | `Bluefin.Capability.Await` | `Await a e` |
| `Bluefin.Jump` | `Bluefin.Capability.JumpTo` | `JumpTo e` |
| `Bluefin.State` | `Bluefin.Capability.Modify` | `Modify s e` |
| `Bluefin.Coroutine` | `Bluefin.Capability.Request` | `Request a b e` |
| `Bluefin.EarlyReturn` | `Bluefin.Capability.ReturnEarly` | `ReturnEarly a e` |
| `Bluefin.Writer` | `Bluefin.Capability.Tell` | `Tell w e` |
| `Bluefin.Exception` | `Bluefin.Capability.Throw` | `Throw exn e` |
| `Bluefin.Stream` | `Bluefin.Capability.Yield` | `Yield a e` |

---

## 13. Tips for Inference

For better type inference with Bluefin, use these GHC extensions:

```haskell
{-# LANGUAGE NoMonoLocalBinds #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
```

(These can be reverted to defaults after adding inferred type signatures.)

Writing a handler often requires an explicit type signature.

---

## 14. API Surface Summary

### Key Modules

| Module | Purpose |
|---|---|
| `Bluefin.Eff` | `Eff` monad, `runPureEff`, `runEff`, `bracket`, `finally`, `Effects`, `(<:)`, `(:&)` |
| `Bluefin.IO` | `IOE`, `effIO`, `withMonadIO`, `EffReader` |
| `Bluefin.Capability.Modify` | `Modify`/`State`, `evalModify`/`runModify`, `get`/`put`/`modify` |
| `Bluefin.Capability.Throw` | `Throw`/`Exception`, `try`/`handle`/`catch`, `throw`/`rethrowIO` |
| `Bluefin.Capability.Ask` | `Ask`/`Reader`, `runAsk`, `ask`/`asks`/`local` |
| `Bluefin.Capability.Tell` | `Tell`/`Writer`, `runTell`/`execTell`, `tell` |
| `Bluefin.Capability.Yield` | `Yield`/`Stream`, `yieldToList`, `yield`, combinators |
| `Bluefin.Capability.Await` | `Await`/`Consume`, `await`, `takeAwait` |
| `Bluefin.Capability.Request` | `Request`/`Coroutine`, `request`, `connectRequests` |
| `Bluefin.Capability.JumpTo` | `Jump`/`EarlyReturn`, `withJumpTo`, `jumpTo` |
| `Bluefin.Capability.ReturnEarly` | `ReturnEarly`/`EarlyReturn`, `withReturnEarly` |
| `Bluefin.Compound` | `OneWayCoercibleHandle`, `Handle`, `OneWayCoercible` — custom effects |
| `Bluefin.GadtEffect` | `Effect`, `Send`, `interpret`, `interpose` — GADT-defined effects |
| `Bluefin.HandleReader` | `HandleReader`, `runHandleReader`, `asksHandle` |
| `Bluefin.Pipes` | `Proxy`, `Producer`, `Consumer`, `Pipe`, pipeline composition |
| `Bluefin.Pipes.Prelude` | Common pipe operations |
| `Bluefin.DslBuilder` | Applicative DSL for building `Eff` computations |
| `Bluefin.StateSource` | Alternative to `evalState` |
| `Bluefin.Exception.GeneralBracket` | Generalised bracket |
| `Bluefin.System.IO` | System-level IO wrappers |

---

## 15. Key Design Idioms

### Handler callback pattern

All handlers follow the same pattern:
1. Take initial configuration (e.g., initial state)
2. Take a callback that receives a capability
3. The callback's return type involves `Eff (e :& es) a` — the capability's effect tag is added to the effect set
4. Return `Eff es r` — the handler removes the effect from the set

```haskell
handler :: config -> (forall e. Capability e -> Eff (e :& es) a) -> Eff es r
```

### Effectful function pattern

Functions using effects follow:

```haskell
(e <: es, ...) => Capability1 e1 -> Capability2 e2 -> ... -> Eff es r
```

### Effect scoping (ST-like)

Bluefin's phantom type parameters ensure capabilities cannot escape their handler's scope, just like `ST` ensures `STRef`s cannot escape `runST`. The type error on escape attempts:

```
Couldn't match type 'e0' with 'e'
  because type variable 'e' would escape its scope
```

### Running pure code

```haskell
runPureEff :: (forall es. Eff es a) -> a
```

This uses `unsafePerformIO` internally, justified because the type system guarantees no unhandled effects remain.

---

## 16. Multishot Continuations Limitation

Bluefin does **not** support multishot continuations (like `LogicT` with `[]`). This means:

- No backtracking with multiple continuations
- No `Alternative`-style nondeterminism within `Eff`
- Coroutines are "second-class stackful coroutines" — they can suspend/resume but cannot be forked/cloned

If multishot continuations are needed, use MTL-style (e.g., `StateT Int []`) or `fused-effects`/`polysemy` instead.

---

## 17. Additional References

### Bluefin / Effect Systems

- Tom Ellis: [A History of Effect Systems](https://www.youtube.com/watch?v=RsTuy1jXQ6Y) (Zurihac 2025)
- Alexis King: [Effects for Less](https://www.youtube.com/watch?v=0jI-AlWEwYI) (Zurihac 2020) — performance of synthetic effects
- Alexis King: [Unresolved challenges of scoped effects](https://www.twitch.tv/videos/1163853841)
- Michael Snoyman: [The Tale of Two Brackets](https://academy.fpblock.com/blog/2017/06/tale-of-two-brackets/)
- Michael Snoyman: [The `ReaderT` Design Pattern](https://academy.fpblock.com/blog/2017/06/readert-design-pattern/)
- Jasper Van der Jeugt: [The Handle Pattern](https://jaspervdj.be/posts/2018-03-08-handle-pattern.html)
- Bluefin blog: [Bluefin streams finalize promptly](https://h2.jaguarpaw.co.uk/posts/bluefin-streams-finalize-promptly/)

### Scoped State / Fork-Fragility

- Tom Ellis: [Haskell's missing mutable reference type (IOScopedRef)](https://h2.jaguarpaw.co.uk/posts/haskells-missing-mutable-ref/) (June 2026) — proposes a new RTS primitive for scoped, fork-safe mutable state
- Tom Ellis: [A reference implementation of IOScopedRef](https://h2.jaguarpaw.co.uk/posts/ioscopedref-reference-implementation/)
- Tom Ellis: [Fork-fragile reader-like operations in Haskell](https://h2.jaguarpaw.co.uk/posts/fork-fragile-reader-like-operations/) (June 2026) — catalogue of fork-fragile IO operations in the ecosystem
- GHC proposal: [Scoped thread-locals](https://github.com/ghc-proposals/ghc-proposals/pull/751)
- `context` package: [hackage](https://hackage.haskell.org/package/context) — partial implementation of IOScopedRef (fork-fragile)
