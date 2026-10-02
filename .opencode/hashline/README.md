# Hashline plugin (project-local, OpenCode V2)

OpenCode V2's plugin API is a deliberate break from V1. The upstream plugin
(`AngDrew/opencode-hashline`, npm `@angdrew/opencode-hashline-plugin@1.6.6`)
default-exports a V1 function and fails to load on V2 with:

```
PluginModule.LoadError: Plugin must export a default definition with an id
and an effect or setup function.
```

This directory holds the V2 port. It keeps the upstream hashline core
(`runHashlineRead`, `runHashlineOperationsDetailed`, config/cache/format
helpers) untouched and only re-hosts the five V1 hooks on their V2 domains.

## Layout

```
.opencode/plugins/hashline.js   built bundle, auto-loaded by OpenCode V2
.opencode/hashline/entry.ts     V2 adapter source (the port)
.opencode/hashline/smoke.mjs    synthetic-event smoke test
.opencode/hashline/README.md    this file
```

Upstream-tracking checkout with the full vendored core and build setup:
`~/dev/opencode-hashline` (commit `afa01c6` adds `v2/entry.ts` and exports
the hook helpers the adapter reuses).

## V2 differences the adapter owns

| V1 hook | V2 registration |
| --- | --- |
| `tool.definition` | `ctx.tool.transform(...)` |
| `tool.execute.before` | `ctx.tool.hook("execute.before", ...)` |
| `tool.execute.after` | `ctx.tool.hook("execute.after", ...)` |
| `experimental.chat.system.transform` | `ctx.session.hook("context", ...)` |
| `chat.message` | `ctx.session.hook("prompt", ...)` |

- **Native read/edit/write take `path`** (V1 used `filePath`). Routing
  normalizes aliases into `path` and drops them, because V2 validates tool
  input against the declared schema.
- **Schema validation runs before hooks.** Ref-shaped edit args
  (`operations`, `startRef`, `fileRev`, ...) are rejected by the stock edit
  schema before `execute.before` can translate them, so the port widens the
  native `edit` schema through `ctx.tool.transform`, then translates refs into
  `{path, oldString, newString}` in the hook.
- **Tool hook events** expose a mutable `input` (`execute.before`) and a
  mutable `result` (`execute.after`, `status: "completed"`).
- **Session context `system` is `Array<{type:"text", text}>`**, not
  `string[]`; marker-based instruction dedup works on parts.
- **Prompt file attachments are `{uri, name?, description?}` with no
  `content` field.** Annotation rewrites `uri` to an annotated temp file,
  which OpenCode reads during attachment resolution.
- **Local plugin files must not import `@opencode/plugin`** (the compiled
  binary cannot resolve it); the bundle exports the plain `{id, setup}`
  object.

## Rebuild

From the upstream-tracking checkout (keeps `entry.ts` in sync with the fork):

```sh
cp .opencode/hashline/entry.ts ~/dev/opencode-hashline/v2/entry.ts
cd ~/dev/opencode-hashline
npx esbuild v2/entry.ts --bundle --format=esm --platform=node --target=node22 \
  --outfile=~/dev/dbos-transact-hs/.opencode/plugins/hashline.js
```

The bundle is self-contained (Node builtins only); committing it means no
build step is needed to use the plugin.

## Verify

```sh
node .opencode/hashline/smoke.mjs
```

Asserts the read → edit round trip, cache replay, prefix stripping, system
instruction idempotency, and prompt attachment rewriting. Live checks: a
`read` returns `#HL` refs and a `REV` line; an `edit` with `operations` +
`startRef` + `fileRev` applies; the same edit with a stale `fileRev` is
rejected with `File revision mismatch`.
