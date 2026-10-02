/**
 * Smoke test for the project-local hashline V2 plugin.
 *
 * Drives the registered hooks with synthetic V2 events and asserts the
 * read -> edit round trip, cache replay, prefix stripping, system
 * instruction idempotency, and prompt attachment rewriting.
 *
 *   node .opencode/hashline/smoke.mjs
 */

import assert from "node:assert/strict"
import { mkdtemp, readFile, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { fileURLToPath } from "node:url"

const plugin = (await import(new URL("../plugins/hashline.js", import.meta.url))).default

// --- fake plugin context ---------------------------------------------------

const transforms = {}
const toolHooks = {}
const sessionHooks = {}

const ctx = {
  location: { directory: process.cwd() },
  tool: {
    transform: async (callback) => {
      callback({
        update: (id, update) => {
          const tool = { id, description: "base description", input: {} }
          update(tool)
          transforms[id] = tool
        },
      })
      return { dispose: async () => {} }
    },
    hook: async (name, callback) => {
      toolHooks[name] = callback
      return { dispose: async () => {} }
    },
  },
  session: {
    hook: async (name, callback) => {
      sessionHooks[name] = callback
      return { dispose: async () => {} }
    },
  },
}

await plugin.setup(ctx)

assert.equal(plugin.id, "angdrew.hashline")
assert.ok(transforms.read.description.includes("#HL"), "read description mentions #HL")
assert.ok(transforms.edit.description.includes("batched"), "edit description nudges batching")
assert.ok(transforms.edit.input.properties.operations, "edit schema widened for ref operations")
assert.ok(typeof toolHooks["execute.before"] === "function", "execute.before registered")
assert.ok(typeof toolHooks["execute.after"] === "function", "execute.after registered")
assert.ok(typeof sessionHooks.context === "function", "context hook registered")
assert.ok(typeof sessionHooks.prompt === "function", "prompt hook registered")

// --- read annotation -------------------------------------------------------

const dir = await mkdtemp(path.join(tmpdir(), "hashline-smoke-"))
const filePath = path.join(dir, "example.ts")
const source = ["const x = 1", "return x", ""].join("\n")
await writeFile(filePath, source, "utf8")

const readEvent = {
  tool: "read",
  input: { path: filePath },
  status: "completed",
  result: { content: source },
}
await toolHooks["execute.after"](readEvent)

assert.ok(readEvent.result.content.includes("<hashline-file"), "read output wrapped")
assert.ok(/#HL REV:[A-F0-9]{8}/.test(readEvent.result.content), "REV emitted")
const refLine = readEvent.result.content.split("\n").find((line) => /#HL 2#[A-F0-9]+#/.test(line))
assert.ok(refLine, "second line annotated with a ref")
const startRef = refLine.slice(0, refLine.indexOf("|")).trim()
assert.match(startRef, /^#HL 2#/)

// --- read cache replay -----------------------------------------------------

const cachedEvent = {
  tool: "read",
  input: { path: filePath },
  status: "completed",
  result: { content: source },
}
await toolHooks["execute.after"](cachedEvent)
assert.equal(cachedEvent.result.content, readEvent.result.content, "cache replays the same annotation")

// --- edit translation ------------------------------------------------------

const editEvent = {
  tool: "edit",
  input: { path: filePath, operation: "replace", startRef, replacement: "return x + 1" },
}
await toolHooks["execute.before"](editEvent)

assert.equal(editEvent.input.path, filePath)
assert.equal(editEvent.input.filePath, undefined, "V1 filePath alias never reaches the tool")
assert.equal(editEvent.input.oldString, source, "oldString is the whole pre-edit file")
assert.equal(editEvent.input.newString, source.replace("return x", "return x + 1"), "newString is the whole post-edit file")

// --- plain native edit passes through --------------------------------------

const nativeEdit = {
  tool: "edit",
  input: { filePath: filePath, oldString: "const x = 1", newString: "const x = 2" },
}
await toolHooks["execute.before"](nativeEdit)
assert.deepEqual(nativeEdit.input, {
  path: filePath,
  oldString: "const x = 1",
  newString: "const x = 2",
})

// --- hashline prefixes stripped from write content -------------------------

const writeEvent = {
  tool: "write",
  input: { path: path.join(dir, "new.ts"), content: `#HL 1#AAA#BBB|const y = 2\n` },
}
await toolHooks["execute.before"](writeEvent)
assert.ok(!writeEvent.input.content.includes("#HL"), "hashline prefix stripped from write content")

// --- system instruction idempotency ----------------------------------------

const contextEvent = { system: [] }
await sessionHooks.context(contextEvent)
assert.equal(contextEvent.system.length, 1, "instruction appended once")
assert.ok(contextEvent.system[0].text.includes("hashline-instruction-v1"))

await sessionHooks.context(contextEvent)
assert.equal(contextEvent.system.length, 1, "instruction not duplicated on replay")
assert.ok(contextEvent.system[0].text.includes("hashline-instruction-v1"))

// --- prompt attachment rewriting -------------------------------------------

const promptEvent = { prompt: { files: [{ uri: `file://${filePath}`, name: "example.ts" }] } }
await sessionHooks.prompt(promptEvent)

const rewritten = promptEvent.prompt.files[0].uri
assert.notEqual(rewritten, `file://${filePath}`, "attachment uri rewritten")
const rewrittenPath = fileURLToPath(rewritten)
const rewrittenText = await readFile(rewrittenPath, "utf8")
assert.ok(/#HL REV:[A-F0-9]{8}/.test(rewrittenText), "attachment temp file carries the REV line")
assert.ok(/#HL 1#[A-F0-9]+#[A-F0-9]+\|/.test(rewrittenText), "attachment temp file carries refs")

console.log("hashline V2 smoke: all assertions passed")
