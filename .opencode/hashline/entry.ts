/**
 * OpenCode V2 entrypoint for hashline.
 *
 * V1 plugins return a hook object from a default-exported function; V2 plugins
 * default-export `{ id, setup }` and register hooks through the context. This
 * adapter keeps the hashline core (`runHashlineRead`,
 * `runHashlineOperationsDetailed`, shared config/cache helpers) untouched and
 * only re-hosts the five V1 hooks on their V2 domains:
 *
 *   tool.definition                      -> ctx.tool.transform
 *   tool.execute.before                  -> ctx.tool.hook("execute.before")
 *   tool.execute.after                   -> ctx.tool.hook("execute.after")
 *   experimental.chat.system.transform   -> ctx.session.hook("context")
 *   chat.message                         -> ctx.session.hook("prompt")
 *
 * It deliberately does not import `@opencode/plugin`: OpenCode's compiled
 * binary cannot resolve that bare specifier from local plugin files, and
 * `Plugin.define` is only a typing helper (identity at runtime). The loader
 * validates the plain `{ id, setup }` shape.
 *
 * V2 shape differences this adapter owns:
 *   - Native read/edit/write take `path` (V1 used `filePath`); routing
 *     normalizes aliases into `path` and drops them, since V2 tool inputs are
 *     schema-validated.
 *   - Tool hook events expose a mutable `input` (execute.before) and a mutable
 *     `result` (execute.after, status "completed").
 *   - Session context `system` is `Array<{type:"text", text}>`, not `string[]`.
 *   - Prompt file attachments are `{uri, name?, description?}` with no
 *     `content` field; annotation rewrites `uri` to the annotated temp file,
 *     which OpenCode reads during attachment resolution.
 */

import { promises as fs } from "node:fs"
import path from "node:path"
import { fileURLToPath, pathToFileURL } from "node:url"
import { runHashlineRead } from "../.opencode/lib/hashline-core"
import {
  buildCacheEntryKey,
  buildHashlineSystemInstruction,
  DEFAULT_PREFIX,
  extractPathFromToolArgs,
  formatWithRuntimeConfig,
  getByteLength,
  HashlineAnnotationCache,
  resolveHashlineConfig,
  shouldExclude,
} from "../.opencode/plugins/hashline-shared"
import {
  getCanonicalPath,
  invalidateFileCache,
  isFileEditTool,
  isFileReadTool,
  isNativeEditTool,
  normalizeHashlineInstructionEntry,
  stripNestedHashes,
  translateHashlineEditArgs,
  writeAnnotatedTempFile,
} from "../.opencode/plugins/hashline-hooks"

const HASHLINE_SYSTEM_INSTRUCTION_MARKER_RE = /<!--[\s]*hashline-instruction-v\d+[\s]*-->/i

// ---------------------------------------------------------------------------
// Argument routing (V2 canonical field: `path`)
// ---------------------------------------------------------------------------

const PATH_ALIASES = ["filePath", "file_path", "file"]

function firstString(...values: unknown[]): string | undefined {
  for (const value of values) {
    if (typeof value === "string" && value.length > 0) {
      return value
    }
  }

  return undefined
}

function normalizePathField(args: Record<string, unknown>): Record<string, unknown> {
  const out = { ...args }

  if (typeof out.path !== "string") {
    const candidate = firstString(...PATH_ALIASES.map((key) => out[key]))
    if (candidate) {
      out.path = candidate
    }
  }

  if (typeof out.path === "string") {
    for (const key of PATH_ALIASES) {
      delete out[key]
    }
  }

  return out
}

function normalizeArgs(toolName: string, args: Record<string, unknown>): Record<string, unknown> {
  let out = { ...args }

  if (toolName === "read" || toolName === "edit" || toolName === "write") {
    out = normalizePathField(out)
  }

  if (toolName === "edit") {
    if (typeof out.start_ref === "string" && typeof out.startRef !== "string") out.startRef = out.start_ref
    if (typeof out.end_ref === "string" && typeof out.endRef !== "string") out.endRef = out.end_ref
    if (typeof out.safe_reapply === "boolean" && typeof out.safeReapply !== "boolean") out.safeReapply = out.safe_reapply
    if (typeof out.expected_file_hash === "string" && typeof out.expectedFileHash !== "string") out.expectedFileHash = out.expected_file_hash
    if (typeof out.file_rev === "string" && typeof out.fileRev !== "string") out.fileRev = out.file_rev
    if (typeof out.dry_run === "boolean" && typeof out.dryRun !== "boolean") out.dryRun = out.dry_run
  }

  if (toolName === "patch") {
    if (typeof out.patch_text === "string" && typeof out.patchText !== "string") out.patchText = out.patch_text
    if (typeof out.file_path === "string" && typeof out.path !== "string") out.path = out.file_path
    if (typeof out.expected_file_hash === "string" && typeof out.expectedFileHash !== "string") out.expectedFileHash = out.expected_file_hash
    if (typeof out.file_rev === "string" && typeof out.fileRev !== "string") out.fileRev = out.file_rev
    if (typeof out.dry_run === "boolean" && typeof out.dryRun !== "boolean") out.dryRun = out.dry_run
  }

  return out
}

function normalizeName(name: string): string {
  return name === "view" ? "read" : name
}

// ---------------------------------------------------------------------------
// Widened native edit schema
// ---------------------------------------------------------------------------
//
// V2 validates model-supplied tool input against the tool's declared schema
// before `execute.before` runs, so ref-shaped edit args (`operations`,
// `startRef`, `fileRev`, ...) must be accepted by the schema for the hook to
// ever see them. `tool.transform` may replace a tool's schema; the hook then
// translates the ref shape into the native `{path, oldString, newString}`
// shape the edit executor expects.

const HASHLINE_OPERATION_SCHEMA = {
  type: "object",
  properties: {
    op: { type: "string" },
    ref: { type: "string" },
    startRef: { type: "string" },
    endRef: { type: "string" },
    content: { type: "string" },
    replacement: { type: "string" },
  },
  required: ["op"],
  additionalProperties: false,
}

const EDIT_INPUT_SCHEMA = {
  type: "object",
  properties: {
    path: { type: "string", description: "File to edit" },
    oldString: { type: "string" },
    newString: { type: "string" },
    replaceAll: { type: "boolean" },
    operations: { type: "array", items: HASHLINE_OPERATION_SCHEMA },
    fileRev: { type: "string", description: "REV token from the latest read of this file" },
    expectedFileHash: { type: "string", description: "file_hash from the latest read of this file" },
    safeReapply: { type: "boolean" },
    operation: { type: "string", description: "Single-operation mode op name" },
    startRef: { type: "string", description: "Ref copied from read" },
    endRef: { type: "string" },
    replacement: { type: "string" },
  },
  required: ["path"],
  additionalProperties: true,
}

// ---------------------------------------------------------------------------
// Read result content (string or content blocks)
// ---------------------------------------------------------------------------

function readResultText(result: unknown): string | undefined {
  const content = (result as { content?: unknown } | undefined)?.content
  if (typeof content === "string") {
    return content
  }

  if (Array.isArray(content)) {
    const block = content.find(
      (entry) => entry && typeof entry === "object" && (entry as { type?: string }).type === "text",
    ) as { text?: unknown } | undefined
    return typeof block?.text === "string" ? block.text : undefined
  }

  return undefined
}

function setReadResultText(result: unknown, text: string): boolean {
  if (!result || typeof result !== "object") {
    return false
  }

  const holder = result as { content?: unknown }
  if (typeof holder.content === "string") {
    holder.content = text
    return true
  }

  if (Array.isArray(holder.content)) {
    const block = holder.content.find(
      (entry) => entry && typeof entry === "object" && (entry as { type?: string }).type === "text",
    ) as { text?: unknown } | undefined
    if (block) {
      block.text = text
      return true
    }
  }

  return false
}

// ---------------------------------------------------------------------------
// System instruction over V2 SystemPart entries
// ---------------------------------------------------------------------------

interface SystemPartLike {
  type: "text"
  text: string
  [key: string]: unknown
}

function updateSystemParts(parts: SystemPartLike[], instruction: string): SystemPartLike[] {
  const nextParts: SystemPartLike[] = []
  let insertedInstruction = false

  for (const part of parts) {
    if (typeof part?.text !== "string" || !HASHLINE_SYSTEM_INSTRUCTION_MARKER_RE.test(part.text)) {
      nextParts.push(part)
      continue
    }

    if (!insertedInstruction) {
      nextParts.push({ ...part, text: normalizeHashlineInstructionEntry(part.text, instruction, true) })
      insertedInstruction = true
      continue
    }

    const cleaned = normalizeHashlineInstructionEntry(part.text, instruction, false)
    if (cleaned.trim().length > 0) {
      nextParts.push({ ...part, text: cleaned })
    }
  }

  if (!insertedInstruction) {
    nextParts.push({ type: "text", text: instruction })
  }

  return nextParts
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

const TOOL_DESCRIPTIONS: Record<string, string> = {
  read: `Hashline: Returns canonical ${DEFAULT_PREFIX} refs plus a REV token. Copy refs exactly from the output, then plan all same-file changes before calling edit.`,
  edit: `Hashline: Accepts refs copied from read. Prefer one batched call per file with { path, fileRev?, operations:[{ op, ref|startRef/endRef, content? }] } instead of many single edits.`,
  write: `Hashline: Use write for new files or full rewrites. Prefer edit for targeted existing-file changes; hashline prefixes inside content are stripped automatically.`,
  patch: `Hashline: Compatibility path only. Prefer read -> one batched edit per file for a faster, lower-read workflow.`,
}

export default {
  id: "angdrew.hashline",
  async setup(ctx: any) {
    const directory = ctx.location.directory
    const config = resolveHashlineConfig(directory)
    const cache = new HashlineAnnotationCache(config.cacheSize ?? 128)
    const context = { directory }

    await ctx.tool.transform((editor) => {
      for (const [id, suffix] of Object.entries(TOOL_DESCRIPTIONS)) {
        editor.update(id, (tool) => {
          if (typeof tool.description === "string" && !tool.description.includes(suffix)) {
            tool.description = `${tool.description}\n\n${suffix}`
          }
          if (id === "edit") {
            tool.input = EDIT_INPUT_SCHEMA
          }
        })
      }
    })

    await ctx.tool.hook("execute.before", async (event) => {
      const name = normalizeName(event.tool)
      if (!["read", "edit", "write", "patch"].includes(name)) {
        return
      }

      let args = normalizeArgs(name, (event.input ?? {}) as Record<string, unknown>)

      if (isFileEditTool(name)) {
        args = stripNestedHashes(args, config.prefix) as Record<string, unknown>

        if (isNativeEditTool(name)) {
          const translated = await translateHashlineEditArgs(args, context, config)
          if (translated) {
            // V2 native edit takes `path`, not `filePath`.
            const { filePath, oldString, newString } = translated
            event.input = { path: filePath, oldString, newString }
            return
          }
        }
      }

      event.input = args
    })

    await ctx.tool.hook("execute.after", async (event) => {
      const args = (event.input ?? {}) as Record<string, unknown>

      if (isFileEditTool(event.tool)) {
        invalidateFileCache(cache, args, context)
      }

      if (!isFileReadTool(event.tool, args) || event.status !== "completed") {
        return
      }

      const source = readResultText(event.result)
      if (typeof source !== "string" || source.includes("<type>directory</type>")) {
        return
      }

      const filePathFromArgs = extractPathFromToolArgs(args)
      if (typeof filePathFromArgs !== "string") {
        return
      }

      if (shouldExclude(filePathFromArgs, config.exclude)) {
        return
      }

      const canonicalPath = getCanonicalPath(filePathFromArgs, context)
      const offset = typeof args.offset === "number" ? args.offset : undefined
      const limit = typeof args.limit === "number" ? args.limit : undefined
      const cacheKey = buildCacheEntryKey(canonicalPath, offset, limit)

      const cached = cache.get(cacheKey, source)
      if (cached) {
        setReadResultText(event.result, cached)
        return
      }

      try {
        const annotated = await runHashlineRead({
          filePath: filePathFromArgs,
          offset,
          limit,
          context,
        })

        if (typeof annotated !== "string") {
          return
        }

        if (config.maxFileSize > 0 && getByteLength(annotated) > config.maxFileSize) {
          return
        }

        cache.set(cacheKey, source, annotated)
        setReadResultText(event.result, annotated)
      } catch {
        return
      }
    })

    await ctx.session.hook("context", (event) => {
      const instruction = buildHashlineSystemInstruction(config)
      const parts = Array.isArray(event.system) ? (event.system as unknown as SystemPartLike[]) : []
      event.system = updateSystemParts(parts, instruction) as typeof event.system
    })

    await ctx.session.hook("prompt", async (event) => {
      const files = event.prompt?.files
      if (!Array.isArray(files) || files.length === 0) {
        return
      }

      for (const file of files) {
        const uri = typeof file?.uri === "string" ? file.uri : undefined
        if (!uri || !uri.startsWith("file://")) {
          continue
        }

        let absolutePath: string
        try {
          absolutePath = path.normalize(fileURLToPath(uri))
        } catch {
          continue
        }

        if (shouldExclude(absolutePath, config.exclude)) {
          continue
        }

        let source: string
        try {
          source = await fs.readFile(absolutePath, "utf8")
        } catch {
          continue
        }

        if (config.maxFileSize > 0 && getByteLength(source) > config.maxFileSize) {
          continue
        }

        const cached = cache.get(absolutePath, source)
        const annotated = cached ?? formatWithRuntimeConfig(source, config)
        if (!cached) {
          cache.set(absolutePath, source, annotated)
        }

        const tempPath = await writeAnnotatedTempFile(annotated)
        file.uri = pathToFileURL(tempPath).href
      }
    })
  },
}
