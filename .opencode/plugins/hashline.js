// v2/entry.ts
import { promises as fs3 } from "node:fs";
import path4 from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

// .opencode/lib/hashline-core.ts
import { createHash as createHash2 } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";

// .opencode/plugins/hashline-contract.ts
import { createHash } from "node:crypto";
var SMALL_LINE_HASH_LENGTH = 3;
var LARGE_LINE_HASH_LENGTH = 4;
var HASH_LENGTH_THRESHOLD = 4096;
var DEFAULT_PREFIX = "#HL";
var REV_PATTERN = /^[A-F0-9]{8}$/;
function hashText(text, length = 10) {
  return createHash("sha1").update(text, "utf8").digest("hex").slice(0, length).toUpperCase();
}
function getAdaptiveHashLength(totalLines) {
  return totalLines > HASH_LENGTH_THRESHOLD ? LARGE_LINE_HASH_LENGTH : SMALL_LINE_HASH_LENGTH;
}
function hashlineLineHash(line, length) {
  return hashText(line, length);
}
function hashlineAnchorHash(previousLine, line, nextLine, length) {
  return hashText(`${previousLine ?? ""}\u241E${line}\u241E${nextLine ?? ""}`, length);
}
function normalizePrefix(prefix) {
  if (prefix === false) {
    return "";
  }
  return typeof prefix === "string" ? prefix : DEFAULT_PREFIX;
}
function normalizeRevToken(revInput) {
  const text = revInput.trim();
  if (REV_PATTERN.test(text.toUpperCase())) {
    return text.toUpperCase();
  }
  const match = text.match(/^(?:#HL|;;;)?\s*REV:([A-F0-9]{8})$/i);
  if (!match) {
    throw new Error(`Invalid REV token "${revInput}". Expected REV:<8-char hex> or a raw 8-char hash.`);
  }
  return match[1].toUpperCase();
}
function formatRef(lineNumber, lineHash, anchorHash) {
  if (!Number.isInteger(lineNumber) || lineNumber < 1) {
    throw new Error(`Invalid line number "${lineNumber}". Expected a positive integer.`);
  }
  const normalizedHash = lineHash.trim().toUpperCase();
  if (!normalizedHash) {
    throw new Error("lineHash is required");
  }
  const normalizedAnchor = typeof anchorHash === "string" && anchorHash.trim().length > 0 ? anchorHash.trim().toUpperCase() : "";
  return normalizedAnchor.length > 0 ? `${lineNumber}#${normalizedHash}#${normalizedAnchor}` : `${lineNumber}#${normalizedHash}`;
}
function formatRev(fileHash) {
  return `REV:${normalizeRev(fileHash)}`;
}
function formatAnnotatedLine(line, index, lines, prefix) {
  const hashLength = getAdaptiveHashLength(Math.max(1, lines.length));
  const lineHash = hashlineLineHash(line, hashLength);
  const anchorHash = hashlineAnchorHash(lines[index - 1], line, lines[index + 1], hashLength);
  const prefixText = normalizePrefix(prefix);
  const prefixPart = prefixText.length > 0 ? `${prefixText} ` : "";
  return `${prefixPart}${formatRef(index + 1, lineHash, anchorHash)}|${line}`;
}
function normalizeRev(revInput) {
  return extractHashFromRev(revInput);
}
function extractHashFromRev(revToken) {
  return normalizeRevToken(revToken);
}

// .opencode/lib/hashline-core.ts
var DEFAULT_LIMIT = 2e3;
var MAX_LINE_LENGTH = 2e3;
var SMALL_FILE_HASH_LEN = 3;
var LARGE_FILE_HASH_LEN = 4;
var HASH_LENGTH_THRESHOLD2 = 4096;
function hashText2(text, length = 10) {
  return createHash2("sha1").update(text, "utf8").digest("hex").slice(0, length).toUpperCase();
}
function getAdaptiveHashLength2(totalLines) {
  return totalLines > HASH_LENGTH_THRESHOLD2 ? LARGE_FILE_HASH_LEN : SMALL_FILE_HASH_LEN;
}
function hashlineLineHash2(line, length = LARGE_FILE_HASH_LEN) {
  return hashText2(line, length);
}
function hashlineAnchorHash2(previousLine, line, nextLine, length = LARGE_FILE_HASH_LEN) {
  return hashText2(`${previousLine ?? ""}\u241E${line}\u241E${nextLine ?? ""}`, length);
}
function computeFileRev(raw) {
  const normalized = raw.includes("\r\n") ? raw.replace(/\r\n/g, "\n") : raw;
  return hashText2(normalized, 8);
}
function parseRaw(raw) {
  const eol = raw.includes("\r\n") ? "\r\n" : "\n";
  const normalized = raw.replace(/\r\n/g, "\n");
  const endsWithNewline = normalized.endsWith("\n");
  let lines = [];
  if (normalized.length > 0) {
    lines = normalized.split("\n");
    if (endsWithNewline) {
      lines.pop();
    }
  }
  return {
    raw,
    lines,
    eol,
    endsWithNewline,
    fileHash: hashText2(raw)
  };
}
function stringifyLines(lines, eol, endsWithNewline) {
  if (lines.length === 0) {
    return "";
  }
  const body = lines.join(eol);
  return endsWithNewline ? `${body}${eol}` : body;
}
function splitContentToLines(content) {
  const normalized = content.replace(/\r\n/g, "\n");
  const hasTrailingNewline = normalized.endsWith("\n");
  const parts = normalized.split("\n");
  if (hasTrailingNewline && parts.length > 0) {
    parts.pop();
  }
  return parts;
}
function parseLineRef(rawRef) {
  const text = rawRef.trim().replace(/^(?:#HL|;;;)\s*/i, "");
  const beforePipe = text.split("|")[0].trim();
  const match = beforePipe.match(/^(\d+)\s*[#: ]\s*([A-Za-z0-9]+)(?:\s*[#: ]\s*([A-Za-z0-9]+))?$/);
  if (!match) {
    throw new Error(
      `Invalid line reference "${rawRef}". Expected format: <line>#<hash> or <line>#<hash>#<anchor> (example: 22#A3F or 22#A3F#9BC)`
    );
  }
  const lineNumber = Number.parseInt(match[1], 10);
  if (!Number.isFinite(lineNumber) || lineNumber < 1) {
    throw new Error(`Invalid line number in reference "${rawRef}"`);
  }
  return {
    lineNumber,
    hash: match[2].toUpperCase(),
    anchor: match[3]?.toUpperCase()
  };
}
function findRefCandidates(parsed, snapshot, hashLength) {
  const candidates = [];
  for (let idx = 0; idx < snapshot.lines.length; idx += 1) {
    const line = snapshot.lines[idx];
    const lineHash = hashlineLineHash2(line, hashLength);
    if (lineHash !== parsed.hash) {
      continue;
    }
    if (parsed.anchor) {
      const anchorHash = hashlineAnchorHash2(snapshot.lines[idx - 1], line, snapshot.lines[idx + 1], hashLength);
      if (anchorHash !== parsed.anchor) {
        continue;
      }
    }
    candidates.push({
      index: idx,
      lineNumber: idx + 1
    });
  }
  return candidates;
}
function resolveRef(ref, snapshot, safeReapply = false) {
  const parsed = parseLineRef(ref);
  if (parsed.lineNumber > snapshot.lines.length) {
    throw new Error(
      `Reference ${ref} points to line ${parsed.lineNumber}, but file only has ${snapshot.lines.length} lines. Read the file again.`
    );
  }
  const hashLength = getAdaptiveHashLength2(snapshot.lines.length);
  const index = parsed.lineNumber - 1;
  const actualLine = snapshot.lines[index];
  const actualHash = hashlineLineHash2(actualLine, hashLength);
  const actualAnchor = hashlineAnchorHash2(snapshot.lines[index - 1], actualLine, snapshot.lines[index + 1], hashLength);
  if (actualHash !== parsed.hash || parsed.anchor && actualAnchor !== parsed.anchor) {
    if (safeReapply) {
      const candidates = findRefCandidates(parsed, snapshot, hashLength);
      if (candidates.length === 1) {
        return {
          index: candidates[0].index,
          lineNumber: candidates[0].lineNumber
        };
      }
      if (candidates.length > 1) {
        const candidateLines = candidates.map((candidate) => candidate.lineNumber).join(", ");
        throw new Error(
          `Hash mismatch for line ${parsed.lineNumber}; found multiple relocation candidates (${candidateLines}). Read the file again.`
        );
      }
      throw new Error(`Hash mismatch for line ${parsed.lineNumber}; no relocation candidates found. Read the file again.`);
    }
    const expectedRef = parsed.anchor ? `${parsed.lineNumber}#${parsed.hash}#${parsed.anchor}` : `${parsed.lineNumber}#${parsed.hash}`;
    const actualRef = `${parsed.lineNumber}#${actualHash}#${actualAnchor}`;
    throw new Error(
      `Hash mismatch for line ${parsed.lineNumber}. Expected ${expectedRef}, actual ${actualRef}. Read the file again.`
    );
  }
  return {
    index,
    lineNumber: parsed.lineNumber
  };
}
function resolveFilePath(filePath, context) {
  const baseDirectory = typeof context?.directory === "string" && context.directory.length > 0 ? context.directory : process.cwd();
  return path.isAbsolute(filePath) ? path.normalize(filePath) : path.resolve(baseDirectory, filePath);
}
async function readSnapshot(absolutePath) {
  const raw = await fs.readFile(absolutePath, "utf8");
  const parsed = parseRaw(raw);
  return {
    absolutePath,
    ...parsed
  };
}
async function readSnapshotIfExists(absolutePath) {
  try {
    return await readSnapshot(absolutePath);
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}
function emptySnapshot(absolutePath) {
  return {
    absolutePath,
    raw: "",
    lines: [],
    eol: "\n",
    endsWithNewline: false,
    fileHash: hashText2("")
  };
}
function normalizeOperations(operations) {
  return operations.map((op) => ({
    op: op.op,
    ref: op.ref?.trim(),
    startRef: op.startRef?.trim(),
    endRef: op.endRef?.trim(),
    content: op.content
  }));
}
function resolveRefRange(params) {
  if (params.ref && params.startRef) {
    throw new Error(`${params.label} accepts either ref or startRef/endRef, not both`);
  }
  const baseStartRef = params.startRef ?? params.ref;
  if (!baseStartRef) {
    throw new Error(`${params.label} requires ref or startRef`);
  }
  let start = resolveRef(baseStartRef, params.snapshot, params.safeReapply);
  let end = params.endRef ? resolveRef(params.endRef, params.snapshot, params.safeReapply) : start;
  if (start.index > end.index) {
    const first = start;
    start = end;
    end = first;
  }
  return {
    start,
    end
  };
}
function resolveChanges(snapshot, operations, safeReapply) {
  if (operations.length === 0) {
    throw new Error("No operations provided");
  }
  const setFileCount = operations.filter((op) => op.op === "set_file").length;
  if (setFileCount > 0 && operations.length > 1) {
    throw new Error("set_file cannot be combined with other operations");
  }
  return operations.map((op, order) => {
    switch (op.op) {
      case "replace": {
        if (op.content === void 0) {
          throw new Error("replace requires content");
        }
        const resolvedRange = resolveRefRange({
          snapshot,
          ref: op.ref,
          startRef: op.startRef,
          endRef: op.endRef,
          safeReapply,
          label: "replace"
        });
        return {
          op: op.op,
          spliceStart: resolvedRange.start.index,
          deleteCount: resolvedRange.end.index - resolvedRange.start.index + 1,
          insertLines: splitContentToLines(op.content),
          order,
          anchorIndex: resolvedRange.start.index,
          label: op.startRef || op.endRef ? `replace(${op.startRef ?? op.ref}..${op.endRef ?? op.startRef ?? op.ref})` : `replace(${op.ref})`
        };
      }
      case "delete": {
        const resolvedRange = resolveRefRange({
          snapshot,
          ref: op.ref,
          startRef: op.startRef,
          endRef: op.endRef,
          safeReapply,
          label: "delete"
        });
        return {
          op: op.op,
          spliceStart: resolvedRange.start.index,
          deleteCount: resolvedRange.end.index - resolvedRange.start.index + 1,
          insertLines: [],
          order,
          anchorIndex: resolvedRange.start.index,
          label: op.startRef || op.endRef ? `delete(${op.startRef ?? op.ref}..${op.endRef ?? op.startRef ?? op.ref})` : `delete(${op.ref})`
        };
      }
      case "insert_before": {
        if (op.content === void 0) {
          throw new Error("insert_before requires content");
        }
        const resolvedRange = resolveRefRange({
          snapshot,
          ref: op.ref,
          startRef: op.startRef,
          endRef: op.endRef,
          safeReapply,
          label: "insert_before"
        });
        return {
          op: op.op,
          spliceStart: resolvedRange.start.index,
          deleteCount: 0,
          insertLines: splitContentToLines(op.content),
          order,
          anchorIndex: resolvedRange.start.index,
          label: op.startRef || op.endRef ? `insert_before(${op.startRef ?? op.ref}..${op.endRef ?? op.startRef ?? op.ref})` : `insert_before(${op.ref})`
        };
      }
      case "insert_after": {
        if (op.content === void 0) {
          throw new Error("insert_after requires content");
        }
        const resolvedRange = resolveRefRange({
          snapshot,
          ref: op.ref,
          startRef: op.startRef,
          endRef: op.endRef,
          safeReapply,
          label: "insert_after"
        });
        return {
          op: op.op,
          spliceStart: resolvedRange.end.index + 1,
          deleteCount: 0,
          insertLines: splitContentToLines(op.content),
          order,
          anchorIndex: resolvedRange.end.index,
          label: op.startRef || op.endRef ? `insert_after(${op.startRef ?? op.ref}..${op.endRef ?? op.startRef ?? op.ref})` : `insert_after(${op.ref})`
        };
      }
      case "replace_range": {
        if (!op.startRef || !op.endRef) {
          throw new Error("replace_range requires startRef and endRef");
        }
        if (op.content === void 0) {
          throw new Error("replace_range requires content");
        }
        const start = resolveRef(op.startRef, snapshot, safeReapply);
        const end = resolveRef(op.endRef, snapshot, safeReapply);
        if (start.index > end.index) {
          throw new Error("replace_range startRef must be on or before endRef");
        }
        return {
          op: op.op,
          spliceStart: start.index,
          deleteCount: end.index - start.index + 1,
          insertLines: splitContentToLines(op.content),
          order,
          anchorIndex: start.index,
          label: `replace_range(${op.startRef}..${op.endRef})`
        };
      }
      case "set_file": {
        if (op.content === void 0) {
          throw new Error("set_file requires content");
        }
        return {
          op: op.op,
          spliceStart: 0,
          deleteCount: snapshot.lines.length,
          insertLines: splitContentToLines(op.content),
          order,
          anchorIndex: void 0,
          label: "set_file"
        };
      }
      default:
        throw new Error(`Unsupported operation: ${op.op ?? "unknown"}`);
    }
  });
}
function validateChangeConflicts(changes) {
  const consumed = /* @__PURE__ */ new Map();
  for (const change of changes) {
    if (change.deleteCount === 0) {
      continue;
    }
    for (let idx = change.spliceStart; idx < change.spliceStart + change.deleteCount; idx += 1) {
      const existing = consumed.get(idx);
      if (existing) {
        throw new Error(`Overlapping operations are not allowed: ${change.label} conflicts with ${existing}`);
      }
      consumed.set(idx, change.label);
    }
  }
  for (const change of changes) {
    if (change.deleteCount !== 0 || change.anchorIndex === void 0) {
      continue;
    }
    const existing = consumed.get(change.anchorIndex);
    if (existing) {
      throw new Error(`Operation conflict: ${change.label} references a line already modified by ${existing}`);
    }
  }
}
function applyChanges(snapshot, changes) {
  const nextLines = [...snapshot.lines];
  const ordered = [...changes].sort((a, b) => {
    if (a.spliceStart !== b.spliceStart) {
      return b.spliceStart - a.spliceStart;
    }
    return b.order - a.order;
  });
  let additions = 0;
  let removals = 0;
  for (const change of ordered) {
    additions += change.insertLines.length;
    removals += change.deleteCount;
    nextLines.splice(change.spliceStart, change.deleteCount, ...change.insertLines);
  }
  return {
    lines: nextLines,
    additions,
    removals
  };
}
function snapshotFromLines(base, nextLines) {
  const nextRaw = stringifyLines(nextLines, base.eol, base.endsWithNewline);
  const parsed = parseRaw(nextRaw);
  return {
    absolutePath: base.absolutePath,
    ...parsed
  };
}
async function writeSnapshot(snapshot) {
  await fs.mkdir(path.dirname(snapshot.absolutePath), { recursive: true });
  await fs.writeFile(snapshot.absolutePath, snapshot.raw, "utf8");
}
function formatEditResult(params) {
  return [
    `Hashline ${params.mode} edit ${params.dryRun ? "(dry run) " : ""}completed for ${params.filePath}.`,
    `File hash: ${params.before.fileHash} -> ${params.after.fileHash}`,
    `Operations: ${params.operations}; additions: ${params.additions}; removals: ${params.removals}`,
    `Lines: ${params.before.lines.length} -> ${params.after.lines.length}`,
    "Read the file again before issuing additional hashline refs."
  ].join("\n");
}
function buildOperationResult(params) {
  return {
    summary: formatEditResult(params),
    metadata: {
      filediff: {
        file: params.filePath,
        before: params.before.raw,
        after: params.after.raw,
        additions: params.additions,
        deletions: params.removals
      },
      files: [
        {
          filePath: params.filePath,
          before: params.before.raw,
          after: params.after.raw,
          additions: params.additions,
          deletions: params.removals
        }
      ]
    }
  };
}
async function runHashlineRead(params) {
  const absolutePath = resolveFilePath(params.filePath, params.context);
  const snapshot = await readSnapshot(absolutePath);
  const startLine = Math.max(1, Math.floor(params.offset ?? 1));
  const limit = Math.max(1, Math.floor(params.limit ?? DEFAULT_LIMIT));
  const startIndex = startLine - 1;
  const endIndex = Math.min(snapshot.lines.length, startIndex + limit);
  const body = [];
  body.push(`${DEFAULT_PREFIX} ${formatRev(computeFileRev(snapshot.raw))}`);
  for (let idx = startIndex; idx < endIndex; idx += 1) {
    const line = snapshot.lines[idx];
    const displayLine = line.length > MAX_LINE_LENGTH ? `${line.slice(0, MAX_LINE_LENGTH)}\u2026` : line;
    const annotatedLine = formatAnnotatedLine(line, idx, snapshot.lines, DEFAULT_PREFIX);
    const separatorIndex = annotatedLine.indexOf("|");
    body.push(`${annotatedLine.slice(0, separatorIndex + 1)}${displayLine}`);
  }
  if (snapshot.lines.length === 0) {
    body.push("# file is empty");
  }
  if (startIndex > 0) {
    body.unshift(`# skipped lines: 1-${startIndex}`);
  }
  if (endIndex < snapshot.lines.length) {
    body.push(`# truncated: ${snapshot.lines.length - endIndex} lines not shown`);
  }
  return [
    `<hashline-file path="${absolutePath}" file_hash="${snapshot.fileHash}" total_lines="${snapshot.lines.length}" start_line="${startLine}" shown_until="${endIndex}">`,
    "# format: <line>#<hash>#<anchor>|<content>",
    "# use refs exactly as shown in hashline edit/patch tools",
    ...body,
    "</hashline-file>"
  ].join("\n");
}
async function runHashlineOperationsDetailed(params) {
  const absolutePath = resolveFilePath(params.filePath, params.context);
  const existingSnapshot = await readSnapshotIfExists(absolutePath);
  const snapshot = existingSnapshot ?? emptySnapshot(absolutePath);
  const normalizedOps = normalizeOperations(params.operations);
  if (params.expectedFileHash && snapshot.fileHash !== params.expectedFileHash.toUpperCase()) {
    throw new Error(
      `File hash mismatch for ${params.filePath}. Expected ${params.expectedFileHash.toUpperCase()}, actual ${snapshot.fileHash}. Read the file again before editing.`
    );
  }
  if (params.fileRev) {
    const expectedRev = params.fileRev.toUpperCase();
    const actualRev = computeFileRev(snapshot.raw);
    if (actualRev !== expectedRev) {
      throw new Error(
        `File revision mismatch for ${params.filePath}. Expected ${expectedRev}, actual ${actualRev}. Read the file again before editing.`
      );
    }
  }
  const changes = resolveChanges(snapshot, normalizedOps, Boolean(params.safeReapply));
  validateChangeConflicts(changes);
  const applied = applyChanges(snapshot, changes);
  const after = snapshotFromLines(snapshot, applied.lines);
  if (!params.dryRun) {
    await writeSnapshot(after);
  }
  return buildOperationResult({
    filePath: params.filePath,
    mode: "hashline",
    dryRun: Boolean(params.dryRun),
    before: snapshot,
    after,
    operations: normalizedOps.length,
    additions: applied.additions,
    removals: applied.removals
  });
}
function mapOperationInput(input) {
  return {
    op: input.op,
    ref: input.ref,
    startRef: input.startRef,
    endRef: input.endRef,
    content: input.content
  };
}

// .opencode/plugins/hashline-shared.ts
import { createHash as createHash3 } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import path2 from "node:path";
var CONFIG_FILENAME = "opencode-hashline.json";
var DEFAULT_PREFIX2 = "#HL";
var DEFAULT_EXCLUDE_PATTERNS = [
  "**/node_modules/**",
  "**/*.lock",
  "**/package-lock.json",
  "**/yarn.lock",
  "**/pnpm-lock.yaml",
  "**/*.min.js",
  "**/*.min.css",
  "**/*.map",
  "**/*.wasm",
  "**/*.png",
  "**/*.jpg",
  "**/*.jpeg",
  "**/*.gif",
  "**/*.ico",
  "**/*.svg",
  "**/*.woff",
  "**/*.woff2",
  "**/*.ttf",
  "**/*.eot",
  "**/*.pdf",
  "**/*.zip",
  "**/*.tar",
  "**/*.gz",
  "**/*.exe",
  "**/*.dll",
  "**/*.so",
  "**/*.dylib",
  "**/.env",
  "**/.env.*",
  "**/*.pem",
  "**/*.key",
  "**/*.p12",
  "**/*.pfx",
  "**/id_rsa",
  "**/id_rsa.pub",
  "**/id_ed25519",
  "**/id_ed25519.pub",
  "**/id_ecdsa",
  "**/id_ecdsa.pub"
];
var DEFAULT_HASHLINE_RUNTIME_CONFIG = {
  exclude: DEFAULT_EXCLUDE_PATTERNS,
  maxFileSize: 1048576,
  cacheSize: 100,
  prefix: DEFAULT_PREFIX2,
  fileRev: true,
  safeReapply: false
};
function hashText3(text, length = 10) {
  return createHash3("sha1").update(text, "utf8").digest("hex").slice(0, length).toUpperCase();
}
function sanitizeConfig(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return {};
  }
  const source = input;
  const out = {};
  if (Array.isArray(source.exclude)) {
    out.exclude = source.exclude.filter(
      (item) => typeof item === "string" && item.length > 0 && item.length <= 512
    );
  }
  if (typeof source.maxFileSize === "number" && Number.isFinite(source.maxFileSize) && source.maxFileSize >= 0) {
    out.maxFileSize = Math.floor(source.maxFileSize);
  }
  if (typeof source.cacheSize === "number" && Number.isFinite(source.cacheSize) && source.cacheSize > 0) {
    out.cacheSize = Math.floor(source.cacheSize);
  }
  if (source.prefix === false) {
    out.prefix = false;
  } else if (typeof source.prefix === "string") {
    if (/^[\x20-\x7E]{0,20}$/.test(source.prefix)) {
      out.prefix = source.prefix;
    }
  }
  if (typeof source.fileRev === "boolean") {
    out.fileRev = source.fileRev;
  }
  if (typeof source.safeReapply === "boolean") {
    out.safeReapply = source.safeReapply;
  }
  return out;
}
function readConfigFile(filePath) {
  if (!existsSync(filePath)) {
    return void 0;
  }
  try {
    const raw = readFileSync(filePath, "utf8");
    return sanitizeConfig(JSON.parse(raw));
  } catch {
    return void 0;
  }
}
function resolveHashlineConfig(projectDir) {
  const globalPath = path2.join(homedir(), ".config", "opencode", CONFIG_FILENAME);
  const projectPath = projectDir ? path2.join(projectDir, CONFIG_FILENAME) : void 0;
  const globalConfig = readConfigFile(globalPath);
  const projectConfig = projectPath ? readConfigFile(projectPath) : void 0;
  return {
    ...DEFAULT_HASHLINE_RUNTIME_CONFIG,
    ...globalConfig,
    ...projectConfig,
    exclude: (projectConfig?.exclude ?? globalConfig?.exclude ?? DEFAULT_EXCLUDE_PATTERNS).slice()
  };
}
function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
function normalizeGlobPath(value) {
  return value.replace(/\\/g, "/");
}
function shouldExclude(filePath, patterns) {
  const normalizedPath = normalizeGlobPath(filePath);
  const effectivePatterns = Array.isArray(patterns) ? patterns : DEFAULT_EXCLUDE_PATTERNS;
  return effectivePatterns.some((pattern) => path2.matchesGlob(normalizedPath, normalizeGlobPath(pattern)));
}
var textEncoder = new TextEncoder();
function getByteLength(content) {
  return textEncoder.encode(content).length;
}
function formatWithHashline(content, options) {
  const effectivePrefix = options?.prefix === void 0 ? DEFAULT_PREFIX2 : options.prefix === false ? "" : options.prefix;
  const prefixPart = effectivePrefix.length > 0 ? `${effectivePrefix} ` : "";
  const normalized = content.includes("\r\n") ? content.replace(/\r\n/g, "\n") : content;
  const lines = normalized.split("\n");
  const output = [];
  if (options?.includeFileRev) {
    output.push(`${prefixPart}REV:${computeFileRev(normalized)}`);
  }
  const hashLength = getAdaptiveHashLength2(lines.length);
  for (let idx = 0; idx < lines.length; idx += 1) {
    const line = lines[idx];
    const lineHash = hashlineLineHash2(line, hashLength);
    const anchorHash = hashlineAnchorHash2(lines[idx - 1], line, lines[idx + 1], hashLength);
    output.push(`${prefixPart}${idx + 1}#${lineHash}#${anchorHash}|${line}`);
  }
  return output.join("\n");
}
function formatWithRuntimeConfig(content, config) {
  return formatWithHashline(content, {
    prefix: config.prefix,
    includeFileRev: config.fileRev
  });
}
function stripHashlinePrefixes(content, prefix) {
  const effectivePrefix = prefix === void 0 ? DEFAULT_PREFIX2 : prefix === false ? "" : prefix;
  const escapedPrefix = effectivePrefix.length > 0 ? `${escapeRegex(effectivePrefix)}\\s*` : "";
  const lineEnding = content.includes("\r\n") ? "\r\n" : "\n";
  const normalized = lineEnding === "\r\n" ? content.replace(/\r\n/g, "\n") : content;
  const refPattern = new RegExp(`^([+\\- ])?${escapedPrefix}\\d+\\s*[#: ]\\s*[A-Za-z0-9]+(?:\\s*[#: ]\\s*[A-Za-z0-9]+)?\\|`, "i");
  const revPattern = new RegExp(`^${escapedPrefix}REV:[A-Za-z0-9]{8}$`, "i");
  const stripped = normalized.split("\n").filter((line) => !revPattern.test(line)).map((line) => {
    const match = line.match(refPattern);
    if (!match) {
      return line;
    }
    const marker = match[1] ?? "";
    return marker + line.slice(match[0].length);
  }).join("\n");
  return lineEnding === "\r\n" ? stripped.replace(/\n/g, "\r\n") : stripped;
}
var HASHLINE_SYSTEM_INSTRUCTION_MARKER = "<!-- hashline-instruction-v1 -->";
var HASHLINE_SYSTEM_INSTRUCTION_END_MARKER = "<!-- /hashline-instruction-v1 -->";
function getConfiguredPrefixLabel(prefix) {
  if (prefix === false) {
    return "none";
  }
  if (typeof prefix !== "string") {
    return `"${DEFAULT_PREFIX2}"`;
  }
  if (prefix.length === 0) {
    return '""';
  }
  return `"${prefix}"`;
}
function buildHashlineSystemInstruction(config) {
  const configuredPrefix = getConfiguredPrefixLabel(config.prefix);
  const canonicalReadRef = `${DEFAULT_PREFIX2} 12#A3F#9BC`;
  const canonicalRev = `${DEFAULT_PREFIX2} REV:72C4946C`;
  return [
    HASHLINE_SYSTEM_INSTRUCTION_MARKER,
    "Hashline workflow:",
    `- Read returns canonical refs like \`${canonicalReadRef}\` and \`${canonicalRev}\`. Copy them exactly as shown.`,
    `- Active helper prefix from config: ${configuredPrefix}. Read output stays canonical \`${DEFAULT_PREFIX2}\`, so do not rewrite refs just to match config.`,
    "- After one read, batch same-file changes into one edit call with operations[] instead of many single edits.",
    "- Send fileRev when the read output includes a REV line.",
    "- Reread only when you need more context or an edit fails because refs are stale.",
    "- Prefer edit for targeted changes; use write only for new files or full rewrites.",
    HASHLINE_SYSTEM_INSTRUCTION_END_MARKER
  ].join("\n");
}
var CACHE_KEY_SEPARATOR = "\0";
function buildCacheEntryKey(baseKey, ...parts) {
  if (parts.length === 0) {
    return baseKey;
  }
  return [
    baseKey,
    ...parts.map((part) => part === void 0 ? "" : String(part))
  ].join(CACHE_KEY_SEPARATOR);
}
var HashlineAnnotationCache = class {
  constructor(maxSize = 100) {
    this.maxSize = maxSize;
  }
  maxSize;
  entries = /* @__PURE__ */ new Map();
  get(key, source) {
    const entry = this.entries.get(key);
    if (!entry) {
      return null;
    }
    const currentHash = hashText3(source, 12);
    if (entry.sourceHash !== currentHash) {
      this.entries.delete(key);
      return null;
    }
    this.entries.delete(key);
    this.entries.set(key, entry);
    return entry.annotated;
  }
  set(key, source, annotated) {
    if (this.entries.has(key)) {
      this.entries.delete(key);
    }
    if (this.entries.size >= this.maxSize) {
      const oldestKey = this.entries.keys().next().value;
      if (typeof oldestKey === "string") {
        this.entries.delete(oldestKey);
      }
    }
    this.entries.set(key, {
      sourceHash: hashText3(source, 12),
      annotated
    });
  }
  invalidate(key) {
    this.entries.delete(key);
  }
  invalidateVariants(baseKey) {
    this.entries.delete(baseKey);
    const variantPrefix = `${baseKey}${CACHE_KEY_SEPARATOR}`;
    for (const key of Array.from(this.entries.keys())) {
      if (key.startsWith(variantPrefix)) {
        this.entries.delete(key);
      }
    }
  }
  clear() {
    this.entries.clear();
  }
};
function extractPathFromToolArgs(args) {
  if (!args) {
    return void 0;
  }
  const candidate = args.path ?? args.filePath ?? args.file_path ?? args.file;
  return typeof candidate === "string" && candidate.length > 0 ? candidate : void 0;
}

// .opencode/plugins/hashline-hooks.ts
import path3 from "node:path";
import { promises as fs2, rmSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { tmpdir } from "node:os";
var FILE_EDIT_TOOLS = ["hashline_edit", "hashline_write", "hashline_patch", "edit", "write", "patch", "apply_patch", "file_edit", "file_write", "edit_file", "multiedit", "batch"];
function toolEndsWith(tool, known) {
  const lower = tool.toLowerCase();
  return known.some((item) => lower === item || lower.endsWith(`.${item}`));
}
function isFileReadTool(tool, _args) {
  const lower = tool.toLowerCase();
  return lower === "read" || lower === "view" || lower.endsWith(".read") || lower.endsWith(".view");
}
function isFileEditTool(tool) {
  return toolEndsWith(tool, FILE_EDIT_TOOLS);
}
function isNativeEditTool(tool) {
  return toolEndsWith(tool, ["edit"]);
}
var HASHLINE_SYSTEM_INSTRUCTION_BLOCK_RE = /<!--[\s]*hashline-instruction-v\d+[\s]*-->[\s\S]*?(?:<!--[\s]*\/hashline-instruction-v\d+[\s]*-->|$)/gi;
function normalizeHashlineInstructionEntry(entry, instruction, keepInstruction) {
  let insertedInstruction = false;
  return entry.replace(HASHLINE_SYSTEM_INSTRUCTION_BLOCK_RE, () => {
    if (!keepInstruction) {
      return "";
    }
    if (insertedInstruction) {
      return "";
    }
    insertedInstruction = true;
    return instruction;
  });
}
function getCanonicalPath(filePath, input) {
  try {
    return resolveFilePath(filePath, {
      directory: typeof input?.directory === "string" ? input.directory : void 0
    });
  } catch {
    return filePath;
  }
}
function invalidateFileCache(cache, args, input) {
  const filePath = extractPathFromToolArgs(args);
  if (!filePath) {
    return;
  }
  const canonicalPath = getCanonicalPath(filePath, input);
  cache.invalidateVariants(filePath);
  cache.invalidateVariants(canonicalPath);
}
function firstString(...values) {
  for (const value of values) {
    if (typeof value === "string" && value.length > 0) {
      return value;
    }
  }
  return void 0;
}
function firstBoolean(...values) {
  for (const value of values) {
    if (typeof value === "boolean") {
      return value;
    }
  }
  return void 0;
}
function hasHashlineEditShape(args) {
  return Array.isArray(args.operations) || typeof args.operation === "string" || typeof args.ref === "string" || typeof args.startRef === "string" || typeof args.start_ref === "string";
}
function toHashlineOperations(args) {
  if (Array.isArray(args.operations) && args.operations.length > 0) {
    return args.operations.map((entry) => {
      const item = entry ?? {};
      return {
        op: String(item.op ?? ""),
        ref: firstString(item.ref),
        startRef: firstString(item.startRef, item.start_ref),
        endRef: firstString(item.endRef, item.end_ref),
        content: firstString(item.content, item.replacement)
      };
    });
  }
  const operation = firstString(args.operation);
  if (!operation) {
    return null;
  }
  const ref = firstString(args.ref);
  const startRef = firstString(args.startRef, args.start_ref, ref);
  const endRef = firstString(args.endRef, args.end_ref);
  const content = firstString(args.replacement, args.content);
  if (!startRef && !ref) {
    return null;
  }
  return [
    {
      op: operation === "replace" && endRef ? "replace_range" : operation,
      ref,
      startRef,
      endRef,
      content
    }
  ];
}
async function translateHashlineEditArgs(args, input, config) {
  if (!hasHashlineEditShape(args)) {
    return null;
  }
  const filePath = firstString(args.filePath, args.file_path, args.path, args.file);
  if (!filePath) {
    return null;
  }
  const operations = toHashlineOperations(args);
  if (!operations || operations.length === 0) {
    return null;
  }
  const result = await runHashlineOperationsDetailed({
    filePath,
    operations: operations.map(mapOperationInput),
    expectedFileHash: firstString(args.expectedFileHash, args.expected_file_hash),
    fileRev: firstString(args.fileRev, args.file_rev),
    safeReapply: firstBoolean(args.safeReapply, args.safe_reapply) ?? config.safeReapply,
    dryRun: true,
    context: {
      directory: typeof input.directory === "string" ? input.directory : void 0
    }
  });
  return {
    filePath,
    oldString: result.metadata.filediff.before,
    newString: result.metadata.filediff.after
  };
}
var CONTENT_FIELD_KEYS = /* @__PURE__ */ new Set([
  "content",
  "new_content",
  "old_content",
  "old_string",
  "new_string",
  "replacement",
  "text",
  "diff",
  "patch",
  "patch_text",
  "patchText",
  "body"
]);
function stripNestedHashes(value, prefix) {
  if (typeof value === "string") {
    return stripHashlinePrefixes(value, prefix);
  }
  if (Array.isArray(value)) {
    return value.map((entry) => stripNestedHashes(entry, prefix));
  }
  if (!value || typeof value !== "object") {
    return value;
  }
  const out = { ...value };
  for (const key of Object.keys(out)) {
    if (CONTENT_FIELD_KEYS.has(key)) {
      out[key] = stripNestedHashes(out[key], prefix);
      continue;
    }
    const candidate = out[key];
    if (Array.isArray(candidate) || candidate && typeof candidate === "object") {
      out[key] = stripNestedHashes(candidate, prefix);
    }
  }
  return out;
}
var tempDirPromise = null;
var tempDirPath = null;
var tempCleanupRegistered = false;
async function getTempDirectory() {
  if (!tempDirPromise) {
    tempDirPromise = fs2.mkdtemp(path3.join(tmpdir(), "hashline-chat-")).then((dir) => {
      tempDirPath = dir;
      if (!tempCleanupRegistered) {
        tempCleanupRegistered = true;
        process.on("exit", () => {
          if (!tempDirPath) {
            return;
          }
          try {
            rmSync(tempDirPath, { recursive: true, force: true });
          } catch {
          }
        });
      }
      return dir;
    });
  }
  return tempDirPromise;
}
async function writeAnnotatedTempFile(content) {
  const tempDir = await getTempDirectory();
  const fileName = `hl-${Date.now()}-${randomBytes(6).toString("hex")}.txt`;
  const tempPath = path3.join(tempDir, fileName);
  await fs2.writeFile(tempPath, content, "utf8");
  return tempPath;
}

// v2/entry.ts
var HASHLINE_SYSTEM_INSTRUCTION_MARKER_RE = /<!--[\s]*hashline-instruction-v\d+[\s]*-->/i;
var PATH_ALIASES = ["filePath", "file_path", "file"];
function firstString2(...values) {
  for (const value of values) {
    if (typeof value === "string" && value.length > 0) {
      return value;
    }
  }
  return void 0;
}
function normalizePathField(args) {
  const out = { ...args };
  if (typeof out.path !== "string") {
    const candidate = firstString2(...PATH_ALIASES.map((key) => out[key]));
    if (candidate) {
      out.path = candidate;
    }
  }
  if (typeof out.path === "string") {
    for (const key of PATH_ALIASES) {
      delete out[key];
    }
  }
  return out;
}
function normalizeArgs(toolName, args) {
  let out = { ...args };
  if (toolName === "read" || toolName === "edit" || toolName === "write") {
    out = normalizePathField(out);
  }
  if (toolName === "edit") {
    if (typeof out.start_ref === "string" && typeof out.startRef !== "string") out.startRef = out.start_ref;
    if (typeof out.end_ref === "string" && typeof out.endRef !== "string") out.endRef = out.end_ref;
    if (typeof out.safe_reapply === "boolean" && typeof out.safeReapply !== "boolean") out.safeReapply = out.safe_reapply;
    if (typeof out.expected_file_hash === "string" && typeof out.expectedFileHash !== "string") out.expectedFileHash = out.expected_file_hash;
    if (typeof out.file_rev === "string" && typeof out.fileRev !== "string") out.fileRev = out.file_rev;
    if (typeof out.dry_run === "boolean" && typeof out.dryRun !== "boolean") out.dryRun = out.dry_run;
  }
  if (toolName === "patch") {
    if (typeof out.patch_text === "string" && typeof out.patchText !== "string") out.patchText = out.patch_text;
    if (typeof out.file_path === "string" && typeof out.path !== "string") out.path = out.file_path;
    if (typeof out.expected_file_hash === "string" && typeof out.expectedFileHash !== "string") out.expectedFileHash = out.expected_file_hash;
    if (typeof out.file_rev === "string" && typeof out.fileRev !== "string") out.fileRev = out.file_rev;
    if (typeof out.dry_run === "boolean" && typeof out.dryRun !== "boolean") out.dryRun = out.dry_run;
  }
  return out;
}
function normalizeName(name) {
  return name === "view" ? "read" : name;
}
var HASHLINE_OPERATION_SCHEMA = {
  type: "object",
  properties: {
    op: { type: "string" },
    ref: { type: "string" },
    startRef: { type: "string" },
    endRef: { type: "string" },
    content: { type: "string" },
    replacement: { type: "string" }
  },
  required: ["op"],
  additionalProperties: false
};
var EDIT_INPUT_SCHEMA = {
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
    replacement: { type: "string" }
  },
  required: ["path"],
  additionalProperties: true
};
function readResultText(result) {
  const content = result?.content;
  if (typeof content === "string") {
    return content;
  }
  if (Array.isArray(content)) {
    const block = content.find(
      (entry) => entry && typeof entry === "object" && entry.type === "text"
    );
    return typeof block?.text === "string" ? block.text : void 0;
  }
  return void 0;
}
function setReadResultText(result, text) {
  if (!result || typeof result !== "object") {
    return false;
  }
  const holder = result;
  if (typeof holder.content === "string") {
    holder.content = text;
    return true;
  }
  if (Array.isArray(holder.content)) {
    const block = holder.content.find(
      (entry) => entry && typeof entry === "object" && entry.type === "text"
    );
    if (block) {
      block.text = text;
      return true;
    }
  }
  return false;
}
function updateSystemParts(parts, instruction) {
  const nextParts = [];
  let insertedInstruction = false;
  for (const part of parts) {
    if (typeof part?.text !== "string" || !HASHLINE_SYSTEM_INSTRUCTION_MARKER_RE.test(part.text)) {
      nextParts.push(part);
      continue;
    }
    if (!insertedInstruction) {
      nextParts.push({ ...part, text: normalizeHashlineInstructionEntry(part.text, instruction, true) });
      insertedInstruction = true;
      continue;
    }
    const cleaned = normalizeHashlineInstructionEntry(part.text, instruction, false);
    if (cleaned.trim().length > 0) {
      nextParts.push({ ...part, text: cleaned });
    }
  }
  if (!insertedInstruction) {
    nextParts.push({ type: "text", text: instruction });
  }
  return nextParts;
}
var TOOL_DESCRIPTIONS = {
  read: `Hashline: Returns canonical ${DEFAULT_PREFIX2} refs plus a REV token. Copy refs exactly from the output, then plan all same-file changes before calling edit.`,
  edit: `Hashline: Accepts refs copied from read. Prefer one batched call per file with { path, fileRev?, operations:[{ op, ref|startRef/endRef, content? }] } instead of many single edits.`,
  write: `Hashline: Use write for new files or full rewrites. Prefer edit for targeted existing-file changes; hashline prefixes inside content are stripped automatically.`,
  patch: `Hashline: Compatibility path only. Prefer read -> one batched edit per file for a faster, lower-read workflow.`
};
var entry_default = {
  id: "angdrew.hashline",
  async setup(ctx) {
    const directory = ctx.location.directory;
    const config = resolveHashlineConfig(directory);
    const cache = new HashlineAnnotationCache(config.cacheSize ?? 128);
    const context = { directory };
    await ctx.tool.transform((editor) => {
      for (const [id, suffix] of Object.entries(TOOL_DESCRIPTIONS)) {
        editor.update(id, (tool) => {
          if (typeof tool.description === "string" && !tool.description.includes(suffix)) {
            tool.description = `${tool.description}

${suffix}`;
          }
          if (id === "edit") {
            tool.input = EDIT_INPUT_SCHEMA;
          }
        });
      }
    });
    await ctx.tool.hook("execute.before", async (event) => {
      const name = normalizeName(event.tool);
      if (!["read", "edit", "write", "patch"].includes(name)) {
        return;
      }
      let args = normalizeArgs(name, event.input ?? {});
      if (isFileEditTool(name)) {
        args = stripNestedHashes(args, config.prefix);
        if (isNativeEditTool(name)) {
          const translated = await translateHashlineEditArgs(args, context, config);
          if (translated) {
            const { filePath, oldString, newString } = translated;
            event.input = { path: filePath, oldString, newString };
            return;
          }
        }
      }
      event.input = args;
    });
    await ctx.tool.hook("execute.after", async (event) => {
      const args = event.input ?? {};
      if (isFileEditTool(event.tool)) {
        invalidateFileCache(cache, args, context);
      }
      if (!isFileReadTool(event.tool, args) || event.status !== "completed") {
        return;
      }
      const source = readResultText(event.result);
      if (typeof source !== "string" || source.includes("<type>directory</type>")) {
        return;
      }
      const filePathFromArgs = extractPathFromToolArgs(args);
      if (typeof filePathFromArgs !== "string") {
        return;
      }
      if (shouldExclude(filePathFromArgs, config.exclude)) {
        return;
      }
      const canonicalPath = getCanonicalPath(filePathFromArgs, context);
      const offset = typeof args.offset === "number" ? args.offset : void 0;
      const limit = typeof args.limit === "number" ? args.limit : void 0;
      const cacheKey = buildCacheEntryKey(canonicalPath, offset, limit);
      const cached = cache.get(cacheKey, source);
      if (cached) {
        setReadResultText(event.result, cached);
        return;
      }
      try {
        const annotated = await runHashlineRead({
          filePath: filePathFromArgs,
          offset,
          limit,
          context
        });
        if (typeof annotated !== "string") {
          return;
        }
        if (config.maxFileSize > 0 && getByteLength(annotated) > config.maxFileSize) {
          return;
        }
        cache.set(cacheKey, source, annotated);
        setReadResultText(event.result, annotated);
      } catch {
        return;
      }
    });
    await ctx.session.hook("context", (event) => {
      const instruction = buildHashlineSystemInstruction(config);
      const parts = Array.isArray(event.system) ? event.system : [];
      event.system = updateSystemParts(parts, instruction);
    });
    await ctx.session.hook("prompt", async (event) => {
      const files = event.prompt?.files;
      if (!Array.isArray(files) || files.length === 0) {
        return;
      }
      for (const file of files) {
        const uri = typeof file?.uri === "string" ? file.uri : void 0;
        if (!uri || !uri.startsWith("file://")) {
          continue;
        }
        let absolutePath;
        try {
          absolutePath = path4.normalize(fileURLToPath(uri));
        } catch {
          continue;
        }
        if (shouldExclude(absolutePath, config.exclude)) {
          continue;
        }
        let source;
        try {
          source = await fs3.readFile(absolutePath, "utf8");
        } catch {
          continue;
        }
        if (config.maxFileSize > 0 && getByteLength(source) > config.maxFileSize) {
          continue;
        }
        const cached = cache.get(absolutePath, source);
        const annotated = cached ?? formatWithRuntimeConfig(source, config);
        if (!cached) {
          cache.set(absolutePath, source, annotated);
        }
        const tempPath = await writeAnnotatedTempFile(annotated);
        file.uri = pathToFileURL(tempPath).href;
      }
    });
  }
};
export {
  entry_default as default
};
