# ghciwatch console vs. `--error-file` output (recorded 2026-09-24)

> **Repo note (verified 2026-09-24):** the loop this repo actually runs does
> not tee. The `tasty` entry point in `test/Main.hs` captures the tasty eval
> output and writes it into `ghcid.txt` itself, before `ghciwatch`'s
> `--error-file` write lands `All good (N modules)` and the diagnostics, so
> the file holds the latest reload's whole result (eval output + compile
> errors) with one writer per write and no pipeline. The `tee` recommendation
> in this note applies only to a bare `ghciwatch` without that eval harness —
> adding a tee on top would give the file two writers.

Question: can a single file mirror exactly what `ghciwatch` prints to the console,
i.e. is `ghciwatch ... 2>&1 | tee ghcid.txt` (no `--error-file`, no `--clear`)
a faithful mirror?

Answer: **yes, by construction** — `tee` copies the exact merged byte stream the
terminal shows. The drift came from `--error-file`, which is a separate, lossy
diagnostics-only channel (no progress lines, no eval output), not a tee.

Source under study: `MercuryTechnologies/ghciwatch` tag `v1.4.2`, matching the
installed binary (`ghciwatch --version` → `ghciwatch 1.4.2`). All citations below
are paths inside that repo.

## a. How GHCi output is read and where it goes

- GHCi is spawned with **both** streams piped: `src/ghci/mod.rs:322-323`
  (`.stdout(Stdio::piped())`, `.stderr(Stdio::piped())`).
- **GHCi stdout** is read by `GhciStdout` (`src/ghci/stdout.rs:25-35`) through an
  `IncrementalReader` whose writer is `opts.stdout_writer` (`src/ghci/mod.rs:356-358`).
- **GHCi stderr** is read line-by-line by `GhciStderr` and every line is forwarded
  to `opts.stderr_writer` (`src/ghci/stderr.rs:96-102`).
- In the default ("Standard") mode those writers are the parent process's own
  stdout/stderr: `src/ghci/mod.rs:192-196` — `stdout_writer = GhciWriter::stdout()`,
  `stderr_writer = GhciWriter::stderr()` (`src/ghci/writer.rs:35-42`).
- So there are two distinct sinks: **GHCi stdout → ghciwatch stdout**,
  **GHCi stderr → ghciwatch stderr**. `--error-file` is a third, independent sink
  (see b). "Console" = stdout + stderr; the error file is not part of it.
- Which parts of the stream are forwarded is controlled by `WriteBehavior`
  (`src/incremental_reader.rs:448-458`): `Write` (all), `NoFinalLine` (all but the
  prompt line), `Hide` (e.g. `:show paths` / `:show targets` bookkeeping output,
  `src/ghci/stdout.rs:133-159`). Hidden bookkeeping never reaches the console either.

## b. What `--error-file` contains (the drift source)

- Option: `--error-file` (aliases `--outputfile`, `--errors`), `src/cli.rs:51-55`;
  stored as `GhciOpts.error_path` (`src/ghci/mod.rs:112-113, 202`).
- Written by `ErrorLog` (`src/ghci/error_log.rs`), producing ghcid-compatible output:
  - On success: a single `All good (N modules)\n` line, only when the summary is
    `CompilationResult::Ok` (`src/ghci/error_log.rs:72-85`).
  - Then every collected diagnostic, verbatim (`src/ghci/error_log.rs:87-90`).
  - Before a reload/restart: `[ghciwatch is still compiling]` overwrites the file
    (`src/ghci/error_log.rs:18, 42-56`; called at `src/ghci/mod.rs:490, 505`).
- The collected set comes from parsing GHCi stdout **and** the stderr buffer
  (`src/ghci/stdout.rs:39-54`) into `GhcMessage`
  (`src/ghci/parse/ghc_message/mod.rs:57-91`):
  - `GhcMessage::Compiling` (progress lines) — logged at debug, **not** stored
    (`src/ghci/compilation_log.rs:63-72`).
  - `GhcMessage::Diagnostic` — stored; severity from `Severity`
    (`src/ghci/parse/ghc_message/severity.rs:13`).
  - `GhcMessage::Summary` → `CompilationSummary` / `CompilationResult` /
    `ModulesLoaded` (`src/ghci/parse/ghc_message/mod.rs:104-111`,
    `.../compilation_summary.rs:24, 32`) — summary only used for the `All good` line.
  - Eval results, reload summaries, timings: **never written to the error file**.
- That is exactly the observed drift: `ghcid.txt` via `--error-file` only ever held
  diagnostics + `All good`, while the console also showed progress, reload banners,
  and tasty eval output.

## c. Where ghciwatch's own output goes

- All of ghciwatch's own messages are `tracing` events. The human layer writes to
  **stderr** unless TUI mode: `src/tracing/mod.rs:64-80` — `None => Box::new(std::io::stderr())`,
  wrapped in `tracing_appender::non_blocking` (`:71`).
- Examples:
  - Reload banners `tracing::info!("Reloading ghci:\n{...}")` — `src/ghci/mod.rs:527-530`.
  - Failure summary `tracing::error!("{Event} failed in {elapsed:.2?}")` —
    `src/ghci/mod.rs:1108-1113`; success `tracing::info!("All good! Finished {event} in ...")` —
    `src/ghci/mod.rs:1114-1120`.
  - Eval banners `tracing::info!("Eval {path}:{command}")` — `src/ghci/mod.rs:643`.
- The **eval command output itself** (our tasty results) is GHCi's stdout: eval
  commands are typed into GHCi via `GhciStdin::run_command`
  (`src/ghci/mod.rs:639-646`, `src/ghci/stdin.rs:57-68`) and the response is read by
  `GhciStdout::prompt` with `WriteBehavior::NoFinalLine`
  (`src/ghci/stdout.rs:81-97`), i.e. written to ghciwatch **stdout**.
- Progress lines `[N of M] Compiling ...` are also GHCi stdout, passed through to
  ghciwatch stdout unchanged in Standard mode (`src/ghci/mod.rs:192-196`).
- So: progress + eval output → stdout; ghciwatch's own log lines → stderr. With
  `2>&1` both land in one pipe; `tee` sees the merged stream. There is no separate
  "console" sink — nothing is written to `/dev/tty` by default.
- `--log-json PATH` (`src/cli.rs:235-248`, `src/tracing/mod.rs:104-126`) writes a
  second copy of the *tracing events only* as JSON — GHCi stdout is not a tracing
  event, so the JSON log is **not** a console mirror. There is no `--log-file`
  other than `--log-json`.

## d. `--clear` and TTY dependence

- `--clear` (`src/cli.rs:76-78`) is called before reloads/restarts
  (`src/ghci/mod.rs:488-489, 503-504`); the guard is only the flag, **no TTY check**:
  `src/ghci/mod.rs:229-237` calls `clearscreen::clear()` unconditionally.
- `clearscreen 2.0.1` `clear()` writes the escape sequence to **stdout**
  (`ClearScreen::clear`, clearscreen `src/lib.rs:425-428`). So `--clear` would inject
  ANSI clears into both console and a tee'd file identically.
- `--experimental-features progress` *is* TTY-gated: enabled only when
  `std::io::stdout().is_terminal()` (`src/ghci/mod.rs:167-168`); under `tee` stdout
  is a pipe, so progress mode auto-disables and the raw `[N of M] Compiling` lines
  pass through unchanged.
- Colors: the human layer decides color from `supports_color::on(Stream::Stdout)`
  (`src/tracing/mod.rs:75-79`). Under `2>&1 | tee` stdout is a pipe → no color, so
  the console (which is tee's output) and the file match. GHCi is piped by
  ghciwatch, so GHC colors are off as well. Only when running ghciwatch with a real
  TTY would color escapes differ — and then they would be present in the file too,
  since tee copies the same bytes.

## e. Supported tee/duplicate-output facility

- None. Grep of the CLI finds only `--error-file` (diagnostics-only, see b) and
  `--log-json` (tracing events only, see c). There is no flag that duplicates
  console output to a file. `tee` (or `script`) is the supported mechanism in
  practice: the shell merges the two real streams into one and copies it.

## Verdict

With `ghciwatch ... 2>&1 | tee ghcid.txt`, **no `--error-file`, no `--clear`**,
`ghcid.txt` contains exactly what the console shows, including:

1. GHC compile errors/warnings — GHCi stdout → ghciwatch stdout (`src/ghci/stdout.rs:81-97`,
   `src/ghci/mod.rs:192-196`).
2. Progress lines `[N of M] Compiling ...` — same stdout path, unmodified in
   Standard mode (`src/ghci/mod.rs:192-196`); progress-mode rewriting cannot trigger
   under a pipe (`src/ghci/mod.rs:167-168`).
3. Reload-failure summaries — tracing → stderr (`src/ghci/mod.rs:1108-1113`,
   `src/tracing/mod.rs:64-80`); merged by `2>&1`.
4. `--enable-eval` output (tasty) — eval commands round-trip through GHCi, whose
   stdout is forwarded to ghciwatch stdout (`src/ghci/mod.rs:639-646`,
   `src/ghci/stdin.rs:57-68`).
5. Nothing bypasses the pipe: no `/dev/tty` writes in the default path; the only
   TTY-dependent behavior is color and `progress` mode, both of which auto-disable
   under `tee`, so the console (tee's output) and the file agree byte-for-byte.

Caveats: the relative *interleaving* of stdout and stderr is whatever `2>&1` plus
the OS pipe produces; it is stable because both are already merged into one pipe
before tee, but a run without tee could interleave differently. On SIGKILL of
ghciwatch the non-blocking tracing appender can lose queued stderr lines — from the
console and the file alike.

## Recommended `make dev` invocation

Keep the Makefile as-is (it already has the right shape):

```
ghciwatch --no-interrupt-reloads  \
	--command ghci-$(GHC) \
	--restart-glob Makefile \
	--restart-glob .ghc.environment.* \
	--restart-glob '!dist-newstyle/**/*.cabal' \
	--reload-glob  '!dist-newstyle/**/*.hs' \
	--enable-eval \
	--watch src \
	--watch test 2>&1 | tee ghcid.txt
```

Do not add `--error-file` (lossy, separate channel), `--clear` (injects ANSI into
the file), `--experimental-features progress` (TTY-gated anyway), or `--log-json`
(tracing events only). `tee` is the mechanism.

## Evidence

Commands run (read-only):

```sh
which ghciwatch            # /Users/duke/.cargo/bin/ghciwatch
ghciwatch --version        # ghciwatch 1.4.2
git clone --depth 1 --branch v1.4.2 \
  https://github.com/MercuryTechnologies/ghciwatch \
  /var/folders/fk/w27km5892sb7vbt454rpq7b00000gn/T/opencode/ghciwatch
curl -sL https://static.crates.io/crates/clearscreen/clearscreen-2.0.1.crate \
  -o /var/folders/fk/w27km5892sb7vbt454rpq7b00000gn/T/opencode/clearscreen.crate
tar -xzf /var/folders/fk/w27km5892sb7vbt454rpq7b00000gn/T/opencode/clearscreen.crate \
  -C /var/folders/fk/w27km5892sb7vbt454rpq7b00000gn/T/opencode/
```

Key excerpts (ghciwatch v1.4.2):

```rust
// src/ghci/mod.rs:192-196
OutputMode::Standard => {
    stdout_writer = GhciWriter::stdout();
    stderr_writer = GhciWriter::stderr();
    tui_reader = None;
}
```

```rust
// src/tracing/mod.rs:64-71
let tracing_writer: Box<dyn Write + Send + Sync + 'static> = match self.tui.take() {
    Some((reader, writer)) => { ... }
    None => Box::new(std::io::stderr()),
};
let (tracing_writer, worker_guard) = tracing_appender::non_blocking(tracing_writer);
```

```rust
// src/ghci/error_log.rs:72-90
if let Some(summary) = log.summary {
    if let CompilationResult::Ok = summary.result {
        ... writer.write_all(format!("All good ({modules_loaded})\n").as_bytes()).await?;
    }
}
for diagnostic in &log.diagnostics {
    writer.write_all(diagnostic.to_string().as_bytes()).await?;
}
```

```rust
// src/ghci/mod.rs:229-237  (--clear has no TTY check)
fn clear(&self) {
    if self.clear {
        tracing::trace!("Clearing the screen");
        if let Err(err) = clearscreen::clear() { ... }
    }
}
```

```rust
// clearscreen-2.0.1 src/lib.rs:423-428
impl ClearScreen {
    /// Performs the clearing action, printing to stdout.
    pub fn clear(self) -> Result<(), Error> {
        let mut stdout = io::stdout();
        self.clear_to(&mut stdout)
    }
```

```rust
// src/ghci/mod.rs:167-168  (progress mode is TTY-gated)
} else if opts.has_experimental_feature(ExperimentalFeature::Progress)
    && std::io::stdout().is_terminal()
```
