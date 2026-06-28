# Code Review — Full Codebase Audit

**Date:** 2026-06-11 · **Branch:** `review` @ `dba7511` · **Scope:** all root `*.go` files

**Method:** 9 independent finder angles (5 correctness, 3 cleanup, 1 altitude) → 68 raw
candidates → deduped to 24 → one verifier per candidate → fresh-eyes sweep (+8) → 4
strongest sweep candidates verified and folded into the list below. **All 24 numbered
findings below were CONFIRMED or PLAUSIBLE; zero REFUTED** (several verified empirically
via cross-compile and `go run`). The 4 lower-confidence extras were surfaced but not
individually verified.

Each item below is self-contained: mechanism, trigger, and suggested fix. Check off as
resolved. Line numbers are valid at commit `dba7511`.

---

## P1 — Critical correctness & security

### [ ] 1. `Start` never monitors the child process; readiness is HTTP-ping-only
**Where:** `clickhouse.go:201` (`waitForReady` is the only check after `startProcess`); `health.go:55` (`ping` accepts any HTTP 200)
**Bug:** Nothing watches `cmd.Wait` during startup.
- A child that dies instantly (busy port, bad setting, corrupt binary, missing glibc) burns the full `startTimeout` (30 s single / 240 s cluster) polling a dead port, then returns a misleading `did not become ready: context deadline exceeded`. The real exit error is discarded (`clickhouse.go:193`, `//nolint:errcheck`).
- Worse: with a user-fixed `HTTPPort`, a **foreign** server answering 200 on that port (e.g. another ClickHouse) makes `Start` return success with `e.cmd` a zombie — queries then hit the wrong server.

**Fix:** run `cmd.Wait()` in a goroutine; `select` on {process exited, ready, ctx done}. On early exit, return the exit error immediately. Optionally verify identity via `/ping` only after confirming the child is still alive, or query a nonce setting.

### [ ] 2. Cross-process cache corruption: no file locking, deterministic `.tmp` names
**Where:** `download.go:17` (`downloadMu` is process-local), `download.go:189` (`asset.filename+".tmp"`), `download.go:190` (deferred `os.Remove` of the shared path), `extract.go:79-81` (`destPath+".tmp"`, `O_CREATE|O_WRONLY|O_TRUNC`, no `O_EXCL`), `download.go:140` (stat-only freshness)
**Bug:** Two `go test` processes with a cold cache truncate/delete each other's in-flight files. Where verification is skipped (missing `.sha512`, unhashed `CustomArchiveURL`), an interleaved file is renamed into the cache and trusted forever by the stat-only check.
**Fix:** per-cache-entry advisory lock (`flock` on a `.lock` file) around download+extract, plus unique temp names (`os.CreateTemp` in the same dir) with rename-into-place.

### [ ] 3. In-process race: `ensureCustomArchiveFromPath` skips `downloadMu`
**Where:** `download.go:45-79` (no lock; contrast `download.go:97-103` and `144-150` which lock and double-check)
**Bug:** Two concurrent `Start`s sharing one `CustomArchivePath` both stat-miss `binPath` (`download.go:67`) and both extract to the identical `binPath+".tmp"` → spurious rename failures, or a truncated binary persisted into the content-addressed cache.
**Fix:** take `downloadMu` (or the per-key lock from item 2) and re-stat after acquiring, exactly like the other two ensure paths.

### [ ] 4. Checksum verification fails open, and strictness is inverted
**Where:** `download.go:275-281` (non-200 on `.sha512` → warn + `return nil`; silent when logger is nil), `download.go:311-316` (`parseSHA512` requires exactly `<hash> <exact-filename>`), `download.go:336-338` (custom-archive hashes optional)
**Bug:** A mirror that 404s the checksum bypasses verification entirely (unverified binary cached + executed forever), while a mirror serving a *valid but differently formatted* checksum (bare hash, `hash *file`, `hash ./file`) hard-fails every Start with `ErrSHA512NotFound`. Less verification data succeeds; slightly different format aborts.
**Fix:** make missing checksum a hard error by default (opt-out flag for air-gapped mirrors); accept the common sha512sum output variants (bare hash, `*file`, `./file` prefixes).

### [ ] 5. Credential leak: full `customArchiveURL` in logs and error chains
**Where:** `download.go:105` (`logf(... "Downloading ClickHouse from %s", cfg.customArchiveURL)`; default logger is `os.Stdout`, `config.go:54`), `download.go:240,245` (URL baked into wrapped errors)
**Bug:** The documented private-GitLab `CustomArchiveURL` use case (README:228,246; `gitlab_integration_test.go`) offers no auth-header option — `downloadFile` uses a bare `httpClient.Get`, so any credentials must ride in the URL (`oauth2:glpat-…@` or `?private_token=…`). Such tokens land in CI logs on every cold start and in `tb.Fatal` output on any failure. No redaction exists.
**Fix:** log/wrap `u.Redacted()` (and strip query params or redact known token params); longer-term, add an auth-header option so credentials don't ride in the URL.

### [ ] 6. Cluster ignores `DataPath`/`TCPPort`/`HTTPPort` and deletes "persistent" data
**Where:** `cluster.go:126-133` (ports always auto-allocated), `cluster.go:147` (always `os.MkdirTemp`), `cluster.go:223-225` (`Stop` unconditionally `os.RemoveAll`s node dirs — contrast single-node guard `clickhouse.go:235`), `config.go:84` (doc: data "survives Stop")
**Bug:** User-configured data path and ports are silently ignored in cluster mode, and `Stop` deletes node data despite the documented persistence contract. No error or warning.
**Fix:** either honor the options (per-node subdirs under `dataPath`, `tcpPort+i` strides) or return a validation error from `NewCluster` when unsupported options are set. Never `RemoveAll` when a user data path is in play.

---

## P2 — High-impact correctness

### [ ] 7. Standard-binary cache key omits the repository URL
**Where:** `cache.go:35-38` (key = version+GOOS+GOARCH), contrast `download.go:90-91` (custom key = URL+digests); stat-hit at `download.go:138-141` before the URL is ever consulted (line 157)
**Bug:** A binary fetched from mirror A (possibly unverified, see item 4) is silently served forever for all configs, including the default GitHub repo. Switching mirrors never re-downloads.
**Fix:** include a digest of `binaryRepositoryURL` in `cachedBinaryPath`.

### [ ] 8. `Config` is never validated or defaulted at use
**Where:** `clickhouse.go:77-86` (`NewServer` stores cfg verbatim), `clickhouse.go:198` (`context.WithTimeout(…, 0)` born expired; only `NewCluster` backfills via `startTimeoutSet`, `cluster.go:59-61`), `process.go:75` (`time.After(0)` → instant SIGKILL + `ErrStopTimeout`), `platform.go:89-91` (version `""` → empty URL → `unsupported protocol scheme`), `config.go:66-76` (port setters store raw `uint32`; no ≤65535 or tcp≠http check; straight into XML via `server_config.go:21-22`)
**Bug:** `Config{}.TCPPort(9000)` compiles naturally (builder methods chain off any value) and fails opaquely — after a full ~300 MB download.
**Fix:** one `resolveConfig()` at Start: zero-means-default for timeouts (kills the `startTimeoutSet` sentinel), explicit `version must not be empty` error, port range/conflict validation. Fail before downloading.

### [ ] 9. Crashed server reported as clean stop (`ExitCode() == -1`)
**Where:** `process.go:84-89` (`if exitErr.ExitCode() == -1 || exitErr.ExitCode() == 143 { return nil }`)
**Bug:** In `os/exec`, `-1` means killed by **any** signal. A child that crashed (SIGSEGV/SIGABRT) or was OOM-killed before `Stop` is a zombie; `Getpgid` still succeeds, our SIGTERM is a no-op, `Wait` yields `-1`, and `Stop`/`tb.Cleanup` report success — the suite stays green despite a mid-test crash.
**Fix:** inspect `exitErr.Sys().(syscall.WaitStatus)`: accept only `Signaled() && Signal() == SIGTERM` (and exit code 143) as clean.

### [ ] 10. Package does not compile on Windows
**Where:** `process.go:45,61,66,77` (`syscall.SysProcAttr{Setpgid}`, `syscall.Getpgid`, `syscall.Kill`) — no build tags anywhere, no `process_windows.go`
**Bug:** Verified: `GOOS=windows go build ./...` fails with 4 errors. Consumers that cross-build break at import time instead of getting the designed runtime `ErrUnsupportedPlatform` (`platform.go:64`).
**Fix:** build-tagged `process_unix.go` / `process_windows.go` (stub returning `ErrUnsupportedPlatform`) behind a small start/stop seam.

### [ ] 11. Data race: one logger shared as Stdout/Stderr of N node processes
**Where:** `cluster.go:141-144,159` (same writer instance per node), `process.go:42-43` (assigned raw)
**Bug:** For any non-`*os.File` writer, `os/exec` spawns one pipe-copy goroutine per process; N goroutines `Write` concurrently to one unsynchronized writer (`bytes.Buffer`, `t.Logf` adapters…). Console logging is on (`cluster_config.go:14-17`), so output flows. `go test -race` fails. Single-node is safe only because exec dedupes identical Stdout/Stderr.
**Fix:** wrap the user writer in a mutex-guarded writer (or per-node prefixed writers) before handing it to `exec.Cmd` in cluster mode.

### [ ] 12. Unbounded decompression and download size (gzip bomb / disk fill)
**Where:** `extract.go:86` (`io.Copy` of tar entry; `hdr.Size` never checked), `download.go:253` (`io.Copy` of response body, no cap)
**Bug:** A compromised mirror or attacker-controlled `CustomArchiveURL` (verification optional/fail-open, item 4) can fill the disk hosting the cache (typically `$HOME`), wedging CI.
**Fix:** `io.LimitReader` with a sane ceiling (e.g. 4 GiB decompressed, configurable) on both paths; reject `hdr.Size` above the cap up front.

---

## P3 — Correctness, ergonomics & API contract

### [ ] 13. `NewServerForTest` cleanup fails tests that call `Stop` themselves
**Where:** `clickhouse.go:99-102` (Cleanup → `tb.Errorf` on any Stop error), `clickhouse.go:224-225` (`ErrServerNotStarted` when already stopped); same in `cluster.go:80-83` / `cluster.go:207-208` (`ErrClusterNotStarted`)
**Bug:** A test that legitimately stops the server mid-test (e.g. to exercise app behavior after DB shutdown) goes red at teardown.
**Fix:** in the Cleanup, ignore `errors.Is(err, ErrServerNotStarted)` / `ErrClusterNotStarted`.

### [ ] 14. `Cluster.Stop` leaves stale, unrestartable node handles
**Where:** `cluster.go:229-230` (resets only `started`/`cmd`; ports untouched — contrast `clickhouse.go:241-244`), `cluster.go:178` (`clusterManaged: true`, never cleared), `cluster.go:253` (`Node` returns live pointers)
**Bug:** A handle saved via `Node(i)` before Stop keeps returning the dead port from `TCPAddr()`/`DSN()` (OS may reassign it → queries silently hit a wrong server), and `Start()` on it returns `ErrClusterManaged` forever.
**Fix:** zero port fields in `Cluster.Stop` (ideally by routing through a shared `stopNode`, see Cleanup 1).

### [ ] 15. `".."` path guard false-positives valid paths, misses real traversal
**Where:** `extract.go:70-72` (`filepath.Clean` then `strings.Contains(destPath, "..")`)
**Bug:** Verified empirically: `CachePath("/data/ci..runner/cache")` or relative `"../shared-cache"` → permanent `ErrInvalidPath`; meanwhile a genuine `/safe/dir/../escape` is *cleaned away* before the check and passes. The guard only harms legitimate paths.
**Fix:** validate the *relationship* instead: resolve `destPath` and ensure it stays within the intended cache root (`filepath.Rel` + check the result doesn't start with `..`).

### [ ] 16. User `Settings` can silently duplicate structural config elements
**Where:** `server_config.go:74` (`^[a-zA-Z][a-zA-Z0-9_]*$` accepts `tcp_port`, `path`, `users`, `logger`…), template line 21 vs settings range lines 49-51; same hole + more collidable keys in `cluster_config.go:103-104,189-195` (`keeper_server`, `zookeeper`, `remote_servers`, `macros`)
**Bug:** `Settings{"tcp_port":"9100"}` emits a second top-level `<tcp_port>` — ClickHouse's resolution diverges from what `TCPAddr()`/`DSN()` report, with no error. (Value escaping is fine: `xmlEscape` is used.)
**Fix:** denylist of template-managed element names in `validSettingKey`'s caller; return `ErrInvalidSettingKey`.

### [ ] 17. Address accessors return `127.0.0.1:0` before Start / after Stop
**Where:** `clickhouse.go:250-278` (`TCPAddr`/`HTTPAddr`/`DSN`/`HTTPURL` — no `started` check; docs state no precondition); `clickhouse.go:243-244` (Stop zeroes ports, making post-Stop calls equally silent)
**Bug:** `NewServer().DSN()` yields a well-formed `clickhouse://127.0.0.1:0/default` that `sql.Open` accepts; failure surfaces later as an opaque dial error far from the ordering mistake.
**Fix:** return `("", ErrServerNotStarted)` (breaking) or at minimum document the precondition and add a `Started()` accessor.

### [ ] 18. `allocatePort` TOCTOU + no uniqueness across 5×N allocations *(PLAUSIBLE)*
**Where:** `process.go:14-33` (bind :0, close, return; comment acknowledges the race), `cluster.go:275-301` (5 sequential calls per node, no dedup over the set)
**Bug:** Kernel may hand a just-released port to a later allocation or a concurrent process; duplicate role/node ports → opaque 240 s cluster timeout. Linux bind(:0) randomization makes same-process duplicates rare but unguarded.
**Fix:** track allocated ports in a set and re-allocate on duplicates; consider holding listeners open until just before spawn.

---

## P4 — Cleanup & design debt (all verified)

### [ ] C1. Cluster duplicates the single-node lifecycle core
`Cluster.Start` re-implements temp-dir/config/spawn/rollback/readiness and fabricates nodes by writing private fields (`cluster.go:168-179`); `Cluster.Stop` duplicates `Stop`'s body inline (`cluster.go:217-231`); `EmbeddedClickHouse.Start/Stop` are fenced with `clusterManaged` guards (`clickhouse.go:113,220`). Divergence already shipped: items 6, 14. Dead write-only fields: `interserverPort`/`keeperPort`/`keeperRaftPort` (`clickhouse.go:69-71`, written `cluster.go:175-177`, never read).
**Fix:** internal `startNode`/`stopNode` core shared by both paths; delete `clusterManaged` and the dead fields. *Resolving this absorbs items 6, 9, 13, 14 fixes naturally.*

### [ ] C2. `keeperReady` is byte-identical to `ping`; poll loops duplicated
`cluster.go:365-380` ≡ `health.go:41-56`; `waitForKeeperQuorum` re-implements `waitForReady`'s immediate-check-then-ticker loop. Only URL, interval, and error wrapping differ.
**Fix:** one `pollUntilHTTP200(ctx, client, url, interval)` helper; callers keep their error wrapping.

### [ ] C3. `writeClusterNodeConfig` duplicates `writeServerConfig` (already drifted)
Byte-identical validation error string (`server_config.go:104` / `cluster_config.go:191`), same MkdirAll loop, verbatim logger/users/profiles/quotas XML blocks. Drift: cluster removes a half-written config on template error (`cluster_config.go:248-253`), server leaks it (`server_config.go:136-139`). `buildClusterTopology` bypasses `mergeSettings` (`cluster_config.go:173-180`) — latent because `defaultServerSettings` is empty today (`server_config.go:58-60`; if it stays empty, delete it and `mergeSettings`).
**Fix:** shared `validateSettings`/`makeServerDirs`/`writeConfigFile` helpers + a shared base template fragment.

### [ ] C4. Redundant full-file hashing I/O
Custom archives: up to **3 full reads per Start, even warm-cache** — `verifyCustomArchive` does separate SHA256/SHA512 passes (`download.go:339,350`), then `fileSHA256` again for the cache key (line 55); the `os.Stat(binPath)` fast path comes only at line 67, *after* hashing. Downloads: archive written then re-read entirely for SHA512 (`download.go:253` → `293`).
**Fix:** single pass with `io.MultiWriter(sha256, sha512)`; hash during download via `io.TeeReader`. Note: stat-before-hash requires rethinking the content-addressed key for the local-path flow. Also dedupe `fileSHA256`/`fileSHA512` into one `fileDigest(path, hash.Hash)`.

### [ ] C5. `Start` holds the write lock for its entire duration
`clickhouse.go:110-111`: `e.mu.Lock()` across download (minutes; 10-min HTTP timeout) + readiness wait. `Stop` needs the same lock (line 217) → a watchdog calling `Stop` deadlocks until Start finishes; accessors block too. No `context.Context` parameter on `Start` as an alternate abort path. Same shape in `Cluster.Start` (`cluster.go:91-92`, up to 240 s).
**Fix:** lock only to check-and-set a `starting` flag and to publish results; and/or add `StartContext(ctx)`.

### [ ] C6. `Cluster.Stop` is sequential — teardown is sum(), not max()
`cluster.go:213-232`: each `stopProcess` waits up to `stopTimeout` (default 10 s, `config.go:53`) before the next node is signalled; a hung 3-node cluster takes 30 s+. Start already waits for readiness in parallel (`cluster.go:310-326`). No semantic ordering requirement found (every node co-hosts a Keeper; sequential stop actually leaves the last node quorum-less anyway).
**Fix:** SIGTERM all process groups first, then wait concurrently (`errgroup`/WaitGroup), joining errors.

---

## Lower-confidence extras (from the sweep; not individually verified)

- `download.go:20` — `http.Client{Timeout: 10min}` caps the *entire body transfer*; a ~300 MB archive on a slow link can never complete and every retry restarts from zero. Consider a per-request context with progress-based deadline instead.
- `clickhouse.go:184` / `cluster.go:142` — `Logger(nil)` silences download logs (`logf` no-ops) but substitutes `os.Stdout` for the *server process* output — the explicit nil logger re-enables the noisiest stream.
- `cluster.go:261` — `Nodes()` returns the live internal slice (no copy); callers can corrupt cluster state through it.
- `gitlab_integration_test.go:49` — `requestedPath` written in the httptest handler goroutine, read/reset by the test goroutine without synchronization; may trip `go test -race` intermittently.

## Suggested resolution order

1. **Item 1 + C1 together** (process-exit monitoring + shared node core) — biggest payoff; absorbs/shrinks 6, 9, 13, 14.
2. **Items 2, 3, 7** (cache locking + keying) — one coherent "cache integrity" PR.
3. **Items 4, 5, 12** (verification policy, credential redaction, size caps) — one "download security" PR.
4. **Item 8** (`resolveConfig` validation) — small, high-leverage UX fix.
5. **Items 10, 11** (Windows build tags, logger wrapping) — independent, mechanical.
6. Remaining P3 + cleanup items opportunistically.
