# Cross-process download race fix

Issue: [#23](https://github.com/franchb/embedded-clickhouse/issues/23)

## Problem

`embedded-clickhouse` v0.4.0 serializes binary downloads with a single
in-process `sync.Mutex` (`downloadMu` in `download.go`). When `go test ./...`
runs packages in separate processes, every `TestMain` instantiates its own
mutex — they do not interlock across processes.

Several temp paths in the download/extract path are deterministic and
shared across processes:

- `archivePath := binPath + ".tar.gz.tmp"` — `ensureCustomArchiveFromURL`
- `archivePath := filepath.Join(dir, asset.filename + ".tmp")` — standard archive download
- `tmp := binPath + ".tmp"` — raw binary download (in `downloadRawBinary`)
- `tmp := destPath + ".tmp"` — extraction (in `writeExecutable`, reached from every path that extracts a tar.gz, including `ensureCustomArchiveFromPath` for a purely local archive)

When two processes hit the same temp path concurrently, their `os.Create` /
`os.OpenFile(O_TRUNC)` + `io.Copy` calls interleave bytes. SHA verification
trips on at least one; even if it doesn't, the extracted binary is corrupt. The user-facing failures
in CI:

```
attempt 1: panic: start embedded clickhouse: ... fork/exec
           /builds/.../custom-...: text file busy
attempt 2: panic: start embedded clickhouse: SHA256 mismatch:
           expected fa2547b7..., got 88e66dce...
```

`text file busy` (ETXTBSY) is the kernel refusing to `exec` a file that
another process still has open for write.

## Goals

- One process downloads; sibling processes wait and reuse the cached binary.
- The fix is transparent to callers — no public API change.
- The fix covers every code path that writes to a deterministic
  per-binary temp file, not just the `CustomArchiveURL` path mentioned in
  the issue. That includes `ensureCustomArchiveFromPath`, which does no
  network I/O but still extracts to `binPath + ".tmp"` and races the same
  way under cold cache.
- Existing in-process behavior is unchanged for single-process callers.

## Non-goals

- Windows support. The project targets Linux + macOS only.
- Distributed locking across machines (NFS-shared cache is out of scope and
  will surface as an error rather than silently degrade).

## Design

### Layered locking

Three layers, cheapest to most expensive:

1. **Cache hit fast path** (existing) — `os.Stat(binPath)` before any
   locking. Unchanged.
2. **In-process mutex** (existing `downloadMu`) — kept as-is. Serializes
   goroutines in the same process before they touch the filesystem.
3. **Per-binary OS file lock** (new) — `unix.Flock(fd, LOCK_EX)` on
   `<binPath>.lock`. Serializes processes.

After acquiring the file lock, re-check the cache one more time (a third
double-check). The previous holder almost certainly just placed the binary,
in which case the new holder returns immediately without downloading.

The critical section under the file lock covers download → SHA verify →
extract → atomic rename. Only the lock holder writes the temp path; the
corruption window from the issue does not exist anymore.

### Lock files

- One lock file per cache entry: `<binPath>.lock`.
- Created with `O_CREATE | O_RDWR`, mode `0o644`.
- Live forever in the cache dir. Never removed (deleting a lock file is
  itself a race).
- Lock files are tiny (zero bytes — flock holds no payload).

For different ClickHouse versions or different custom-URL hashes, the
`binPath` differs, so the lock file differs, so unrelated downloads do not
serialize on each other.

### Components

#### New file: `lock_unix.go`

```go
//go:build linux || darwin

package embeddedclickhouse

type fileLock struct {
    f *os.File
}

// acquireFileLock opens path with O_CREATE|O_RDWR and takes LOCK_EX.
// Returns ErrDownloadLockTimeout if the lock cannot be acquired within timeout.
func acquireFileLock(path string, timeout time.Duration) (*fileLock, error)

func (l *fileLock) release() error // flock LOCK_UN, then Close
```

Timeout implementation: spawn a goroutine that performs the blocking
`unix.Flock(fd, LOCK_EX)` call and signals on a channel. The main path
selects on that channel and `time.After(timeout)`. On timeout, close the fd
to unblock the syscall (`Flock` then returns `EBADF`); the goroutine drains
its result and exits.

A new sentinel `ErrDownloadLockTimeout` is added to the existing error vars
so callers can use `errors.Is`.

#### Modified file: `download.go`

All three call sites that may write to `<binPath>.tmp` gain the file-lock
layer:

- `ensureCustomArchiveFromURL` (currently line ~97)
- `ensureStandardBinary` (currently line ~144)
- `ensureCustomArchiveFromPath` (currently line ~45) — has no network I/O
  but still extracts to `<binPath>.tmp` via `writeExecutable`, racing the
  same way under cold cache.

`ensureCustomArchiveFromPath` does not currently take `downloadMu`. The
fix adds the same in-process-mutex + file-lock pair to it for consistency,
so cold-cache extracts of a local archive are also serialized across
processes.

The pattern at each site, immediately after the existing `downloadMu.Lock()`
(or new lock acquisition for `ensureCustomArchiveFromPath`) and the
second `os.Stat` double-check:

```go
lockPath := binPath + ".lock"
fl, err := acquireFileLock(lockPath, downloadLockTimeout)
if err != nil {
    return "", err
}
defer fl.release()

// Triple-check: a sibling process may have just placed the binary.
if _, err := os.Stat(binPath); err == nil {
    return binPath, nil
}

// ... existing download / verify / extract logic
```

The lock timeout is a package-private constant `downloadLockTimeout = 15 *
time.Minute` (HTTP timeout 10 min + extraction headroom). Hard-coded, no
config knob.

`downloadMu` stays — layered outside the file lock — so two goroutines in
the same process do not both syscall before one of them blocks.

#### No changes

- `ensureBinary` precedence logic (`BinaryPath > CustomArchivePath > CustomArchiveURL > standard`) is unchanged.
- `cache.go` is unchanged.
- `extract.go` is unchanged. `writeExecutable`'s deterministic
  `destPath + ".tmp"` is no longer a problem because every caller now
  holds `<destPath>.lock` for the entire write.
- Public API (`Config`, `Server`, etc.) is unchanged.

### Data flow

Two processes A and B both call `ensureCustomArchiveFromURL` for the same
URL with a cold cache:

```
A: stat(binPath)                → ENOENT
A: downloadMu.Lock()            → ok
A: stat(binPath)                → ENOENT
A: open(<binPath>.lock)         → fd_A
A: flock(fd_A, LOCK_EX)         → ok (holds lock)
A: stat(binPath)                → ENOENT
A: download → verify → extract  → binPath now exists
A: flock(fd_A, LOCK_UN); close  → lock released
A: return binPath

B: stat(binPath)                → ENOENT (started before A finished)
B: downloadMu.Lock()            → (different process — no contention)
B: stat(binPath)                → ENOENT
B: open(<binPath>.lock)         → fd_B
B: flock(fd_B, LOCK_EX)         → BLOCKS until A releases
B: (after A releases)           → ok
B: stat(binPath)                → EXISTS (A just placed it)
B: flock(fd_B, LOCK_UN); close
B: return binPath               (no download, no extract)
```

Three goroutines in a single process A:

```
G1: stat(binPath)               → ENOENT
G1: downloadMu.Lock()           → ok
G2: downloadMu.Lock()           → BLOCKS
G3: downloadMu.Lock()           → BLOCKS
G1: stat(binPath)               → ENOENT
G1: open + flock                → ok
G1: download → ... → place binary
G1: release flock
G1: downloadMu.Unlock()
G2: downloadMu.Lock()           → ok
G2: stat(binPath)               → EXISTS (G1 just placed it)
G2: return binPath              (no flock, no download)
G3: same as G2
```

This is why the in-process mutex is layered outside the file lock: G2 and G3
never even open the lock file.

### Error handling

| Failure                                | Behavior                                                                                                                                                                |
|----------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Lock file open fails (EACCES, ENOSPC)  | Wrap and return. Surfaces a real disk problem; do not silently bypass.                                                                                                  |
| `flock` syscall returns error          | Return wrapped error including the lock path.                                                                                                                           |
| Lock not acquired within 15 min        | Return `ErrDownloadLockTimeout`. Reachable via `errors.Is`.                                                                                                             |
| Process crashes while holding the lock | Kernel releases the lock when the fd closes on process exit. Next caller proceeds. No stale state on disk.                                                              |
| `flock` not supported (NFS/FUSE)       | `unix.Flock` returns ENOSYS or EINVAL. Surface as wrapped error. The user can work around it by setting `BinaryPath` (already-cached binary) or pointing the cache at a local filesystem. |
| Download fails inside the locked region | Existing handling unchanged. `defer os.Remove(archivePath)` removes the corrupt temp file; `defer fl.release()` releases the lock. The next caller takes the lock and retries cleanly. |

The corrupt-temp scenario from the issue is gone by construction — only the
lock holder writes the temp path.

## Testing

### `lock_unix_test.go` (new)

Unit tests for `acquireFileLock`:

- **Two-goroutine contention** — first goroutine acquires the lock and holds
  it for 100 ms; second goroutine starts to acquire while the first holds it
  and observes a wait of approximately 100 ms.
- **Timeout** — first goroutine holds the lock; second uses a 50 ms timeout
  and gets `ErrDownloadLockTimeout`.
- **Cross-process release on exit** — spawn a subprocess
  (`exec.Command(os.Args[0], "-test.run=TestLockHolderHelper")`, gated by an
  env var) that opens the lock and exits without releasing. Parent then
  acquires the lock without blocking.

### `download_test.go` (additions)

- **Cross-process download race regression** — the actual scenario from the
  issue. `httptest.NewServer` serves a tar.gz with a small deliberate
  `time.Sleep` to widen the race window. Spawn N subprocesses (the same
  helper-binary pattern, gated by an env var) all calling
  `ensureCustomArchiveFromURL` against a shared `t.TempDir()` cache and the
  test server URL. Assertions:
  - All N exit zero.
  - All N report the same binary path.
  - The cached binary's SHA256 matches the SHA256 of the binary inside the
    served archive.
  - The HTTP server records exactly one full archive download (not N).
- **Cross-process local-archive extract race regression** — same shape as
  above but for `ensureCustomArchiveFromPath`. N subprocesses point at a
  shared local tar.gz with a cold shared cache; assert all N return the
  same binary path with matching SHA256, no panics, no ETXTBSY.
- **In-process layered-lock test** — N goroutines in one process call
  `ensureCustomArchiveFromURL` against a slow `httptest.NewServer` that
  counts requests. Assert: server saw exactly one full request, all N
  goroutines returned the same binary path.

Existing `TestEnsureBinary_*` tests remain untouched.

## Build constraints

The new `lock_unix.go` carries `//go:build linux || darwin`. The project
already states Linux + macOS support; no Windows-only stub is added. If a
build for an unsupported OS is attempted in the future, the missing symbol
will be a clean compile error pointing at the platform-support boundary.

## Dependencies

Promote `golang.org/x/sys` from indirect to direct in `go.mod`. No new
modules. No third-party flock library.
