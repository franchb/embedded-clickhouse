# embedded-clickhouse

[![Go Reference](https://pkg.go.dev/badge/github.com/franchb/embedded-clickhouse.svg)](https://pkg.go.dev/github.com/franchb/embedded-clickhouse)
[![Go Report Card](https://goreportcard.com/badge/github.com/franchb/embedded-clickhouse)](https://goreportcard.com/report/github.com/franchb/embedded-clickhouse)
[![codecov](https://codecov.io/gh/franchb/embedded-clickhouse/branch/main/graph/badge.svg)](https://codecov.io/gh/franchb/embedded-clickhouse)
[![CI](https://github.com/franchb/embedded-clickhouse/actions/workflows/ci.yml/badge.svg)](https://github.com/franchb/embedded-clickhouse/actions/workflows/ci.yml)

Run a real ClickHouse database locally on Linux or macOS as part of another Go application or test.

When testing, this provides a much higher level of confidence than mocking or using Docker. It requires no external dependencies beyond the Go toolchain — no Docker, no testcontainers, no pre-installed ClickHouse.

Inspired by [fergusstrange/embedded-postgres](https://github.com/fergusstrange/embedded-postgres). ClickHouse binaries are fetched directly from [official GitHub releases](https://github.com/ClickHouse/ClickHouse/releases).

## Why not testcontainers?

The story starts in a GitLab CI pipeline.

We had a Go service that talked to ClickHouse, and we wanted real integration tests — not mocks, not stubs, just actual SQL hitting an actual database. The natural choice was [testcontainers-go](https://golang.testcontainers.org/). We added the dependency, wrote the tests, pushed — and immediately hit a wall.

testcontainers-go needs a Docker daemon. In GitLab CI that means picking your poison:

**Docker-in-Docker (DinD)** — spin up a `docker:dind` service alongside your job. It works, but the service container must run with `privileged: true`. GitLab's shared runners disable privileged mode by default, and many self-hosted runner fleets lock it down for good reason: a privileged DinD container has unrestricted access to the host kernel. One misconfigured job and you're escalated to root on the runner.

**Docker-outside-of-Docker (DooD)** — mount `/var/run/docker.sock` from the host into the job container. Sounds safer, but the Docker socket is a root-equivalent backdoor. Any code running in the job can create a privileged container that bind-mounts `/` from the host. Security teams tend to notice this and the mount disappears from the runner config shortly after.

On top of the privilege problem, testcontainers spins up a sidecar called **Ryuk** — a reaper container responsible for cleaning up after crashed tests. Ryuk needs to bind a port and phone home to the test process. In CI networks with strict firewall rules, Ryuk silently fails to connect, producing cryptic `context deadline exceeded` errors that vanish on retry and reappear at random. Debugging it means grepping through nested container logs while your CI queue backs up.

We tried Podman as a rootless alternative. Podman rootless requires disabling Ryuk entirely (`TESTCONTAINERS_RYUK_DISABLED=true`), which means manual cleanup. Rootful Podman needs `ryuk.container.privileged=true`, which brings the privilege problem back through a different door.

At some point the complexity of the setup exceeded the complexity of the thing we were testing.

The insight from [fergusstrange/embedded-postgres](https://github.com/fergusstrange/embedded-postgres) was that you don't need Docker at all. ClickHouse ships as a single self-contained binary. You can download it, verify its checksum, and run it as a child process — no daemon, no socket, no sidecar, no privilege escalation. The binary starts in under a second, listens on a random port, and exits when your test exits.

`embedded-clickhouse` applies that same idea to ClickHouse. The binary is fetched once, cached in `~/.cache/embedded-clickhouse/`, and reused across test runs. In CI, you cache that directory and the download never happens again. No Docker. No privileged runners. No Ryuk.

## Installation

```bash
go get github.com/franchb/embedded-clickhouse
```

Requires Go 1.25+.

## Quick start

```go
func TestQuery(t *testing.T) {
    ch := embeddedclickhouse.NewServerForTest(t)
    // server starts automatically; t.Cleanup calls Stop()

    db, err := sql.Open("clickhouse", ch.DSN())
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()

    var result int
    db.QueryRow("SELECT 1").Scan(&result)
    // result == 1
}
```

## How to use

### Defaults

| Configuration      | Default                                              |
|--------------------|------------------------------------------------------|
| Version            | `DefaultVersion` = `V26_8` (see [Available versions](#available-versions)) |
| TCP Port           | Auto-allocated                                       |
| HTTP Port          | Auto-allocated                                       |
| Cache Path         | `$XDG_CACHE_HOME/embedded-clickhouse/` or `~/.cache/embedded-clickhouse/` |
| Data Path          | Temporary directory (removed on Stop)                |
| Start Timeout      | 30 seconds (240 seconds in cluster mode)             |
| Stop Timeout       | 10 seconds                                           |
| Logger             | `os.Stdout`                                          |

A zero `StartTimeout` or `StopTimeout` means "use the default". `Start` validates the
configuration before downloading anything and returns `ErrInvalidConfig` for an empty
version, a port above 65535, or `TCPPort == HTTPPort`.

### Minimal usage

```go
ch := embeddedclickhouse.NewServer()
if err := ch.Start(); err != nil {
    log.Fatal(err)
}
defer ch.Stop()

// ch.DSN()     => "clickhouse://127.0.0.1:<port>/default"
// ch.HTTPURL() => "http://127.0.0.1:<port>"
```

### Custom configuration

```go
ch := embeddedclickhouse.NewServer(
    embeddedclickhouse.DefaultConfig().
        Version(embeddedclickhouse.V26_3).
        TCPPort(19000).
        HTTPPort(18123).
        DataPath("/tmp/ch-data").
        StartTimeout(60 * time.Second).
        Logger(io.Discard).
        Settings(map[string]string{"max_threads": "2"}),
)
if err := ch.Start(); err != nil {
    log.Fatal(err)
}
defer ch.Stop()
```

### TestMain pattern

```go
var testServer *embeddedclickhouse.EmbeddedClickHouse

func TestMain(m *testing.M) {
    testServer = embeddedclickhouse.NewServer(
        embeddedclickhouse.DefaultConfig().
            Version(embeddedclickhouse.V26_8),
    )
    if err := testServer.Start(); err != nil {
        log.Fatal(err)
    }
    code := m.Run()
    testServer.Stop()
    os.Exit(code)
}

func TestSomething(t *testing.T) {
    db, err := sql.Open("clickhouse", testServer.DSN())
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()
    // ...
}
```

### Per-test with auto-cleanup

```go
func TestInsertAndSelect(t *testing.T) {
    ch := embeddedclickhouse.NewServerForTest(t)
    // Stop() is called via t.Cleanup (calling ch.Stop() yourself is fine too)

    db, err := sql.Open("clickhouse", ch.DSN())
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()

    db.Exec(`CREATE TABLE t (id UInt64) ENGINE = MergeTree() ORDER BY id`)
    db.Exec(`INSERT INTO t VALUES (1), (2), (3)`)

    var count int
    db.QueryRow("SELECT count() FROM t").Scan(&count)
    // count == 3
}
```

## Cluster mode

Cluster mode runs multiple ClickHouse replicas on localhost using embedded Keeper (Raft-based coordination built into the ClickHouse binary). No additional binaries or Docker containers needed.

This enables testing `ReplicatedMergeTree` tables with `ON CLUSTER` queries — the same binary handles both the database engine and the coordination layer.

### Quick start (cluster)

```go
func TestReplication(t *testing.T) {
    cluster := embeddedclickhouse.NewClusterForTest(t, 3)
    // 3 nodes start automatically; t.Cleanup calls Stop()

    db, err := sql.Open("clickhouse", cluster.DSN())
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()

    db.Exec(`CREATE TABLE t ON CLUSTER 'test_cluster' (id UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t', '{replica}')
        ORDER BY id`)
    db.Exec(`INSERT INTO t VALUES (1), (2), (3)`)

    // Read from a different replica:
    db1, _ := sql.Open("clickhouse", cluster.Node(1).DSN())
    defer db1.Close()
    db1.Exec("SYSTEM SYNC REPLICA t")

    var count int
    db1.QueryRow("SELECT count() FROM t").Scan(&count)
    // count == 3
}
```

### Manual Start/Stop (cluster)

```go
cluster := embeddedclickhouse.NewCluster(3)
if err := cluster.Start(); err != nil {
    log.Fatal(err)
}
defer cluster.Stop()

cluster.DSN()           // DSN for node 0
cluster.Node(0).DSN()   // same as above
cluster.Node(1).DSN()   // DSN for node 1
cluster.ClusterName()   // "test_cluster"
```

### Cluster defaults

| Configuration           | Default                 |
|-------------------------|-------------------------|
| Replicas                | User-specified (min 2)  |
| Start Timeout           | 240 seconds             |
| Memory per node         | ClickHouse default (no explicit limit) |
| Cluster name            | `test_cluster`          |
| All ports               | Auto-allocated          |

Each node requires 5 ports (TCP, HTTP, interserver HTTP, Keeper client, Keeper Raft), all auto-allocated on localhost, so `TCPPort`, `HTTPPort` and `DataPath` are rejected in cluster mode (`ErrClusterUnsupportedOption`). On CI machines running 3 replicas, cap memory per node via `Settings(map[string]string{"max_server_memory_usage": "1073741824"})` (see [Memory limits](#memory-limits)).

`Cluster.Stop` stops all nodes concurrently. Node handles obtained from `Node()`/`Nodes()` are invalidated by `Stop`: their accessors return `""`.

### Composability

embedded-clickhouse handles ClickHouse itself. For external dependencies (Kafka, S3, etc.), combine with testcontainers or docker-compose — ClickHouse connects to them via exposed ports.

## Custom builds

If you maintain a custom ClickHouse build (e.g., hosted on a private GitLab registry), you can point embedded-clickhouse at your archive instead of using official GitHub releases.

### From a local archive

```go
ch := embeddedclickhouse.NewServer(
    embeddedclickhouse.DefaultConfig().
        CustomArchivePath("/path/to/clickhouse-custom.tar.gz"),
)
```

The archive must be a `.tar.gz` containing a `clickhouse` binary (at any path — `clickhouse`, `bin/clickhouse`, or `usr/bin/clickhouse` all work). The binary is extracted once and cached for reuse.

### From a custom URL

```go
ch := embeddedclickhouse.NewServer(
    embeddedclickhouse.DefaultConfig().
        CustomArchiveURL("https://gitlab.example.com/api/v4/projects/my-project/packages/generic/clickhouse/26.3.3.20.1/clickhouse_26.3.3.20.1_linux_amd64.tar.gz"),
)
```

The archive is downloaded, extracted, and cached (keyed by URL). Subsequent runs skip the download.

For a private registry, pass credentials as a header rather than in the URL (URLs end up in logs and error messages; embedded-clickhouse masks them, but a header never appears there at all):

```go
ch := embeddedclickhouse.NewServer(
    embeddedclickhouse.DefaultConfig().
        CustomArchiveURL("https://gitlab.example.com/api/v4/projects/my-project/packages/generic/clickhouse/26.3.3.20.1/clickhouse_26.3.3.20.1_linux_amd64.tar.gz").
        DownloadHeader("PRIVATE-TOKEN", os.Getenv("GITLAB_TOKEN")),
)
```

Headers are sent only to `CustomArchiveURL` and `BinaryRepositoryURL`, never to the default GitHub source, and they are dropped as soon as a redirect leaves the original host (e.g. a registry redirecting to object storage).

### Checksum verification

Optionally verify the archive with SHA256 and/or SHA512:

```go
ch := embeddedclickhouse.NewServer(
    embeddedclickhouse.DefaultConfig().
        CustomArchiveURL("https://gitlab.example.com/.../clickhouse.tar.gz").
        SHA256("abc123...").
        SHA512("def456..."),
)
```

When no hash is provided, verification is skipped for custom assets. For standard downloads, the published `.sha512` is always checked; a missing checksum fails the download (`ErrSHA512Unavailable`) unless `AllowMissingChecksum(true)` is set.

Downloads are capped at 4 GiB (and the extracted binary at 4 GiB), and a download that receives no data for 2 minutes fails with `ErrDownloadStalled`.

### Priority

When multiple binary sources are configured, the first match wins:

1. `BinaryPath` — pre-extracted binary on disk
2. `CustomArchivePath` — local archive
3. `CustomArchiveURL` — remote archive
4. Standard GitHub release download

## Configuration reference

All configuration methods use a builder pattern with value receivers, so the original config is never mutated:

```go
base := embeddedclickhouse.DefaultConfig()
custom := base.Version(embeddedclickhouse.V26_3) // base is unchanged
```

| Method                     | Description                                              |
|----------------------------|----------------------------------------------------------|
| `Version(ClickHouseVersion)` | ClickHouse version to download and run                 |
| `TCPPort(uint32)`          | Native protocol port (0 = auto-allocate)                 |
| `HTTPPort(uint32)`         | HTTP interface port (0 = auto-allocate)                  |
| `CachePath(string)`        | Override binary cache directory                          |
| `DataPath(string)`         | Persistent data directory (survives Stop)                |
| `BinaryPath(string)`       | Use a pre-existing binary, skip download                 |
| `BinaryRepositoryURL(string)` | Custom mirror URL (default: GitHub releases)          |
| `CustomArchivePath(string)` | Local `.tar.gz` archive containing a ClickHouse binary  |
| `CustomArchiveURL(string)` | Remote URL to a `.tar.gz` archive (fully custom URL)     |
| `SHA256(string)`           | Expected SHA256 hex digest for custom archive verification |
| `SHA512(string)`           | Expected SHA512 hex digest for custom archive verification |
| `AllowMissingChecksum(bool)` | Tolerate a missing `.sha512` on a mirror (default: fail) |
| `DownloadHeader(key, value)` | HTTP header for custom download sources (e.g. auth)    |
| `StartTimeout(time.Duration)` | Max wait for server readiness (0 = default)           |
| `StopTimeout(time.Duration)`  | Max wait for graceful shutdown (0 = default)          |
| `Logger(io.Writer)`        | Destination for download progress and server stdout/stderr (`nil` silences both) |
| `Settings(map[string]string)` | Arbitrary ClickHouse server settings                  |

`Settings` keys must be valid XML element names and must not name an element embedded-clickhouse writes itself (`tcp_port`, `path`, `users`, `logger`, and in cluster mode `keeper_server`, `zookeeper`, `remote_servers`, `macros`, ...); such keys fail with `ErrInvalidSettingKey`.

embedded-clickhouse also writes `shutdown_wait_unfinished` as 5 seconds, so that `Stop` finishes within the default `StopTimeout`: ClickHouse 26.8 raised its default to 120 seconds. Pass your own value through `Settings` to change it.

## Available versions

<!-- versions:begin (generated from config.go by internal/cmd/chversions; do not edit) -->
| Constant | Version | Channel |
|----------|---------|---------|
| `V26_8` | 26.8.34.13-lts | LTS (default) |
| `V26_3` | 26.3.60.2-lts | LTS |
| `V26_1` | 26.1.12.23-stable | Stable (deprecated) |
| `V25_8` | 25.8.33.6-lts | LTS (deprecated) |
| `V25_3` | 25.3.14.14-lts | LTS (deprecated) |
<!-- versions:end -->

Each constant tracks the latest patch release of its line, so upgrading embedded-clickhouse can move a constant to a newer patch. Deprecated constants name lines that ClickHouse no longer patches ([support table](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md)); they keep working but will be removed in a future major version.

Any version string can be used — these constants are provided for convenience. Pass the full version from a [ClickHouse release tag](https://github.com/ClickHouse/ClickHouse/releases), e.g. `embeddedclickhouse.ClickHouseVersion("24.8.6.70-lts")`.

## Server accessors

After `Start()` returns successfully (they return `""` before `Start` completes and after `Stop`; `Started()` reports the state):

| Method      | Example return value                          |
|-------------|-----------------------------------------------|
| `TCPAddr()` | `"127.0.0.1:19000"`                          |
| `HTTPAddr()`| `"127.0.0.1:18123"`                          |
| `DSN()`     | `"clickhouse://127.0.0.1:19000/default"`     |
| `HTTPURL()` | `"http://127.0.0.1:18123"`                   |

`StartContext(ctx)` (and `Cluster.StartContext`) aborts the download and readiness wait when `ctx` is done. `Stop` called while a `Start` is in progress aborts that start. `EnsureBinary`/`EnsureBinaryContext` resolve and cache the binary without starting a server.

## Platform support

| OS     | Arch  | Asset type  |
|--------|-------|-------------|
| Linux  | amd64 | `.tgz` archive |
| Linux  | arm64 | `.tgz` archive |
| macOS  | amd64 | Raw binary  |
| macOS  | arm64 | Raw binary  |

The package also compiles on other platforms (e.g. Windows), where `Start` returns `ErrUnsupportedPlatform`.

## CI caching

The downloaded ClickHouse binary (~200MB for Linux, ~130MB for macOS) is cached at the cache path. In CI, cache this directory to avoid re-downloading on every run. Keying on `go.sum` starts a fresh cache whenever an embedded-clickhouse upgrade changes the pinned version:

```yaml
# GitHub Actions
- uses: actions/cache@v4
  with:
    path: ~/.cache/embedded-clickhouse
    key: clickhouse-${{ runner.os }}-${{ runner.arch }}-${{ hashFiles('go.sum') }}
```

## Memory limits

No server memory limit is imposed by default. ClickHouse uses its built-in ratio-based default (`max_server_memory_usage_to_ram_ratio = 0.9`). Since ClickHouse 26.6 that cap is recomputed while the server runs, as 0.9 × (the server's resident memory + the memory the kernel reports available), so several test servers on one host leave room for each other ([ClickHouse#104964](https://github.com/ClickHouse/ClickHouse/pull/104964)); `memory_worker_dynamic_hard_limit = 0` restores the fixed startup cap.

ClickHouse has two separate memory settings:

- **`max_server_memory_usage`** — server-wide ceiling across all queries and background operations
- **`max_memory_usage`** — per-query limit (set in user profiles, not server config)

For constrained CI environments, set an explicit server limit via `Settings()`:

```go
embeddedclickhouse.DefaultConfig().
    Settings(map[string]string{"max_server_memory_usage": "1073741824"}) // 1 GiB
```

Environments with less than 2 GB total RAM will be fragile regardless of settings — ClickHouse needs memory for internal overhead (mark cache, logs, query cache, metadata) beyond query execution.

## Upgrading to ClickHouse 26.8

`DefaultVersion` is 26.8 since embedded-clickhouse v0.6.0. Changes since 26.3 that tests are likely to notice ([changelog](https://github.com/ClickHouse/ClickHouse/blob/master/CHANGELOG.md)):

- **CPU requirement.** x86-64 release builds target x86-64-v3 (AVX2, BMI2, FMA) since 26.6 ([#105019](https://github.com/ClickHouse/ClickHouse/pull/105019)); there is no older-CPU build among the release assets. On such a CPU the server dies with SIGILL, and `Start` returns `ErrServerExited` saying so. Use `V26_3`, or `BinaryPath` with a build for that CPU. Check a Linux host with `grep -o -w avx2 /proc/cpuinfo | head -1`; QEMU/KVM guests need a CPU model that exposes it (e.g. `-cpu host`).
- **`max_insert_threads` defaults to the number of cores** (26.8, [#109000](https://github.com/ClickHouse/ClickHouse/pull/109000)). `INSERT ... SELECT` can create more parts, and rows land in a different order; queries that relied on insertion order without `ORDER BY` can change results. `Settings` cannot change it (it is a query setting): pass `max_insert_threads=1` in the DSN or per query, or set `compatibility`.
- **`date_time_input_format` defaults to `best_effort`** (26.5, [#89334](https://github.com/ClickHouse/ClickHouse/pull/89334)): strings such as `Apr 15, 2020 10:30:00` now parse as `DateTime` instead of failing, so a test expecting a parse error fails.
- **Shutdown wait.** 26.8 waits up to 120 seconds for open connections when stopping ([#110838](https://github.com/ClickHouse/ClickHouse/pull/110838)); embedded-clickhouse sets 5 seconds (see `Settings` above).

## How it works

1. **Download** — fetches the ClickHouse binary from GitHub releases (or a configured mirror) on first use
2. **Verify** — checks SHA512 hash for downloaded assets
3. **Cache** — stores the extracted binary at `~/.cache/embedded-clickhouse/` for reuse
4. **Configure** — generates a minimal XML config with allocated ports and a temp data directory
5. **Start** — launches `clickhouse server` as a child process
6. **Health check** — every 100ms, asks the server over HTTP for a random per-launch instance ID written into its config (read from `system.macros`) until it answers; if the process exits first, `Start` fails immediately with `ErrServerExited`
7. **Port conflicts** — auto-allocated ports are released before ClickHouse binds them, so a concurrent process (say, another test's server under `go test -parallel`) can take one in between. The config sets `<listen_host>127.0.0.1</listen_host>`, so ClickHouse then exits instead of running without that listener, and the health check rejects anything else answering on the HTTP port, since only this server knows its instance ID. Either way `Start` reports `ErrPortConflict`, after up to 3 attempts with fresh ports when every port was auto-allocated. `listen_try` and `listen_reuse_port` are reserved, since either would defeat this. The server listens on `127.0.0.1` only, not on `::1`. You can set `listen_host` through `Settings`, but the value must cover `127.0.0.1` (for example `0.0.0.0`), because the health check and every address accessor dial `127.0.0.1`
8. **Stop** — sends SIGTERM, waits for graceful shutdown, then SIGKILL if needed; cleans up the temp directory. A server that crashed (e.g. SIGSEGV, OOM kill) before `Stop` is reported as an error, not as a clean stop

## License

Apache 2.0
