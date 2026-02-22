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
| Version            | `V25_8` (25.8.16.34-lts)                            |
| TCP Port           | Auto-allocated                                       |
| HTTP Port          | Auto-allocated                                       |
| Cache Path         | `$XDG_CACHE_HOME/embedded-clickhouse/` or `~/.cache/embedded-clickhouse/` |
| Data Path          | Temporary directory (removed on Stop)                |
| Start Timeout      | 30 seconds                                           |
| Stop Timeout       | 10 seconds                                           |
| Logger             | `os.Stdout`                                          |

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
        Version(embeddedclickhouse.V25_3).
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
            Version(embeddedclickhouse.V25_8),
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
    // Stop() is called via t.Cleanup

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

## Configuration reference

All configuration methods use a builder pattern with value receivers, so the original config is never mutated:

```go
base := embeddedclickhouse.DefaultConfig()
custom := base.Version(embeddedclickhouse.V25_3) // base is unchanged
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
| `StartTimeout(time.Duration)` | Max wait for server readiness                         |
| `StopTimeout(time.Duration)`  | Max wait for graceful shutdown                        |
| `Logger(io.Writer)`        | Destination for server stdout/stderr                     |
| `Settings(map[string]string)` | Arbitrary ClickHouse server settings                  |

## Available versions

| Constant | Version               | Channel |
|----------|-----------------------|---------|
| `V26_1`  | 26.1.3.52-stable      | Stable  |
| `V25_8`  | 25.8.16.34-lts        | LTS (default) |
| `V25_3`  | 25.3.14.14-lts        | LTS     |

Any version string can be used — these constants are provided for convenience. Pass the full version from a [ClickHouse release tag](https://github.com/ClickHouse/ClickHouse/releases), e.g. `embeddedclickhouse.ClickHouseVersion("24.8.6.70-lts")`.

## Server accessors

After `Start()` returns successfully:

| Method      | Example return value                          |
|-------------|-----------------------------------------------|
| `TCPAddr()` | `"127.0.0.1:19000"`                          |
| `HTTPAddr()`| `"127.0.0.1:18123"`                          |
| `DSN()`     | `"clickhouse://127.0.0.1:19000/default"`     |
| `HTTPURL()` | `"http://127.0.0.1:18123"`                   |

## Platform support

| OS     | Arch  | Asset type  |
|--------|-------|-------------|
| Linux  | amd64 | `.tgz` archive |
| Linux  | arm64 | `.tgz` archive |
| macOS  | amd64 | Raw binary  |
| macOS  | arm64 | Raw binary  |

## CI caching

The downloaded ClickHouse binary (~200MB for Linux, ~130MB for macOS) is cached at the cache path. In CI, cache this directory to avoid re-downloading on every run:

```yaml
# GitHub Actions
- uses: actions/cache@v4
  with:
    path: ~/.cache/embedded-clickhouse
    key: clickhouse-${{ runner.os }}-${{ runner.arch }}-25.8.16.34-lts
```

## Memory limits

No server memory limit is imposed by default. ClickHouse uses its built-in ratio-based default (`max_server_memory_usage_to_ram_ratio = 0.9`), which caps the server at 90% of available RAM.

ClickHouse has two separate memory settings:

- **`max_server_memory_usage`** — server-wide ceiling across all queries and background operations
- **`max_memory_usage`** — per-query limit (set in user profiles, not server config)

For constrained CI environments, set an explicit server limit via `Settings()`:

```go
embeddedclickhouse.DefaultConfig().
    Settings(map[string]string{"max_server_memory_usage": "1073741824"}) // 1 GiB
```

Environments with less than 2 GB total RAM will be fragile regardless of settings — ClickHouse needs memory for internal overhead (mark cache, logs, query cache, metadata) beyond query execution.

## How it works

1. **Download** — fetches the ClickHouse binary from GitHub releases (or a configured mirror) on first use
2. **Verify** — checks SHA512 hash for downloaded assets
3. **Cache** — stores the extracted binary at `~/.cache/embedded-clickhouse/` for reuse
4. **Configure** — generates a minimal XML config with allocated ports and a temp data directory
5. **Start** — launches `clickhouse server` as a child process
6. **Health check** — polls `GET /ping` every 100ms until the server responds
7. **Stop** — sends SIGTERM, waits for graceful shutdown, then SIGKILL if needed; cleans up the temp directory

## License

Apache 2.0
