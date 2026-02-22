# embedded-clickhouse

Run a real ClickHouse database locally on Linux or macOS as part of another Go application or test.

When testing, this provides a much higher level of confidence than mocking or using Docker. It requires no external dependencies beyond the Go toolchain — no Docker, no testcontainers, no pre-installed ClickHouse.

Inspired by [fergusstrange/embedded-postgres](https://github.com/fergusstrange/embedded-postgres). ClickHouse binaries are fetched directly from [official GitHub releases](https://github.com/ClickHouse/ClickHouse/releases).

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
    db, _ := sql.Open("clickhouse", testServer.DSN())
    defer db.Close()
    // ...
}
```

### Per-test with auto-cleanup

```go
func TestInsertAndSelect(t *testing.T) {
    ch := embeddedclickhouse.NewServerForTest(t)
    // Stop() is called via t.Cleanup

    db, _ := sql.Open("clickhouse", ch.DSN())
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

## How it works

1. **Download** — fetches the ClickHouse binary from GitHub releases (or a configured mirror) on first use
2. **Verify** — checks SHA512 hash for Linux `.tgz` archives
3. **Cache** — stores the extracted binary at `~/.cache/embedded-clickhouse/` for reuse
4. **Configure** — generates a minimal XML config with allocated ports and a temp data directory
5. **Start** — launches `clickhouse server` as a child process
6. **Health check** — polls `GET /ping` every 100ms until the server responds
7. **Stop** — sends SIGTERM, waits for graceful shutdown, then SIGKILL if needed; cleans up the temp directory

## License

Apache 2.0
