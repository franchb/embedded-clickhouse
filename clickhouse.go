// Package embeddedclickhouse provides an embedded ClickHouse server for Go tests.
// It downloads, caches, and manages a real ClickHouse server process, similar to
// how fergusstrange/embedded-postgres works for PostgreSQL.
package embeddedclickhouse

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// ErrServerNotStarted is returned by Stop when the server has not been started.
var ErrServerNotStarted = errors.New("embedded-clickhouse: server has not been started")

// ErrServerAlreadyStarted is returned by Start when the server is already running.
var ErrServerAlreadyStarted = errors.New("embedded-clickhouse: server is already started")

// ErrUnsupportedPlatform is returned when the current OS/architecture has no ClickHouse release asset.
var ErrUnsupportedPlatform = errors.New("embedded-clickhouse: unsupported platform")

// ErrStopTimeout is returned when the server does not stop within the configured StopTimeout; the process is killed.
var ErrStopTimeout = errors.New("embedded-clickhouse: server did not stop within timeout, killed")

// ErrDownloadFailed is returned when the HTTP download of a ClickHouse asset returns a non-200 status.
var ErrDownloadFailed = errors.New("embedded-clickhouse: download failed")

// ErrSHA256Mismatch is returned when the downloaded file's SHA256 hash does not match the expected value.
var ErrSHA256Mismatch = errors.New("embedded-clickhouse: SHA256 mismatch")

// ErrSHA512Mismatch is returned when the downloaded file's SHA512 hash does not match the expected value.
var ErrSHA512Mismatch = errors.New("embedded-clickhouse: SHA512 mismatch")

// ErrSHA512NotFound is returned when the SHA512 checksum file does not contain a hash for the expected filename.
var ErrSHA512NotFound = errors.New("embedded-clickhouse: SHA512 hash not found")

// ErrSHA512Unavailable is returned when the SHA512 checksum file cannot be fetched (non-200)
// and AllowMissingChecksum has not been enabled.
var ErrSHA512Unavailable = errors.New("embedded-clickhouse: SHA512 checksum unavailable")

// ErrBinaryNotFound is returned when the ClickHouse binary cannot be located inside a downloaded archive.
var ErrBinaryNotFound = errors.New("embedded-clickhouse: binary not found in archive")

// ErrInvalidPath is returned when a destination path contains a path traversal sequence ("..").
var ErrInvalidPath = errors.New("embedded-clickhouse: invalid destination path")

// ErrUnexpectedAddrType is returned when the listener address is not the expected *net.TCPAddr type.
var ErrUnexpectedAddrType = errors.New("embedded-clickhouse: unexpected listener address type")

// ErrUnknownAssetType is returned when an unrecognised platform asset type is encountered.
var ErrUnknownAssetType = errors.New("embedded-clickhouse: unknown asset type")

// ErrInvalidSettingKey is returned when a settings key contains characters that are unsafe in an XML element name.
var ErrInvalidSettingKey = errors.New("embedded-clickhouse: invalid setting key")

// ErrClusterManaged is returned when Start or Stop is called on a node owned by a Cluster.
var ErrClusterManaged = errors.New("embedded-clickhouse: node is managed by a cluster; use Cluster.Start/Stop")

// ErrServerExited is returned when the ClickHouse process exits during startup before becoming ready.
var ErrServerExited = errors.New("embedded-clickhouse: server process exited during startup")

// ErrInvalidConfig is returned by Start and EnsureBinary when the Config is unusable,
// before any download is attempted.
var ErrInvalidConfig = errors.New("embedded-clickhouse: invalid config")

// ErrLockingUnsupported is returned when cross-process file locking is not supported on the current platform.
var ErrLockingUnsupported = errors.New("embedded-clickhouse: file locking not supported on this platform")

// EmbeddedClickHouse manages a ClickHouse server process for testing.
type EmbeddedClickHouse struct {
	config Config

	mu      sync.RWMutex
	started bool
	gate    startGate
	node    node

	// clusterManaged marks a node owned by a Cluster: its lifecycle belongs to
	// Cluster.Start/Stop, so the single-node Start/Stop refuse to touch it.
	clusterManaged bool
}

// NewServer creates a new EmbeddedClickHouse with the given config.
// If no config is provided, DefaultConfig() is used.
func NewServer(config ...Config) *EmbeddedClickHouse {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	} else {
		cfg = DefaultConfig()
	}

	return &EmbeddedClickHouse{config: cfg}
}

// NewServerForTest creates a server, starts it, and registers tb.Cleanup(server.Stop).
// Calls tb.Fatal on Start() error. A test may call Stop itself; the cleanup then
// finds the server already stopped and does not report that as a failure.
func NewServerForTest(tb testing.TB, config ...Config) *EmbeddedClickHouse {
	tb.Helper()

	s := NewServer(config...)

	if err := s.Start(); err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err := s.Stop(); err != nil && !errors.Is(err, ErrServerNotStarted) {
			tb.Errorf("embedded-clickhouse: stop failed: %v", err)
		}
	})

	return s
}

// Start downloads the ClickHouse binary (if needed), generates config, and starts the server.
// It is equivalent to StartContext(context.Background()).
func (e *EmbeddedClickHouse) Start() error {
	return e.StartContext(context.Background())
}

// StartContext is like Start, but aborts when ctx is done. StartTimeout still bounds
// the readiness wait. A concurrent Stop also aborts an in-progress start.
func (e *EmbeddedClickHouse) StartContext(ctx context.Context) error {
	e.mu.Lock()

	if e.clusterManaged {
		e.mu.Unlock()
		return ErrClusterManaged
	}

	if e.started || e.gate.inFlight() {
		e.mu.Unlock()
		return ErrServerAlreadyStarted
	}

	ctx = e.gate.begin(ctx)
	cfg := e.config
	e.mu.Unlock()

	nd, err := startSingleNode(ctx, cfg)

	e.mu.Lock()

	wake := e.gate.end()

	if err == nil {
		e.node = *nd
		e.started = true
	}

	e.mu.Unlock()
	wake()

	return err
}

// startSingleNode resolves the binary, launches one server, and waits until it is ready.
func startSingleNode(ctx context.Context, cfg Config) (*node, error) {
	cfg, err := cfg.resolve(defaultStartTimeout, serverManagedKeys)
	if err != nil {
		return nil, err
	}

	binPath, err := ensureBinary(cfg)
	if err != nil {
		return nil, err
	}

	tcpPort, httpPort, err := resolveServerPorts(cfg.tcpPort, cfg.httpPort)
	if err != nil {
		return nil, err
	}

	nd, err := launchNode(nodeSpec{
		binPath:     binPath,
		dataPath:    cfg.dataPath,
		tempPattern: "embedded-clickhouse-*",
		tcpPort:     tcpPort,
		httpPort:    httpPort,
		writeConfig: func(dir string) (string, error) {
			return writeServerConfig(dir, tcpPort, httpPort, cfg.settings)
		},
		output: processOutput(cfg.logger),
	})
	if err != nil {
		return nil, err
	}

	// Wait for server to be ready, or abort early if the process exits.
	readyCtx, cancel := context.WithTimeout(ctx, cfg.startTimeout)
	defer cancel()

	if err := waitForReadyOrExit(readyCtx, httpPort, nd.proc); err != nil {
		_ = nd.teardown(cfg.stopTimeout)
		return nil, err
	}

	return nd, nil
}

// resolveServerPorts returns the configured ports, auto-allocating any that are 0.
// Auto-allocated ports come from one allocatePorts batch, so they are distinct from
// each other, and a spare is drawn so they can also avoid a user-fixed port.
func resolveServerPorts(tcpPort, httpPort uint32) (uint32, uint32, error) {
	need := 0

	for _, p := range []uint32{tcpPort, httpPort} {
		if p == 0 {
			need++
		}
	}

	if need == 0 {
		return tcpPort, httpPort, nil
	}

	free, err := allocatePorts(need + 1)
	if err != nil {
		return 0, 0, err
	}

	for _, port := range free {
		if port == tcpPort || port == httpPort {
			continue
		}

		switch {
		case tcpPort == 0:
			tcpPort = port
		case httpPort == 0:
			httpPort = port
		}
	}

	return tcpPort, httpPort, nil
}

// Stop gracefully shuts down the ClickHouse server and cleans up resources.
// If a Start is still in progress, Stop aborts it first and waits for it to unwind.
func (e *EmbeddedClickHouse) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.clusterManaged {
		return ErrClusterManaged
	}

	e.gate.abort(&e.mu)

	if !e.started {
		return ErrServerNotStarted
	}

	e.started = false

	return e.node.teardown(e.config.stopTimeout)
}

// TCPAddr returns the TCP address for the ClickHouse native protocol (e.g., "127.0.0.1:19000").
// It returns "" while the server is not running (before Start completes or after Stop).
func (e *EmbeddedClickHouse) TCPAddr() string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.started {
		return ""
	}

	return fmt.Sprintf("127.0.0.1:%d", e.node.tcpPort)
}

// HTTPAddr returns the HTTP address for the ClickHouse HTTP interface (e.g., "127.0.0.1:18123").
// It returns "" while the server is not running (before Start completes or after Stop).
func (e *EmbeddedClickHouse) HTTPAddr() string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.started {
		return ""
	}

	return fmt.Sprintf("127.0.0.1:%d", e.node.httpPort)
}

// DSN returns a ClickHouse DSN for use with clickhouse-go (e.g., "clickhouse://127.0.0.1:19000/default").
// It returns "" while the server is not running (before Start completes or after Stop).
func (e *EmbeddedClickHouse) DSN() string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.started {
		return ""
	}

	return fmt.Sprintf("clickhouse://127.0.0.1:%d/default", e.node.tcpPort)
}

// HTTPURL returns the base HTTP URL (e.g., "http://127.0.0.1:18123").
// It returns "" while the server is not running (before Start completes or after Stop).
func (e *EmbeddedClickHouse) HTTPURL() string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.started {
		return ""
	}

	return fmt.Sprintf("http://127.0.0.1:%d", e.node.httpPort)
}

// Started reports whether the server is running (Start completed and Stop has not been called).
func (e *EmbeddedClickHouse) Started() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.started
}
