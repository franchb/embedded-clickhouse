// Package embeddedclickhouse provides an embedded ClickHouse server for Go tests.
// It downloads, caches, and manages a real ClickHouse server process, similar to
// how fergusstrange/embedded-postgres works for PostgreSQL.
package embeddedclickhouse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
)

var (
	ErrServerNotStarted     = errors.New("embedded-clickhouse: server has not been started")
	ErrServerAlreadyStarted = errors.New("embedded-clickhouse: server is already started")
	ErrUnsupportedPlatform  = errors.New("embedded-clickhouse: unsupported platform")
	ErrStopTimeout          = errors.New("embedded-clickhouse: server did not stop within timeout, killed")
	ErrDownloadFailed       = errors.New("embedded-clickhouse: download failed")
	ErrSHA512Mismatch       = errors.New("embedded-clickhouse: SHA512 mismatch")
	ErrSHA512NotFound       = errors.New("embedded-clickhouse: SHA512 hash not found")
	ErrBinaryNotFound       = errors.New("embedded-clickhouse: binary not found in archive")
	ErrInvalidPath          = errors.New("embedded-clickhouse: invalid destination path")
	ErrUnexpectedAddrType   = errors.New("embedded-clickhouse: unexpected listener address type")
)

// EmbeddedClickHouse manages a ClickHouse server process for testing.
type EmbeddedClickHouse struct {
	config Config

	mu      sync.Mutex
	started bool
	cmd     *exec.Cmd
	tmpDir  string

	tcpPort  uint32
	httpPort uint32
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

// NewServerForTest creates a server, starts it, and registers t.Cleanup(server.Stop).
// Calls t.Fatal on Start() error.
func NewServerForTest(tb testing.TB, config ...Config) *EmbeddedClickHouse {
	tb.Helper()

	s := NewServer(config...)

	if err := s.Start(); err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err := s.Stop(); err != nil {
			tb.Errorf("embedded-clickhouse: stop failed: %v", err)
		}
	})

	return s
}

// Start downloads the ClickHouse binary (if needed), generates config, and starts the server.
func (e *EmbeddedClickHouse) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return ErrServerAlreadyStarted
	}

	cleanups := make([]func(), 0)
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	success := false

	defer func() {
		if !success {
			cleanup()
		}
	}()

	// Resolve binary.
	binPath, err := ensureBinary(e.config)
	if err != nil {
		return err
	}

	// Allocate ports.
	tcpPort := e.config.tcpPort
	if tcpPort == 0 {
		tcpPort, err = allocatePort()
		if err != nil {
			return err
		}
	}

	httpPort := e.config.httpPort
	if httpPort == 0 {
		httpPort, err = allocatePort()
		if err != nil {
			return err
		}
	}

	// Create temp directory or use configured data path.
	var tmpDir string
	if e.config.dataPath != "" {
		tmpDir = e.config.dataPath

		if err := os.MkdirAll(tmpDir, 0o755); err != nil {
			return fmt.Errorf("embedded-clickhouse: create data dir: %w", err)
		}
	} else {
		tmpDir, err = os.MkdirTemp("", "embedded-clickhouse-*")
		if err != nil {
			return fmt.Errorf("embedded-clickhouse: create temp dir: %w", err)
		}

		cleanups = append(cleanups, func() { os.RemoveAll(tmpDir) })
	}

	// Write server config.
	configPath, err := writeServerConfig(tmpDir, tcpPort, httpPort, e.config.settings)
	if err != nil {
		return err
	}

	// Start process with configured logger for stdout/stderr.
	logger := e.config.logger
	if logger == nil {
		logger = os.Stdout
	}

	cmd, err := startProcess(binPath, configPath, logger)
	if err != nil {
		return err
	}

	cleanups = append(cleanups, func() {
		stopProcess(cmd, e.config.stopTimeout) //nolint:errcheck
	})

	// Wait for server to be ready.
	ctx, cancel := context.WithTimeout(context.Background(), e.config.startTimeout)
	defer cancel()

	if err := waitForReady(ctx, httpPort); err != nil {
		return err
	}

	e.cmd = cmd
	e.tmpDir = tmpDir
	e.tcpPort = tcpPort
	e.httpPort = httpPort
	e.started = true
	success = true

	return nil
}

// Stop gracefully shuts down the ClickHouse server and cleans up resources.
func (e *EmbeddedClickHouse) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.started {
		return ErrServerNotStarted
	}

	var errs []error

	if err := stopProcess(e.cmd, e.config.stopTimeout); err != nil {
		errs = append(errs, err)
	}

	// Only remove temp dir if no explicit data path was set.
	if e.config.dataPath == "" && e.tmpDir != "" {
		if err := os.RemoveAll(e.tmpDir); err != nil {
			errs = append(errs, fmt.Errorf("embedded-clickhouse: remove temp dir: %w", err))
		}
	}

	e.started = false
	e.cmd = nil

	return errors.Join(errs...)
}

// TCPAddr returns the TCP address for the ClickHouse native protocol (e.g., "127.0.0.1:19000").
func (e *EmbeddedClickHouse) TCPAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", e.tcpPort)
}

// HTTPAddr returns the HTTP address for the ClickHouse HTTP interface (e.g., "127.0.0.1:18123").
func (e *EmbeddedClickHouse) HTTPAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", e.httpPort)
}

// DSN returns a ClickHouse DSN for use with clickhouse-go (e.g., "clickhouse://127.0.0.1:19000/default").
func (e *EmbeddedClickHouse) DSN() string {
	return fmt.Sprintf("clickhouse://127.0.0.1:%d/default", e.tcpPort)
}

// HTTPURL returns the base HTTP URL (e.g., "http://127.0.0.1:18123").
func (e *EmbeddedClickHouse) HTTPURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", e.httpPort)
}
