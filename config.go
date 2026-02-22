package embeddedclickhouse

import (
	"io"
	"os"
	"time"
)

// ClickHouseVersion represents a ClickHouse server version string.
type ClickHouseVersion string

const (
	V26_1 ClickHouseVersion = "26.1.3.52-stable"
	V25_8 ClickHouseVersion = "25.8.16.34-lts"
	V25_3 ClickHouseVersion = "25.3.14.14-lts"
)

// DefaultVersion is the default ClickHouse version used when none is specified.
const DefaultVersion = V25_8

// Config holds configuration for an embedded ClickHouse server.
type Config struct {
	version             ClickHouseVersion
	tcpPort             uint32
	httpPort            uint32
	cachePath           string
	dataPath            string
	binaryPath          string
	binaryRepositoryURL string
	startTimeout        time.Duration
	stopTimeout         time.Duration
	logger              io.Writer
	settings            map[string]string
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		version:      DefaultVersion,
		startTimeout: 30 * time.Second,
		stopTimeout:  10 * time.Second,
		logger:       os.Stdout,
	}
}

// Version sets the ClickHouse version to use.
func (c Config) Version(v ClickHouseVersion) Config {
	c.version = v
	return c
}

// TCPPort sets the TCP port for the ClickHouse native protocol.
// 0 means auto-allocate (default).
func (c Config) TCPPort(port uint32) Config {
	c.tcpPort = port
	return c
}

// HTTPPort sets the HTTP port for the ClickHouse HTTP interface.
// 0 means auto-allocate (default).
func (c Config) HTTPPort(port uint32) Config {
	c.httpPort = port
	return c
}

// CachePath overrides the directory used to cache downloaded binaries.
func (c Config) CachePath(path string) Config {
	c.cachePath = path
	return c
}

// DataPath sets a persistent data directory that survives Stop.
func (c Config) DataPath(path string) Config {
	c.dataPath = path
	return c
}

// BinaryPath uses a pre-existing ClickHouse binary, skipping download.
func (c Config) BinaryPath(path string) Config {
	c.binaryPath = path
	return c
}

// BinaryRepositoryURL sets a custom mirror URL for downloading ClickHouse binaries.
func (c Config) BinaryRepositoryURL(url string) Config {
	c.binaryRepositoryURL = url
	return c
}

// StartTimeout sets the maximum time to wait for the server to become ready.
func (c Config) StartTimeout(d time.Duration) Config {
	c.startTimeout = d
	return c
}

// StopTimeout sets the maximum time to wait for the server to shut down gracefully.
func (c Config) StopTimeout(d time.Duration) Config {
	c.stopTimeout = d
	return c
}

// Logger sets the writer for server stdout/stderr output.
func (c Config) Logger(w io.Writer) Config {
	c.logger = w
	return c
}

// Settings sets arbitrary ClickHouse server settings.
func (c Config) Settings(s map[string]string) Config {
	c.settings = s
	return c
}
