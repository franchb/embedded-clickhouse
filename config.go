package embeddedclickhouse

import (
	"encoding/hex"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"time"
)

const (
	// defaultStartTimeout bounds the readiness wait of a single server.
	defaultStartTimeout = 30 * time.Second
	// defaultStopTimeout bounds graceful shutdown before the process is killed.
	defaultStopTimeout = 10 * time.Second
	// maxPort is the largest valid TCP port number.
	maxPort = 65535
)

// ClickHouseVersion represents a ClickHouse server version string.
type ClickHouseVersion string

// Version constants, newest first. Each pins the latest patch release of its line;
// the clickhouse-versions workflow moves them to newer patches. To pin an exact build,
// pass a literal such as ClickHouseVersion("26.8.34.13-lts").

// V26_8 is ClickHouse 26.8 (LTS channel).
const V26_8 ClickHouseVersion = "26.8.34.13-lts"

// V26_3 is ClickHouse 26.3 (LTS channel).
const V26_3 ClickHouseVersion = "26.3.60.2-lts"

// V26_1 is ClickHouse 26.1 (stable channel).
//
// Deprecated: ClickHouse no longer ships security fixes for 26.1. Use V26_8.
const V26_1 ClickHouseVersion = "26.1.12.23-stable"

// V25_8 is ClickHouse 25.8 (LTS channel).
//
// Deprecated: ClickHouse no longer ships security fixes for 25.8. Use V26_8.
const V25_8 ClickHouseVersion = "25.8.33.6-lts"

// V25_3 is ClickHouse 25.3 (LTS channel).
//
// Deprecated: ClickHouse no longer ships security fixes for 25.3. Use V26_8.
const V25_3 ClickHouseVersion = "25.3.14.14-lts"

// DefaultVersion is the default ClickHouse version used when none is specified.
const DefaultVersion = V26_8

// Config holds configuration for an embedded ClickHouse server.
type Config struct {
	version              ClickHouseVersion
	tcpPort              uint32
	httpPort             uint32
	cachePath            string
	dataPath             string
	binaryPath           string
	binaryRepositoryURL  string
	customArchivePath    string
	customArchiveURL     string
	sha256               string
	sha512hash           string
	allowMissingChecksum bool
	httpHeaders          http.Header
	startTimeout         time.Duration
	stopTimeout          time.Duration
	logger               io.Writer
	settings             map[string]string
}

// DefaultConfig returns a Config with sensible defaults. StartTimeout is left at 0,
// which resolves to 30s for a single server and 240s for a Cluster.
func DefaultConfig() Config {
	return Config{
		version:     DefaultVersion,
		stopTimeout: defaultStopTimeout,
		logger:      os.Stdout,
	}
}

// Version sets the ClickHouse version to use.
func (c Config) Version(v ClickHouseVersion) Config {
	c.version = v
	return c
}

// TCPPort sets the TCP port for the ClickHouse native protocol.
// 0 means auto-allocate (default). Single-node only: Cluster.Start auto-allocates
// every node's ports and returns ErrClusterUnsupportedOption if this is set.
func (c Config) TCPPort(port uint32) Config {
	c.tcpPort = port
	return c
}

// HTTPPort sets the HTTP port for the ClickHouse HTTP interface.
// 0 means auto-allocate (default). Single-node only: Cluster.Start auto-allocates
// every node's ports and returns ErrClusterUnsupportedOption if this is set.
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
// Single-node only: cluster nodes always use per-node temp directories, so
// Cluster.Start returns ErrClusterUnsupportedOption if this is set.
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

// CustomArchivePath sets a local .tar.gz archive containing a ClickHouse binary.
// The binary is extracted and cached. This bypasses the standard download logic.
func (c Config) CustomArchivePath(path string) Config {
	c.customArchivePath = path
	return c
}

// CustomArchiveURL sets a fully custom URL to download a .tar.gz archive containing
// a ClickHouse binary. The archive is downloaded, extracted, and cached.
// This bypasses the standard GitHub release download logic entirely.
func (c Config) CustomArchiveURL(url string) Config {
	c.customArchiveURL = url
	return c
}

// SHA256 sets the expected SHA256 hex digest of the custom archive for verification.
// Only used with CustomArchivePath or CustomArchiveURL.
func (c Config) SHA256(hash string) Config {
	c.sha256 = hash
	return c
}

// SHA512 sets the expected SHA512 hex digest of the custom archive for verification.
// Only used with CustomArchivePath or CustomArchiveURL.
func (c Config) SHA512(hash string) Config {
	c.sha512hash = hash
	return c
}

// AllowMissingChecksum controls whether a missing (non-200) SHA512 checksum file is
// tolerated for archive downloads (the Linux .tgz path). The default (false) is strict:
// a missing checksum fails the download with ErrSHA512Unavailable. Set to true for an
// air-gapped or internal mirror that serves archives without a .sha512.
//
// This does not affect raw-binary downloads (the macOS path): ClickHouse publishes no
// checksum for those, so a missing one is always tolerated regardless of this setting.
func (c Config) AllowMissingChecksum(allow bool) Config {
	c.allowMissingChecksum = allow
	return c
}

// DownloadHeader adds an HTTP header (for example "Authorization" or "PRIVATE-TOKEN")
// to download requests, so a private mirror's credentials need not ride in the URL.
// Headers are sent only to user-chosen sources: CustomArchiveURL and
// BinaryRepositoryURL (including its .sha512 files), never to the default GitHub
// release URL. They are also dropped as soon as a redirect leaves the original host,
// so a registry that redirects to object storage never forwards them. Calling it again
// with the same key adds a value.
func (c Config) DownloadHeader(key, value string) Config {
	h := c.httpHeaders.Clone()
	if h == nil {
		h = make(http.Header)
	}

	h.Add(key, value)
	c.httpHeaders = h

	return c
}

// StartTimeout sets the maximum time to wait for the server to become ready.
// 0 (the default) means 30s for a single server and 240s for a Cluster.
func (c Config) StartTimeout(d time.Duration) Config {
	c.startTimeout = d
	return c
}

// StopTimeout sets the maximum time to wait for the server to shut down gracefully.
// 0 means the default (10s).
func (c Config) StopTimeout(d time.Duration) Config {
	c.stopTimeout = d
	return c
}

// Logger sets the writer for download progress and server stdout/stderr output.
// nil silences both.
func (c Config) Logger(w io.Writer) Config {
	c.logger = w
	return c
}

// Settings sets arbitrary ClickHouse server settings.
// The provided map is copied; subsequent caller mutations do not affect the Config.
func (c Config) Settings(s map[string]string) Config {
	m := make(map[string]string, len(s))
	maps.Copy(m, s)

	c.settings = m

	return c
}

// resolve validates the config and fills zero-valued timeouts with defaults, so a
// misconfiguration fails before any download. defaultStart is the mode's default
// StartTimeout, and managedKeys are the setting names its config template writes.
func (c Config) resolve(defaultStart time.Duration, managedKeys []string) (Config, error) {
	if c.startTimeout <= 0 {
		c.startTimeout = defaultStart
	}

	if c.stopTimeout <= 0 {
		c.stopTimeout = defaultStopTimeout
	}

	if err := c.validateBinarySource(); err != nil {
		return c, err
	}

	if c.tcpPort > maxPort || c.httpPort > maxPort {
		return c, fmt.Errorf("%w: ports must be at most %d (tcp=%d, http=%d)",
			ErrInvalidConfig, maxPort, c.tcpPort, c.httpPort)
	}

	if c.tcpPort != 0 && c.tcpPort == c.httpPort {
		return c, fmt.Errorf("%w: TCPPort and HTTPPort are both %d", ErrInvalidConfig, c.tcpPort)
	}

	if err := validateSettings(c.settings, managedKeys); err != nil {
		return c, err
	}

	return c, nil
}

// validateBinarySource checks the options that select and verify the binary.
func (c Config) validateBinarySource() error {
	usesRelease := c.binaryPath == "" && c.customArchivePath == "" && c.customArchiveURL == ""
	if usesRelease && c.version == "" {
		return fmt.Errorf("%w: version must not be empty", ErrInvalidConfig)
	}

	if err := validateHexDigest("SHA256", c.sha256, sha256HexLen); err != nil {
		return err
	}

	return validateHexDigest("SHA512", c.sha512hash, sha512HexLen)
}

// validateHexDigest checks that a configured digest (if any) is hex of the right length.
func validateHexDigest(name, digest string, wantLen int) error {
	if digest == "" {
		return nil
	}

	if _, err := hex.DecodeString(digest); err != nil || len(digest) != wantLen {
		return fmt.Errorf("%w: %s must be %d hex characters", ErrInvalidConfig, name, wantLen)
	}

	return nil
}
