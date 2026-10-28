package embeddedclickhouse

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const cacheSubdir = "embedded-clickhouse"

// cacheDir returns the absolute directory used to store cached ClickHouse binaries.
// Priority: explicit override > $XDG_CACHE_HOME/embedded-clickhouse > ~/.cache/embedded-clickhouse.
// A relative override (e.g. "../shared-cache") is resolved against the working directory,
// so every cache path built from it is absolute and free of ".." segments.
func cacheDir(override string) (string, error) {
	if override != "" {
		dir, err := filepath.Abs(override)
		if err != nil {
			return "", fmt.Errorf("embedded-clickhouse: resolve cache path: %w", err)
		}

		return dir, nil
	}

	if xdg := os.Getenv("XDG_CACHE_HOME"); xdg != "" {
		return filepath.Join(xdg, cacheSubdir), nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("embedded-clickhouse: cannot determine home directory: %w", err)
	}

	return filepath.Join(home, ".cache", cacheSubdir), nil
}

// cachedBinaryPath returns the full path to a cached ClickHouse binary for the given
// version, platform, and download source. Binaries fetched from a custom
// BinaryRepositoryURL get their own entry, keyed by a digest of that URL, so a binary
// from one mirror is never served for another (or for the default GitHub source).
// The default source keeps the historical name, so existing caches stay valid.
func cachedBinaryPath(cacheDir string, version ClickHouseVersion, repositoryURL string) string {
	// Replace path separators in the version string to prevent directory traversal in cache filenames.
	safeVersion := strings.NewReplacer("/", "_", `\`, "_").Replace(string(version))
	name := fmt.Sprintf("clickhouse-%s-%s-%s", safeVersion, runtime.GOOS, runtime.GOARCH)

	if repo := strings.TrimRight(repositoryURL, "/"); repo != "" && repo != defaultBaseURL {
		h := sha256.Sum256([]byte(repo))
		name += "-" + hex.EncodeToString(h[:])[:16]
	}

	return filepath.Join(cacheDir, name)
}

// customCachedBinaryPath returns the full path to a cached binary for a custom asset.
// The key is the first 16 hex characters of the SHA256 hash of the input (URL or file content hash).
func customCachedBinaryPath(cacheDir, hashInput string) string {
	h := sha256.Sum256([]byte(hashInput))
	key := hex.EncodeToString(h[:])[:16]

	return filepath.Join(cacheDir, "custom-"+key)
}

// lockPathFor returns the sidecar advisory-lock file path for a cached binary path.
// The lock file lives next to the binary so it shares the same (already created) directory.
func lockPathFor(binPath string) string {
	return binPath + ".lock"
}
