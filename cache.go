package embeddedclickhouse

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

const cacheSubdir = "embedded-clickhouse"

// cacheDir returns the directory used to store cached ClickHouse binaries.
// Priority: explicit override > $XDG_CACHE_HOME/embedded-clickhouse > ~/.cache/embedded-clickhouse.
func cacheDir(override string) (string, error) {
	if override != "" {
		return override, nil
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

// cachedBinaryPath returns the full path to a cached ClickHouse binary for the given version and platform.
func cachedBinaryPath(cacheDir string, version ClickHouseVersion) string {
	return filepath.Join(cacheDir, fmt.Sprintf("clickhouse-%s-%s-%s", string(version), runtime.GOOS, runtime.GOARCH))
}
