package embeddedclickhouse

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCacheDir_Override(t *testing.T) {
	t.Parallel()

	dir, err := cacheDir("/custom/path")
	if err != nil {
		t.Fatal(err)
	}

	if dir != "/custom/path" {
		t.Errorf("cacheDir = %q, want /custom/path", dir)
	}
}

func TestCacheDir_XDG(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", "/xdg/cache")

	dir, err := cacheDir("")
	if err != nil {
		t.Fatal(err)
	}

	want := filepath.Join("/xdg/cache", cacheSubdir)
	if dir != want {
		t.Errorf("cacheDir = %q, want %q", dir, want)
	}
}

func TestCacheDir_Default(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", "")

	dir, err := cacheDir("")
	if err != nil {
		t.Fatal(err)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}

	want := filepath.Join(home, ".cache", cacheSubdir)
	if dir != want {
		t.Errorf("cacheDir = %q, want %q", dir, want)
	}
}

func TestCachedBinaryPath(t *testing.T) {
	t.Parallel()

	path := cachedBinaryPath("/cache", V25_8)
	if !strings.HasPrefix(path, "/cache/clickhouse-25.8.16.34-lts-") {
		t.Errorf("unexpected path: %q", path)
	}
}
