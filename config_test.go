package embeddedclickhouse

import (
	"bytes"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()

	if cfg.version != DefaultVersion {
		t.Errorf("version = %q, want %q", cfg.version, DefaultVersion)
	}

	if cfg.tcpPort != 0 {
		t.Errorf("tcpPort = %d, want 0", cfg.tcpPort)
	}

	if cfg.httpPort != 0 {
		t.Errorf("httpPort = %d, want 0", cfg.httpPort)
	}

	if cfg.startTimeout != 30*time.Second {
		t.Errorf("startTimeout = %v, want 30s", cfg.startTimeout)
	}

	if cfg.stopTimeout != 10*time.Second {
		t.Errorf("stopTimeout = %v, want 10s", cfg.stopTimeout)
	}

	if cfg.logger == nil {
		t.Error("logger should not be nil")
	}
}

func TestConfigBuilderChaining(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	settings := map[string]string{"max_threads": "2"}

	cfg := DefaultConfig().
		Version(V25_3).
		TCPPort(19000).
		HTTPPort(18123).
		CachePath("/tmp/cache").
		DataPath("/tmp/data").
		BinaryPath("/usr/bin/clickhouse").
		BinaryRepositoryURL("https://mirror.example.com").
		StartTimeout(60 * time.Second).
		StopTimeout(20 * time.Second).
		Logger(buf).
		Settings(settings)

	if cfg.version != V25_3 {
		t.Errorf("version = %q, want %q", cfg.version, V25_3)
	}

	if cfg.tcpPort != 19000 {
		t.Errorf("tcpPort = %d, want 19000", cfg.tcpPort)
	}

	if cfg.httpPort != 18123 {
		t.Errorf("httpPort = %d, want 18123", cfg.httpPort)
	}

	if cfg.cachePath != "/tmp/cache" {
		t.Errorf("cachePath = %q, want /tmp/cache", cfg.cachePath)
	}

	if cfg.dataPath != "/tmp/data" {
		t.Errorf("dataPath = %q, want /tmp/data", cfg.dataPath)
	}

	if cfg.binaryPath != "/usr/bin/clickhouse" {
		t.Errorf("binaryPath = %q, want /usr/bin/clickhouse", cfg.binaryPath)
	}

	if cfg.binaryRepositoryURL != "https://mirror.example.com" {
		t.Errorf("binaryRepositoryURL = %q, want https://mirror.example.com", cfg.binaryRepositoryURL)
	}

	if cfg.startTimeout != 60*time.Second {
		t.Errorf("startTimeout = %v, want 60s", cfg.startTimeout)
	}

	if cfg.stopTimeout != 20*time.Second {
		t.Errorf("stopTimeout = %v, want 20s", cfg.stopTimeout)
	}

	if cfg.logger != buf {
		t.Error("logger mismatch")
	}

	if cfg.settings["max_threads"] != "2" {
		t.Errorf("settings[max_threads] = %q, want 2", cfg.settings["max_threads"])
	}
}

func TestConfigBuilderImmutability(t *testing.T) {
	t.Parallel()

	base := DefaultConfig()
	modified := base.Version(V25_3).TCPPort(9000)

	if base.version != DefaultVersion {
		t.Errorf("base version was modified: %q", base.version)
	}

	if base.tcpPort != 0 {
		t.Errorf("base tcpPort was modified: %d", base.tcpPort)
	}

	if modified.version != V25_3 {
		t.Errorf("modified version = %q, want %q", modified.version, V25_3)
	}

	if modified.tcpPort != 9000 {
		t.Errorf("modified tcpPort = %d, want 9000", modified.tcpPort)
	}
}
