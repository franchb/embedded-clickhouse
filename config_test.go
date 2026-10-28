package embeddedclickhouse

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
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

	// 0 means "use the mode default"; resolve fills it in.
	if cfg.startTimeout != 0 {
		t.Errorf("startTimeout = %v, want 0", cfg.startTimeout)
	}

	resolved, err := cfg.resolve(defaultStartTimeout, serverManagedKeys)
	if err != nil {
		t.Fatal(err)
	}

	if resolved.startTimeout != 30*time.Second {
		t.Errorf("resolved startTimeout = %v, want 30s", resolved.startTimeout)
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

func TestConfigBuilderChaining_CustomAssets(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig().
		CustomArchivePath("/path/to/archive.tar.gz").
		CustomArchiveURL("https://example.com/clickhouse.tar.gz").
		SHA256("abc123").
		SHA512("def456")

	if cfg.customArchivePath != "/path/to/archive.tar.gz" {
		t.Errorf("customArchivePath = %q, want /path/to/archive.tar.gz", cfg.customArchivePath)
	}

	if cfg.customArchiveURL != "https://example.com/clickhouse.tar.gz" {
		t.Errorf("customArchiveURL = %q, want https://example.com/clickhouse.tar.gz", cfg.customArchiveURL)
	}

	if cfg.sha256 != "abc123" {
		t.Errorf("sha256 = %q, want abc123", cfg.sha256)
	}

	if cfg.sha512hash != "def456" {
		t.Errorf("sha512hash = %q, want def456", cfg.sha512hash)
	}
}

func TestConfigAllowMissingChecksum(t *testing.T) {
	t.Parallel()

	base := DefaultConfig()
	if base.allowMissingChecksum {
		t.Error("default allowMissingChecksum should be false (strict)")
	}

	enabled := base.AllowMissingChecksum(true)
	if !enabled.allowMissingChecksum {
		t.Error("AllowMissingChecksum(true) should set the field")
	}

	// Immutability: the builder must not mutate the receiver.
	if base.allowMissingChecksum {
		t.Error("base config was mutated by AllowMissingChecksum")
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

func TestConfigResolve_ZeroValueDefaults(t *testing.T) {
	t.Parallel()

	// A zero Config with only a version must not produce a born-expired start
	// context or an instant-SIGKILL stop.
	cfg, err := Config{}.Version(DefaultVersion).resolve(defaultStartTimeout, serverManagedKeys)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.startTimeout != defaultStartTimeout {
		t.Errorf("startTimeout = %v, want %v", cfg.startTimeout, defaultStartTimeout)
	}

	if cfg.stopTimeout != defaultStopTimeout {
		t.Errorf("stopTimeout = %v, want %v", cfg.stopTimeout, defaultStopTimeout)
	}
}

func TestConfigResolve_Rejects(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		cfg     Config
		wantErr error
	}{
		"empty version":         {Config{}, ErrInvalidConfig},
		"port above 65535":      {DefaultConfig().TCPPort(70000), ErrInvalidConfig},
		"http port above 65535": {DefaultConfig().HTTPPort(65536), ErrInvalidConfig},
		"tcp equals http":       {DefaultConfig().TCPPort(19000).HTTPPort(19000), ErrInvalidConfig},
		"short sha256":          {DefaultConfig().CustomArchiveURL("http://x/a.tgz").SHA256("abc"), ErrInvalidConfig},
		"non-hex sha512": {
			DefaultConfig().CustomArchiveURL("http://x/a.tgz").SHA512(strings.Repeat("z", 128)),
			ErrInvalidConfig,
		},
		"managed setting": {DefaultConfig().Settings(map[string]string{"tcp_port": "1"}), ErrInvalidSettingKey},
		"unsafe setting":  {DefaultConfig().Settings(map[string]string{"a b": "1"}), ErrInvalidSettingKey},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, err := tc.cfg.resolve(defaultStartTimeout, serverManagedKeys); !errors.Is(err, tc.wantErr) {
				t.Errorf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestConfigResolve_EmptyVersionAllowedWithExplicitBinary(t *testing.T) {
	t.Parallel()

	for name, cfg := range map[string]Config{
		"BinaryPath":        Config{}.BinaryPath("/usr/bin/clickhouse"),
		"CustomArchivePath": Config{}.CustomArchivePath("/tmp/ch.tgz"),
		"CustomArchiveURL":  Config{}.CustomArchiveURL("https://example.com/ch.tgz"),
	} {
		if _, err := cfg.resolve(defaultStartTimeout, serverManagedKeys); err != nil {
			t.Errorf("%s: unexpected error %v", name, err)
		}
	}
}

// TestStart_InvalidConfigFailsBeforeDownload: the cache dir points at a file, so any
// download attempt would fail with a different error. ErrInvalidConfig proves Start
// validated first.
func TestStart_InvalidConfigFailsBeforeDownload(t *testing.T) {
	t.Parallel()

	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	err := NewServer(DefaultConfig().Version("").CachePath(blocker)).Start()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("Start err = %v, want ErrInvalidConfig", err)
	}

	err = NewCluster(2, DefaultConfig().Version("").CachePath(blocker)).Start()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("Cluster.Start err = %v, want ErrInvalidConfig", err)
	}

	if _, err := EnsureBinary(DefaultConfig().Version("").CachePath(blocker)); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("EnsureBinary err = %v, want ErrInvalidConfig", err)
	}
}
