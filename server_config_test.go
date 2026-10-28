package embeddedclickhouse

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteServerConfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	settings := map[string]string{"max_threads": "4"}

	configPath, err := writeServerConfig(dir, testInstanceID, 19000, 18123, settings)
	if err != nil {
		t.Fatal(err)
	}

	if filepath.Dir(configPath) != dir {
		t.Errorf("config not in expected dir: %q", configPath)
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}

	xml := string(content)

	// The XML declaration must be the very first bytes, or ClickHouse rejects the file.
	if !strings.HasPrefix(xml, "<?xml") {
		t.Errorf("config must start with the XML declaration, got %q", xml[:min(len(xml), 20)])
	}

	checks := []string{
		"<listen_host>127.0.0.1</listen_host>",
		"<embedded_clickhouse_instance>" + testInstanceID + "</embedded_clickhouse_instance>",
		"<tcp_port>19000</tcp_port>",
		"<http_port>18123</http_port>",
		"<max_threads>4</max_threads>",
		"<password></password>",
	}

	for _, check := range checks {
		if !strings.Contains(xml, check) {
			t.Errorf("config missing %q", check)
		}
	}
}

func TestWriteServerConfig_CreatesSubdirs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	_, err := writeServerConfig(dir, testInstanceID, 19000, 18123, nil)
	if err != nil {
		t.Fatal(err)
	}

	subdirs := []string{"data", "tmp", "user_files", "format_schemas"}
	for _, sub := range subdirs {
		info, err := os.Stat(filepath.Join(dir, sub))
		if err != nil {
			t.Errorf("subdir %q not created: %v", sub, err)
			continue
		}

		if !info.IsDir() {
			t.Errorf("%q is not a directory", sub)
		}
	}
}

func TestWriteServerConfig_OverrideMaxMemory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	override := "2147483648" // 2 GiB
	settings := map[string]string{testKeyMaxServerMemoryUsage: override}

	configPath, err := writeServerConfig(dir, testInstanceID, 19000, 18123, settings)
	if err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}

	xml := string(content)

	want := "<max_server_memory_usage>" + override + "</max_server_memory_usage>"
	if !strings.Contains(xml, want) {
		t.Errorf("config missing override %q", want)
	}

	if count := strings.Count(xml, "<max_server_memory_usage>"); count != 1 {
		t.Errorf("expected exactly 1 max_server_memory_usage element, got %d", count)
	}
}

func TestWriteServerConfig_RejectsManagedKeys(t *testing.T) {
	t.Parallel()

	for _, key := range serverManagedKeys {
		_, err := writeServerConfig(t.TempDir(), testInstanceID, 19000, 18123, map[string]string{key: "1"})
		if !errors.Is(err, ErrInvalidSettingKey) {
			t.Errorf("setting %q: err = %v, want ErrInvalidSettingKey", key, err)
		}
	}
}

func TestWriteConfigFile_RemovesPartialFileOnError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.xml")

	// Executing a template that references a missing field fails mid-render.
	tmpl := newConfigTemplate("bad", "<clickhouse>{{.Missing}}</clickhouse>")

	if err := writeConfigFile(path, tmpl, struct{}{}); err == nil {
		t.Fatal("expected template execution error")
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("partial config left behind: stat err = %v", err)
	}
}

func TestWriteServerConfig_NoSettings(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	configPath, err := writeServerConfig(dir, testInstanceID, 9000, 8123, nil)
	if err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(content), "<tcp_port>9000</tcp_port>") {
		t.Error("config missing tcp_port")
	}
}

// TestWriteServerConfig_UserListenHost: a user-supplied listen_host replaces the default
// instead of adding a second <listen_host> next to it.
func TestWriteServerConfig_UserListenHost(t *testing.T) {
	t.Parallel()

	configPath, err := writeServerConfig(t.TempDir(), testInstanceID, 19000, 18123, map[string]string{"listen_host": "0.0.0.0"})
	if err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}

	xml := string(content)

	if got := strings.Count(xml, "<listen_host>"); got != 1 {
		t.Fatalf("config has %d <listen_host> elements, want 1:\n%s", got, xml)
	}

	if !strings.Contains(xml, "<listen_host>0.0.0.0</listen_host>") {
		t.Errorf("config missing the user listen_host:\n%s", xml)
	}
}
