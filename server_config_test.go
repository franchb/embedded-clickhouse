package embeddedclickhouse

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteServerConfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	settings := map[string]string{"max_threads": "4"}

	configPath, err := writeServerConfig(dir, 19000, 18123, settings)
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

	checks := []string{
		"<tcp_port>19000</tcp_port>",
		"<http_port>18123</http_port>",
		"<max_threads>4</max_threads>",
		"<password></password>",
		"<max_server_memory_usage>1073741824</max_server_memory_usage>",
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

	_, err := writeServerConfig(dir, 19000, 18123, nil)
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

func TestWriteServerConfig_NoSettings(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	configPath, err := writeServerConfig(dir, 9000, 8123, nil)
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
