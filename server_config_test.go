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

func TestWriteServerConfig_OverrideMaxMemory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	override := "2147483648" // 2 GiB
	settings := map[string]string{"max_server_memory_usage": override}

	configPath, err := writeServerConfig(dir, 19000, 18123, settings)
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

func TestMergeSettings(t *testing.T) {
	t.Parallel()

	t.Run("nil input returns empty map", func(t *testing.T) {
		t.Parallel()

		got := mergeSettings(nil)
		if len(got) != 0 {
			t.Errorf("expected empty map, got %v", got)
		}
	})

	t.Run("user values pass through", func(t *testing.T) {
		t.Parallel()

		got := mergeSettings(map[string]string{"max_server_memory_usage": "999"})
		if got["max_server_memory_usage"] != "999" {
			t.Errorf("expected value 999, got %q", got["max_server_memory_usage"])
		}
	})

	t.Run("additive keys preserved", func(t *testing.T) {
		t.Parallel()

		got := mergeSettings(map[string]string{"max_threads": "4"})
		if got["max_threads"] != "4" {
			t.Errorf("expected max_threads=4, got %q", got["max_threads"])
		}
	})
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
