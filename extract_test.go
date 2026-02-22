package embeddedclickhouse

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExtractClickHouseBinary(t *testing.T) {
	t.Parallel()

	archivePath := filepath.Join("testdata", "clickhouse-test.tgz")
	if _, err := os.Stat(archivePath); os.IsNotExist(err) {
		t.Skip("testdata fixture not found")
	}

	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "clickhouse")

	err := extractClickHouseBinary(archivePath, destPath)
	if err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(destPath)
	if err != nil {
		t.Fatal(err)
	}

	if info.Mode()&0o111 == 0 {
		t.Error("extracted binary is not executable")
	}

	content, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatal(err)
	}

	if len(content) == 0 {
		t.Error("extracted binary is empty")
	}
}

func TestExtractClickHouseBinary_MissingArchive(t *testing.T) {
	t.Parallel()

	err := extractClickHouseBinary("/nonexistent/archive.tgz", "/tmp/clickhouse")
	if err == nil {
		t.Fatal("expected error for missing archive")
	}
}

func TestExtractClickHouseBinary_NotATgz(t *testing.T) {
	t.Parallel()

	// Create a non-gzip file.
	tmpFile := filepath.Join(t.TempDir(), "bad.tgz")
	if err := os.WriteFile(tmpFile, []byte("not a gzip file"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := extractClickHouseBinary(tmpFile, filepath.Join(t.TempDir(), "clickhouse"))
	if err == nil {
		t.Fatal("expected error for non-gzip file")
	}
}
