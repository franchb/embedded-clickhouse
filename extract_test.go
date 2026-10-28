package embeddedclickhouse

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

	err := extractClickHouseBinary("/nonexistent/archive.tgz", filepath.Join(t.TempDir(), "clickhouse"))
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

// TestWriteExecutable_ConcurrentNoTruncate runs N writeExecutable calls against the
// SAME destination with distinct-length payloads. Because each writer uses a unique
// temp file and an atomic rename, the final file must equal exactly ONE input length
// (a winner), never a truncated or interleaved mix.
func TestWriteExecutable_ConcurrentNoTruncate(t *testing.T) {
	t.Parallel()

	destPath := filepath.Join(t.TempDir(), "clickhouse")

	const writers = 8

	// Build a payload per writer, each with a unique byte and a unique length.
	payloads := make([][]byte, writers)
	lengths := make(map[int]bool, writers)

	for i := range writers {
		length := 100 + i*37
		payloads[i] = bytes.Repeat([]byte{byte('A' + i)}, length)
		lengths[length] = true
	}

	var wg sync.WaitGroup

	start := make(chan struct{})

	errs := make([]error, writers)

	for i := range writers {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			<-start

			errs[idx] = writeExecutable(bytes.NewReader(payloads[idx]), destPath)
		}(i)
	}

	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d failed: %v", i, err)
		}
	}

	got, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatal(err)
	}

	// The final file must be exactly one of the inputs: a single repeated byte of a
	// known length (never a truncated/interleaved mix).
	if !lengths[len(got)] {
		t.Fatalf("final file length %d is not one of the input lengths %v", len(got), lengths)
	}

	if len(got) > 0 {
		want := strings.Repeat(string(got[0:1]), len(got))
		if string(got) != want {
			t.Fatalf("final file is not a single uninterleaved payload (first byte %q)", got[0])
		}
	}

	// Final file must be executable after the chmod.
	info, err := os.Stat(destPath)
	if err != nil {
		t.Fatal(err)
	}

	if info.Mode()&0o111 == 0 {
		t.Error("final file is not executable")
	}

	// No stray temp files should remain in the directory.
	entries, err := os.ReadDir(filepath.Dir(destPath))
	if err != nil {
		t.Fatal(err)
	}

	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("stray temp file remained: %s", e.Name())
		}
	}
}
