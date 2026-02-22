package embeddedclickhouse

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestDownloadFile(t *testing.T) {
	t.Parallel()

	content := "hello clickhouse"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, content)
	}))
	defer ts.Close()

	dest := filepath.Join(t.TempDir(), "downloaded")

	err := downloadFile(ts.URL, dest)
	if err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}

	if string(got) != content {
		t.Errorf("content = %q, want %q", got, content)
	}
}

func TestDownloadFile_HTTPError(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	dest := filepath.Join(t.TempDir(), "downloaded")

	err := downloadFile(ts.URL, dest)
	if err == nil {
		t.Fatal("expected error for 404 response")
	}
}

func TestParseSHA512(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		content  string
		filename string
		want     string
		wantErr  bool
	}{
		{
			name:     "standard format",
			content:  "abc123def456  myfile.tgz\n",
			filename: "myfile.tgz",
			want:     "abc123def456",
		},
		{
			name:     "single hash line (128 chars)",
			content:  "a66ab5824e9d826188a467170e7b24b031a21f936c4c5aa73e49d4c3a01dc13627523395699cea3c1d4395db391c1f8047eace1b9a28fcac4aa0eac7a5707483  clickhouse-common-static-25.3.3.42-amd64.tgz\n",
			filename: "clickhouse-common-static-25.3.3.42-amd64.tgz",
			want:     "a66ab5824e9d826188a467170e7b24b031a21f936c4c5aa73e49d4c3a01dc13627523395699cea3c1d4395db391c1f8047eace1b9a28fcac4aa0eac7a5707483",
		},
		{
			name:     "filename not found",
			content:  "abc123  otherfile.tgz\n",
			filename: "myfile.tgz",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseSHA512(tt.content, tt.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr = %v", err, tt.wantErr)
				return
			}

			if got != tt.want {
				t.Errorf("hash = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestVerifySHA512(t *testing.T) {
	t.Parallel()

	// Create a temp file with known content.
	content := []byte("test content for sha512 verification")
	tmpDir := t.TempDir()

	filePath := filepath.Join(tmpDir, "testfile.tgz")
	if err := os.WriteFile(filePath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// Compute expected hash.
	h := sha512.Sum512(content)
	expectedHash := hex.EncodeToString(h[:])

	// Create a server that serves the SHA512 file.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s  testfile.tgz\n", expectedHash)
	}))
	defer ts.Close()

	err := verifySHA512(filePath, ts.URL, "testfile.tgz", nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestVerifySHA512_Mismatch(t *testing.T) {
	t.Parallel()

	content := []byte("real content")
	tmpDir := t.TempDir()

	filePath := filepath.Join(tmpDir, "testfile.tgz")
	if err := os.WriteFile(filePath, content, 0o644); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000  testfile.tgz\n")
	}))
	defer ts.Close()

	err := verifySHA512(filePath, ts.URL, "testfile.tgz", nil)
	if err == nil {
		t.Fatal("expected SHA512 mismatch error")
	}
}

func TestEnsureBinary_ExplicitPath(t *testing.T) {
	t.Parallel()

	// Create a fake binary.
	tmpDir := t.TempDir()

	binPath := filepath.Join(tmpDir, "clickhouse")
	if err := os.WriteFile(binPath, []byte("fake"), 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig().BinaryPath(binPath)

	got, err := ensureBinary(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if got != binPath {
		t.Errorf("binary = %q, want %q", got, binPath)
	}
}

func TestEnsureBinary_ExplicitPathNotFound(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig().BinaryPath("/nonexistent/clickhouse")

	_, err := ensureBinary(cfg)
	if err == nil {
		t.Fatal("expected error for missing binary")
	}
}

func TestEnsureBinary_CachedBinary(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	cfg := DefaultConfig().CachePath(tmpDir)

	// Pre-place a cached binary.
	binPath := cachedBinaryPath(tmpDir, cfg.version)
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(binPath, []byte("cached"), 0o755); err != nil {
		t.Fatal(err)
	}

	got, err := ensureBinary(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if got != binPath {
		t.Errorf("binary = %q, want %q", got, binPath)
	}
}

func TestFileSHA512(t *testing.T) {
	t.Parallel()

	content := []byte("test")

	tmpFile := filepath.Join(t.TempDir(), "test")
	if err := os.WriteFile(tmpFile, content, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := fileSHA512(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	h := sha512.Sum512(content)

	want := hex.EncodeToString(h[:])
	if got != want {
		t.Errorf("hash = %q, want %q", got, want)
	}
}
