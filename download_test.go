package embeddedclickhouse

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
			content:  "a66ab5824e9d826188a467170e7b24b031a21f936c4c5aa73e49d4c3a01dc13627523395699cea3c1d4395db391c1f8047eace1b9a28fcac4aa0eac7a5707483\n",
			filename: "clickhouse-common-static-25.3.3.42-amd64.tgz",
			wantErr:  true,
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

func TestDownloadRawBinary_WithVerification(t *testing.T) {
	t.Parallel()

	binaryContent := []byte("fake clickhouse binary")
	h := sha512.Sum512(binaryContent)
	expectedHash := hex.EncodeToString(h[:])
	filename := "clickhouse-macos"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".sha512") {
			fmt.Fprintf(w, "%s  %s\n", expectedHash, filename)
		} else {
			w.Write(binaryContent)
		}
	}))
	defer ts.Close()

	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, filename)
	asset := platformAsset{filename: filename, assetType: assetRawBinary}
	cfg := DefaultConfig().BinaryRepositoryURL(ts.URL).CachePath(tmpDir)

	if err := downloadRawBinary(cfg, asset, ts.URL+"/"+filename, binPath); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(binPath)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, binaryContent) {
		t.Errorf("binary content mismatch")
	}
}

func TestDownloadRawBinary_SHA512Unavailable(t *testing.T) {
	t.Parallel()

	binaryContent := []byte("fake binary no sha512")
	filename := "clickhouse-macos"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".sha512") {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.Write(binaryContent)
		}
	}))
	defer ts.Close()

	tmpDir := t.TempDir()
	binPath := filepath.Join(tmpDir, filename)
	asset := platformAsset{filename: filename, assetType: assetRawBinary}
	cfg := DefaultConfig().BinaryRepositoryURL(ts.URL).CachePath(tmpDir)

	if err := downloadRawBinary(cfg, asset, ts.URL+"/"+filename, binPath); err != nil {
		t.Fatal(err)
	}
}

// createTestArchive creates a .tar.gz with a single "clickhouse" binary entry.
func createTestArchive(t *testing.T, dir string) string {
	t.Helper()

	archivePath := filepath.Join(dir, "clickhouse-test.tar.gz")

	f, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}

	gw := gzip.NewWriter(f)
	tw := tar.NewWriter(gw)

	binaryContent := []byte("#!/bin/sh\necho clickhouse")

	hdr := new(tar.Header)
	hdr.Name = "clickhouse"
	hdr.Mode = 0o755

	hdr.Size = int64(len(binaryContent))
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}

	if _, err := tw.Write(binaryContent); err != nil {
		t.Fatal(err)
	}

	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	return archivePath
}

func TestEnsureBinary_CustomArchivePath(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	archivePath := createTestArchive(t, tmpDir)
	cacheDir := filepath.Join(tmpDir, "cache")

	cfg := DefaultConfig().
		CustomArchivePath(archivePath).
		CachePath(cacheDir).
		Logger(io.Discard)

	got, err := ensureBinary(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(got, cacheDir) {
		t.Errorf("binary path %q not under cache dir %q", got, cacheDir)
	}

	// Binary should be executable.
	info, err := os.Stat(got)
	if err != nil {
		t.Fatal(err)
	}

	if info.Mode()&0o111 == 0 {
		t.Error("extracted binary is not executable")
	}

	// Second call should return cached path.
	got2, err := ensureBinary(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if got != got2 {
		t.Errorf("cache miss: first=%q, second=%q", got, got2)
	}
}

func TestEnsureBinary_CustomArchivePath_NotFound(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig().
		CustomArchivePath("/nonexistent/archive.tar.gz").
		Logger(io.Discard)

	_, err := ensureBinary(cfg)
	if err == nil {
		t.Fatal("expected error for missing archive")
	}
}

func TestEnsureBinary_CustomArchivePath_SHA256(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	archivePath := createTestArchive(t, tmpDir)

	// Compute the actual SHA256.
	content, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}

	h := sha256.Sum256(content)
	hash := hex.EncodeToString(h[:])

	cfg := DefaultConfig().
		CustomArchivePath(archivePath).
		SHA256(hash).
		CachePath(filepath.Join(tmpDir, "cache")).
		Logger(io.Discard)

	_, err = ensureBinary(cfg)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnsureBinary_CustomArchivePath_SHA256Mismatch(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	archivePath := createTestArchive(t, tmpDir)

	cfg := DefaultConfig().
		CustomArchivePath(archivePath).
		SHA256("0000000000000000000000000000000000000000000000000000000000000000").
		CachePath(filepath.Join(tmpDir, "cache")).
		Logger(io.Discard)

	_, err := ensureBinary(cfg)
	if err == nil {
		t.Fatal("expected SHA256 mismatch error")
	}

	if !errors.Is(err, ErrSHA256Mismatch) {
		t.Fatalf("expected ErrSHA256Mismatch, got: %v", err)
	}
}

func TestEnsureBinary_CustomArchiveURL(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	archivePath := createTestArchive(t, tmpDir)

	archiveContent, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(archiveContent)
	}))
	defer ts.Close()

	cacheDir := filepath.Join(tmpDir, "cache")

	cfg := DefaultConfig().
		CustomArchiveURL(ts.URL + "/clickhouse.tar.gz").
		CachePath(cacheDir).
		Logger(io.Discard)

	got, err := ensureBinary(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(got, cacheDir) {
		t.Errorf("binary path %q not under cache dir %q", got, cacheDir)
	}

	// Binary should be executable.
	info, err := os.Stat(got)
	if err != nil {
		t.Fatal(err)
	}

	if info.Mode()&0o111 == 0 {
		t.Error("extracted binary is not executable")
	}

	// Second call should use cache.
	got2, err := ensureBinary(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if got != got2 {
		t.Errorf("cache miss: first=%q, second=%q", got, got2)
	}
}

func TestEnsureBinary_CustomArchiveURL_WithSHA256(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	archivePath := createTestArchive(t, tmpDir)

	archiveContent, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}

	h := sha256.Sum256(archiveContent)
	hash := hex.EncodeToString(h[:])

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(archiveContent)
	}))
	defer ts.Close()

	cfg := DefaultConfig().
		CustomArchiveURL(ts.URL + "/clickhouse.tar.gz").
		SHA256(hash).
		CachePath(filepath.Join(tmpDir, "cache")).
		Logger(io.Discard)

	_, err = ensureBinary(cfg)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnsureBinary_CustomArchiveURL_SHA256Mismatch(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	archivePath := createTestArchive(t, tmpDir)

	archiveContent, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(archiveContent)
	}))
	defer ts.Close()

	cfg := DefaultConfig().
		CustomArchiveURL(ts.URL + "/clickhouse.tar.gz").
		SHA256("0000000000000000000000000000000000000000000000000000000000000000").
		CachePath(filepath.Join(tmpDir, "cache")).
		Logger(io.Discard)

	_, err = ensureBinary(cfg)
	if err == nil {
		t.Fatal("expected SHA256 mismatch error")
	}

	if !errors.Is(err, ErrSHA256Mismatch) {
		t.Fatalf("expected ErrSHA256Mismatch, got: %v", err)
	}
}

func TestEnsureBinary_Precedence(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	// Create a fake binary for BinaryPath.
	binPath := filepath.Join(tmpDir, "explicit-binary")
	if err := os.WriteFile(binPath, []byte("explicit"), 0o755); err != nil {
		t.Fatal(err)
	}

	archivePath := createTestArchive(t, tmpDir)

	// BinaryPath should take precedence over CustomArchivePath.
	cfg := DefaultConfig().
		BinaryPath(binPath).
		CustomArchivePath(archivePath).
		CachePath(filepath.Join(tmpDir, "cache")).
		Logger(io.Discard)

	got, err := ensureBinary(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if got != binPath {
		t.Errorf("BinaryPath should take precedence, got %q, want %q", got, binPath)
	}
}

func TestFileSHA256(t *testing.T) {
	t.Parallel()

	content := []byte("test sha256 content")

	tmpFile := filepath.Join(t.TempDir(), "test")
	if err := os.WriteFile(tmpFile, content, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := fileSHA256(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	h := sha256.Sum256(content)
	want := hex.EncodeToString(h[:])

	if got != want {
		t.Errorf("hash = %q, want %q", got, want)
	}
}

func TestVerifyCustomArchive_BothHashes(t *testing.T) {
	t.Parallel()

	content := []byte("test verify content")
	tmpFile := filepath.Join(t.TempDir(), "test.tar.gz")

	if err := os.WriteFile(tmpFile, content, 0o644); err != nil {
		t.Fatal(err)
	}

	sha256Hash := sha256.Sum256(content)
	sha512Hash := sha512.Sum512(content)

	cfg := DefaultConfig().
		SHA256(hex.EncodeToString(sha256Hash[:])).
		SHA512(hex.EncodeToString(sha512Hash[:]))

	if err := verifyCustomArchive(tmpFile, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyCustomArchive_NoHashes(t *testing.T) {
	t.Parallel()

	tmpFile := filepath.Join(t.TempDir(), "test.tar.gz")
	if err := os.WriteFile(tmpFile, []byte("anything"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()

	if err := verifyCustomArchive(tmpFile, cfg); err != nil {
		t.Fatal("expected no error when no hashes configured")
	}
}
