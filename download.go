package embeddedclickhouse

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var downloadMu sync.Mutex //nolint:gochecknoglobals // serializes concurrent binary downloads

// httpClient is a shared HTTP client with a timeout to prevent indefinite hangs on slow CDNs.
var httpClient = &http.Client{Timeout: 10 * time.Minute} //nolint:gochecknoglobals

// ensureBinary returns the path to a ClickHouse binary, downloading it if necessary.
func ensureBinary(cfg Config) (string, error) {
	// Priority: BinaryPath > CustomArchivePath > CustomArchiveURL > standard download.
	if cfg.binaryPath != "" {
		if _, err := os.Stat(cfg.binaryPath); err != nil {
			return "", fmt.Errorf("embedded-clickhouse: specified binary not found: %w", err)
		}

		return cfg.binaryPath, nil
	}

	if cfg.customArchivePath != "" {
		return ensureCustomArchiveFromPath(cfg)
	}

	if cfg.customArchiveURL != "" {
		return ensureCustomArchiveFromURL(cfg)
	}

	return ensureStandardBinary(cfg)
}

// ensureCustomArchiveFromPath extracts a ClickHouse binary from a local archive.
func ensureCustomArchiveFromPath(cfg Config) (string, error) {
	if _, err := os.Stat(cfg.customArchivePath); err != nil {
		return "", fmt.Errorf("embedded-clickhouse: custom archive not found: %w", err)
	}

	if err := verifyCustomArchive(cfg.customArchivePath, cfg); err != nil {
		return "", err
	}

	// Content-addressed cache key: hash the file content.
	contentHash, err := fileSHA256(cfg.customArchivePath)
	if err != nil {
		return "", err
	}

	dir, err := cacheDir(cfg.cachePath)
	if err != nil {
		return "", err
	}

	binPath := customCachedBinaryPath(dir, contentHash)

	if _, err := os.Stat(binPath); err == nil {
		return binPath, nil
	}

	logf(cfg.logger, "Extracting ClickHouse from custom archive %s...\n", cfg.customArchivePath)

	if err := extractClickHouseBinary(cfg.customArchivePath, binPath); err != nil {
		return "", err
	}

	logf(cfg.logger, "Done.\n")

	return binPath, nil
}

// ensureCustomArchiveFromURL downloads and extracts a ClickHouse binary from a custom URL.
func ensureCustomArchiveFromURL(cfg Config) (string, error) {
	dir, err := cacheDir(cfg.cachePath)
	if err != nil {
		return "", err
	}

	// Include configured digests in cache key so hash changes invalidate the cache.
	cacheInput := cfg.customArchiveURL + "\x00" + strings.ToLower(cfg.sha256) + "\x00" + strings.ToLower(cfg.sha512hash)
	binPath := customCachedBinaryPath(dir, cacheInput)

	if _, err := os.Stat(binPath); err == nil {
		return binPath, nil
	}

	downloadMu.Lock()
	defer downloadMu.Unlock()

	// Double-check after acquiring lock.
	if _, err := os.Stat(binPath); err == nil {
		return binPath, nil
	}

	logf(cfg.logger, "Downloading ClickHouse from %s...\n", cfg.customArchiveURL)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("embedded-clickhouse: create cache dir: %w", err)
	}

	archivePath := binPath + ".tar.gz.tmp"
	defer os.Remove(archivePath)

	if err := downloadFile(cfg.customArchiveURL, archivePath); err != nil {
		return "", err
	}

	if err := verifyCustomArchive(archivePath, cfg); err != nil {
		return "", err
	}

	if err := extractClickHouseBinary(archivePath, binPath); err != nil {
		return "", err
	}

	logf(cfg.logger, "Done.\n")

	return binPath, nil
}

// ensureStandardBinary handles the standard GitHub release download path.
func ensureStandardBinary(cfg Config) (string, error) {
	dir, err := cacheDir(cfg.cachePath)
	if err != nil {
		return "", err
	}

	binPath := cachedBinaryPath(dir, cfg.version)

	if _, err := os.Stat(binPath); err == nil {
		return binPath, nil
	}

	downloadMu.Lock()
	defer downloadMu.Unlock()

	// Double-check after acquiring lock.
	if _, err := os.Stat(binPath); err == nil {
		return binPath, nil
	}

	asset, err := resolveCurrentPlatformAsset(cfg.version)
	if err != nil {
		return "", err
	}

	url := downloadURL(cfg.binaryRepositoryURL, cfg.version, asset)

	logf(cfg.logger, "Downloading ClickHouse v%s...\n", cfg.version)

	switch asset.assetType {
	case assetArchive:
		if err := downloadAndExtract(cfg, url, asset, binPath); err != nil {
			return "", err
		}
	case assetRawBinary:
		if err := downloadRawBinary(cfg, asset, url, binPath); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("%w: %d", ErrUnknownAssetType, asset.assetType)
	}

	logf(cfg.logger, "Done.\n")

	return binPath, nil
}

func downloadAndExtract(cfg Config, url string, asset platformAsset, binPath string) error {
	dir, err := cacheDir(cfg.cachePath)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("embedded-clickhouse: create cache dir: %w", err)
	}

	archivePath := filepath.Join(dir, asset.filename+".tmp")
	defer os.Remove(archivePath)

	if err := downloadFile(url, archivePath); err != nil {
		return err
	}

	// Verify SHA512 for archives.
	sha512url := sha512URL(cfg.binaryRepositoryURL, cfg.version, asset)

	if err := verifySHA512(archivePath, sha512url, asset.filename, cfg.logger); err != nil {
		return err
	}

	return extractClickHouseBinary(archivePath, binPath)
}

func downloadRawBinary(cfg Config, asset platformAsset, url, binPath string) error {
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		return fmt.Errorf("embedded-clickhouse: create cache dir: %w", err)
	}

	tmp := binPath + ".tmp"

	if err := downloadFile(url, tmp); err != nil {
		return err
	}

	sha512url := sha512URL(cfg.binaryRepositoryURL, cfg.version, asset)

	if err := verifySHA512(tmp, sha512url, asset.filename, cfg.logger); err != nil {
		os.Remove(tmp)
		return err
	}

	if err := os.Chmod(tmp, 0o755); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("embedded-clickhouse: chmod binary: %w", err)
	}

	if err := os.Rename(tmp, binPath); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("embedded-clickhouse: rename binary: %w", err)
	}

	return nil
}

func downloadFile(url, destPath string) error {
	resp, err := httpClient.Get(url) //nolint:noctx // URL is constructed internally
	if err != nil {
		return fmt.Errorf("embedded-clickhouse: download %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %s: HTTP %d", ErrDownloadFailed, url, resp.StatusCode)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("embedded-clickhouse: create %s: %w", destPath, err)
	}

	if _, err := io.Copy(out, resp.Body); err != nil {
		out.Close()
		os.Remove(destPath)

		return fmt.Errorf("embedded-clickhouse: write %s: %w", destPath, err)
	}

	if err := out.Close(); err != nil {
		os.Remove(destPath)
		return fmt.Errorf("embedded-clickhouse: close %s: %w", destPath, err)
	}

	return nil
}

func verifySHA512(filePath, sha512URL, expectedFilename string, logger io.Writer) error {
	resp, err := httpClient.Get(sha512URL) //nolint:noctx // URL is constructed internally
	if err != nil {
		return fmt.Errorf("embedded-clickhouse: download SHA512: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// SHA512 file not available — skip verification but warn the caller.
		logf(logger, "embedded-clickhouse: SHA512 not available for %s (HTTP %d), skipping verification\n",
			expectedFilename, resp.StatusCode)

		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("embedded-clickhouse: read SHA512: %w", err)
	}

	expectedHash, err := parseSHA512(string(body), expectedFilename)
	if err != nil {
		return err
	}

	actualHash, err := fileSHA512(filePath)
	if err != nil {
		return err
	}

	if actualHash != expectedHash {
		os.Remove(filePath)
		return fmt.Errorf("%w: %s: expected %s, got %s", ErrSHA512Mismatch, expectedFilename, expectedHash, actualHash)
	}

	return nil
}

// parseSHA512 parses a sha512sum-format string and returns the hex hash for the given filename.
// Format: "<hash>  <filename>\n".
func parseSHA512(content, filename string) (string, error) {
	for line := range strings.SplitSeq(strings.TrimSpace(content), "\n") {
		parts := strings.Fields(line)
		if len(parts) >= 2 && parts[1] == filename {
			return strings.ToLower(parts[0]), nil
		}
	}

	return "", fmt.Errorf("%w: %s", ErrSHA512NotFound, filename)
}

func fileSHA512(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("embedded-clickhouse: open for SHA512: %w", err)
	}
	defer f.Close()

	h := sha512.New()

	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("embedded-clickhouse: compute SHA512: %w", err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// verifyCustomArchive checks the archive against user-provided SHA256 and/or SHA512 hashes.
// If neither hash is configured, verification is skipped.
func verifyCustomArchive(archivePath string, cfg Config) error {
	if cfg.sha256 != "" {
		actual, err := fileSHA256(archivePath)
		if err != nil {
			return err
		}

		if actual != strings.ToLower(cfg.sha256) {
			return fmt.Errorf("%w: expected %s, got %s", ErrSHA256Mismatch, cfg.sha256, actual)
		}
	}

	if cfg.sha512hash != "" {
		actual, err := fileSHA512(archivePath)
		if err != nil {
			return err
		}

		if actual != strings.ToLower(cfg.sha512hash) {
			return fmt.Errorf("%w: expected %s, got %s", ErrSHA512Mismatch, cfg.sha512hash, actual)
		}
	}

	return nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("embedded-clickhouse: open for SHA256: %w", err)
	}
	defer f.Close()

	h := sha256.New()

	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("embedded-clickhouse: compute SHA256: %w", err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func logf(w io.Writer, format string, args ...any) {
	if w != nil {
		fmt.Fprintf(w, format, args...)
	}
}
