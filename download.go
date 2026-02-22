package embeddedclickhouse

import (
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
	if cfg.binaryPath != "" {
		if _, err := os.Stat(cfg.binaryPath); err != nil {
			return "", fmt.Errorf("embedded-clickhouse: specified binary not found: %w", err)
		}

		return cfg.binaryPath, nil
	}

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
		if err := downloadRawBinary(url, binPath); err != nil {
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

func downloadRawBinary(url, binPath string) error {
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		return fmt.Errorf("embedded-clickhouse: create cache dir: %w", err)
	}

	tmp := binPath + ".tmp"

	if err := downloadFile(url, tmp); err != nil {
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

	// If there's exactly one line with just a hash, use it.
	lines := strings.Split(strings.TrimSpace(content), "\n")
	if len(lines) == 1 {
		parts := strings.Fields(lines[0])
		if len(parts) >= 1 && len(parts[0]) == 128 {
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

func logf(w io.Writer, format string, args ...any) {
	if w != nil {
		fmt.Fprintf(w, format, args...)
	}
}
