package embeddedclickhouse

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

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

	// Lock-free fast path.
	if _, err := os.Stat(binPath); err == nil {
		return binPath, nil
	}

	// The lock file lives in dir, so the directory must exist before locking.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("embedded-clickhouse: create cache dir: %w", err)
	}

	lock, err := acquireLock(lockPathFor(binPath))
	if err != nil {
		return "", err
	}
	defer lock.release() //nolint:errcheck

	// Re-stat under the lock: another process/goroutine may have extracted it.
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

	// Lock-free fast path.
	if _, err := os.Stat(binPath); err == nil {
		return binPath, nil
	}

	// The lock file lives in dir, so the directory must exist before locking.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("embedded-clickhouse: create cache dir: %w", err)
	}

	lock, err := acquireLock(lockPathFor(binPath))
	if err != nil {
		return "", err
	}
	defer lock.release() //nolint:errcheck

	// Re-stat under the lock: another process/goroutine may have downloaded it.
	if _, err := os.Stat(binPath); err == nil {
		return binPath, nil
	}

	logf(cfg.logger, "Downloading ClickHouse from %s...\n", redactURL(cfg.customArchiveURL))

	archiveFile, err := os.CreateTemp(dir, filepath.Base(binPath)+".tar.gz.*.tmp")
	if err != nil {
		return "", fmt.Errorf("embedded-clickhouse: create temp file: %w", err)
	}

	archivePath := archiveFile.Name()
	archiveFile.Close()

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

	// Lock-free fast path.
	if _, err := os.Stat(binPath); err == nil {
		return binPath, nil
	}

	// The lock file lives in dir, so the directory must exist before locking.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("embedded-clickhouse: create cache dir: %w", err)
	}

	lock, err := acquireLock(lockPathFor(binPath))
	if err != nil {
		return "", err
	}
	defer lock.release() //nolint:errcheck

	// Re-stat under the lock: another process/goroutine may have downloaded it.
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

	archiveFile, err := os.CreateTemp(dir, asset.filename+".*.tmp")
	if err != nil {
		return fmt.Errorf("embedded-clickhouse: create temp file: %w", err)
	}

	archivePath := archiveFile.Name()
	archiveFile.Close()

	defer os.Remove(archivePath)

	if err := downloadFile(url, archivePath); err != nil {
		return err
	}

	// Verify SHA512 for archives.
	sha512url := sha512URL(cfg.binaryRepositoryURL, cfg.version, asset)

	if err := verifySHA512(archivePath, sha512url, asset.filename, cfg.allowMissingChecksum, cfg.logger); err != nil {
		return err
	}

	return extractClickHouseBinary(archivePath, binPath)
}

func downloadRawBinary(cfg Config, asset platformAsset, url, binPath string) error {
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		return fmt.Errorf("embedded-clickhouse: create cache dir: %w", err)
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(binPath), filepath.Base(binPath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("embedded-clickhouse: create temp file: %w", err)
	}

	tmp := tmpFile.Name()
	tmpFile.Close()

	defer os.Remove(tmp)

	if err := downloadFile(url, tmp); err != nil {
		return err
	}

	sha512url := sha512URL(cfg.binaryRepositoryURL, cfg.version, asset)

	// Raw binaries (macOS) are published without a .sha512 upstream, so a missing
	// checksum is expected here and must not fail the download — unlike archives,
	// which always ship one and are verified strictly. A checksum that IS present
	// is still verified regardless.
	if err := verifySHA512(tmp, sha512url, asset.filename, true, cfg.logger); err != nil {
		return err
	}

	if err := os.Chmod(tmp, 0o755); err != nil {
		return fmt.Errorf("embedded-clickhouse: chmod binary: %w", err)
	}

	if err := os.Rename(tmp, binPath); err != nil {
		return fmt.Errorf("embedded-clickhouse: rename binary: %w", err)
	}

	return nil
}

func downloadFile(url, destPath string) error {
	resp, err := httpClient.Get(url) //nolint:noctx // URL is constructed internally
	if err != nil {
		return fmt.Errorf("embedded-clickhouse: download %s: %w", redactURL(url), redactURLError(err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: %s: HTTP %d", ErrDownloadFailed, redactURL(url), resp.StatusCode)
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

func verifySHA512(filePath, sha512URL, expectedFilename string, allowMissing bool, logger io.Writer) error {
	resp, err := httpClient.Get(sha512URL) //nolint:noctx // URL is constructed internally
	if err != nil {
		return fmt.Errorf("embedded-clickhouse: download SHA512 %s: %w", redactURL(sha512URL), redactURLError(err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if !allowMissing {
			// Fail closed: a missing checksum is a verification failure by default.
			return fmt.Errorf("%w: %s: HTTP %d", ErrSHA512Unavailable, expectedFilename, resp.StatusCode)
		}

		// Opt-in fallback: skip verification but warn the caller.
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
// Format: "<hash>  <filename>\n". It also tolerates two common real-world variants:
//   - a GNU binary-mode marker ("<hash> *<filename>") and a "./<filename>" prefix on the
//     filename token, both of which are stripped before comparison;
//   - a checksum file that contains only a single bare hash (no filename), accepted only
//     when exactly one hash-looking line is present (ambiguous multi-bare files are rejected).
func parseSHA512(content, filename string) (string, error) {
	// A standard sha512sum line is "<hash> <name>" (2 fields); a bare-hash line is 1 field.
	const hashAndNameFields = 2

	var bareHashes []string

	for line := range strings.SplitSeq(strings.TrimSpace(content), "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		// Phase 1: exact filename match (after stripping "*" / "./" markers).
		if len(parts) >= hashAndNameFields {
			candidate := strings.TrimPrefix(parts[1], "*")
			candidate = strings.TrimPrefix(candidate, "./")

			if candidate == filename {
				return strings.ToLower(parts[0]), nil
			}

			continue
		}

		// Collect single-token lines that look like a bare SHA512 hash for phase 2.
		if isSHA512Hex(parts[0]) {
			bareHashes = append(bareHashes, strings.ToLower(parts[0]))
		}
	}

	// Phase 2: accept a bare hash only when it is unambiguous (exactly one).
	if len(bareHashes) == 1 {
		return bareHashes[0], nil
	}

	return "", fmt.Errorf("%w: %s", ErrSHA512NotFound, filename)
}

// isSHA512Hex reports whether s is exactly 128 hexadecimal digits (a SHA512 digest).
func isSHA512Hex(s string) bool {
	const sha512HexLen = 128
	if len(s) != sha512HexLen {
		return false
	}

	_, err := hex.DecodeString(s)

	return err == nil
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

// redactURL returns a display-only copy of raw with credentials masked. Both userinfo
// (e.g. "oauth2:TOKEN@host") and query parameters (e.g. "?private_token=TOKEN") are
// redacted, since url.Redacted alone masks only userinfo. This is for logs and error
// messages only; the actual request URL and cache key are never altered.
func redactURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "[redacted url]"
	}

	if parsed.User != nil {
		parsed.User = url.User("redacted")
	}

	if parsed.RawQuery != "" {
		values, err := url.ParseQuery(parsed.RawQuery)
		if err != nil {
			// Unparseable query (legacy ';' separators, bad %-encoding): fail
			// closed rather than risk re-emitting a secret verbatim.
			parsed.RawQuery = "redacted"
		} else {
			for k := range values {
				values.Set(k, "redacted")
			}

			parsed.RawQuery = values.Encode()
		}
	}

	return parsed.String()
}

// redactURLError returns a copy of a *url.Error with its URL redacted. A
// *url.Error's Error() string re-embeds the full request URL — including
// query-string credentials that the stdlib does not mask — so wrapping it with
// %w would leak them. Rebuilding the *url.Error with a redacted URL keeps the
// error type intact (so callers can still errors.As for *url.Error and observe
// net.Error timeouts) while removing the credentials.
func redactURLError(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return &url.Error{
			Op:  urlErr.Op,
			URL: redactURL(urlErr.URL),
			Err: urlErr.Err,
		}
	}

	return err
}
