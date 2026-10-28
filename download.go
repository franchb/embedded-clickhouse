package embeddedclickhouse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ErrDownloadTooLarge is returned when a download exceeds the size limit.
var ErrDownloadTooLarge = errors.New("embedded-clickhouse: download exceeds size limit")

// ErrDownloadStalled is returned when a download receives no data for downloadStallTimeout.
var ErrDownloadStalled = errors.New("embedded-clickhouse: download stalled")

const (
	// maxDownloadBytes caps any single download (archive or raw binary). Current
	// ClickHouse release assets are a few hundred MiB, so this leaves ample headroom
	// while still stopping a hostile or broken mirror from filling the disk.
	maxDownloadBytes int64 = 4 << 30

	// maxChecksumBytes caps a .sha512 checksum file.
	maxChecksumBytes int64 = 1 << 20

	// downloadStallTimeout aborts a download that receives no data for this long.
	// Unlike a whole-request timeout, it never cuts off a slow but progressing
	// transfer of a large archive.
	downloadStallTimeout = 2 * time.Minute
)

// httpClient is the shared HTTP client for downloads. It has no overall Timeout:
// a whole-request deadline would abort a large archive on a slow but healthy link.
// Hung transfers are caught by the per-download stall timer in fetch instead.
var httpClient = &http.Client{CheckRedirect: stripHeadersOffOrigin} //nolint:gochecknoglobals // shared pool

// maxRedirects matches net/http's default redirect limit.
const maxRedirects = 10

// errTooManyRedirects is returned when a download redirects more than maxRedirects times.
var errTooManyRedirects = errors.New("embedded-clickhouse: stopped after 10 redirects")

// stripHeadersOffOrigin keeps DownloadHeader values (credentials such as PRIVATE-TOKEN)
// on the host the user chose. net/http copies the original request's headers onto every
// redirect before calling CheckRedirect, and only strips its own short list of sensitive
// headers (Authorization, Cookie, ...) when the domain changes; a custom header would
// otherwise follow a redirect to any host, e.g. object storage behind a package registry.
// Once the chain leaves the original host, every header from the original request is
// removed for the rest of the chain. fetch sets no other headers on the original request,
// so exactly the configured ones are dropped.
func stripHeadersOffOrigin(req *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return errTooManyRedirects
	}

	origin := via[0].URL.Host

	leftOrigin := req.URL.Host != origin
	for _, hop := range via[1:] {
		leftOrigin = leftOrigin || hop.URL.Host != origin
	}

	if leftOrigin {
		for key := range via[0].Header {
			req.Header.Del(key)
		}
	}

	return nil
}

// fetchOptions controls a single HTTP download.
type fetchOptions struct {
	headers  http.Header
	maxBytes int64
	stall    time.Duration
}

// downloadOptions returns the fetch options for a download of up to maxBytes.
// headers are sent only for user-chosen URLs (see Config.DownloadHeader).
func downloadOptions(headers http.Header, maxBytes int64) fetchOptions {
	return fetchOptions{headers: headers, maxBytes: maxBytes, stall: downloadStallTimeout}
}

// userHeaders returns the configured download headers for requests to a URL the user
// chose. Requests to the default GitHub release URL never carry them.
func userHeaders(cfg Config, userChosenURL bool) http.Header {
	if !userChosenURL {
		return nil
	}

	return cfg.httpHeaders
}

// digests holds the hex SHA256 and SHA512 of a file, computed in one pass.
type digests struct {
	sha256 string
	sha512 string
}

// digester computes digests while data is written through it.
type digester struct {
	h256 hash.Hash
	h512 hash.Hash
}

func newDigester() *digester {
	return &digester{h256: sha256.New(), h512: sha512.New()}
}

func (d *digester) writer() io.Writer {
	return io.MultiWriter(d.h256, d.h512)
}

func (d *digester) sum() digests {
	return digests{sha256: hex.EncodeToString(d.h256.Sum(nil)), sha512: hex.EncodeToString(d.h512.Sum(nil))}
}

// fileDigests hashes path once, computing both SHA256 and SHA512.
func fileDigests(path string) (digests, error) {
	f, err := os.Open(path)
	if err != nil {
		return digests{}, fmt.Errorf("embedded-clickhouse: open for hashing: %w", err)
	}
	defer f.Close()

	hasher := newDigester()

	if _, err := io.Copy(hasher.writer(), f); err != nil {
		return digests{}, fmt.Errorf("embedded-clickhouse: hash %s: %w", path, err)
	}

	return hasher.sum(), nil
}

// EnsureBinary resolves the ClickHouse binary described by cfg, downloading,
// verifying, and caching it if necessary, and returns the path to the
// executable. It performs no server startup — callers that only need the binary
// (for example, to run `clickhouse local`) can use the returned path directly.
//
// Resolution follows the same precedence as Start: an explicit Config.BinaryPath
// is honored as-is, otherwise a custom archive (path or URL) or the standard
// GitHub release download is used, with SHA verification and on-disk caching
// (content-addressed for custom assets, version-pinned for standard releases).
func EnsureBinary(cfg Config) (string, error) {
	return EnsureBinaryContext(context.Background(), cfg)
}

// EnsureBinaryContext is like EnsureBinary, but aborts a download when ctx is done.
func EnsureBinaryContext(ctx context.Context, cfg Config) (string, error) {
	if err := cfg.validateBinarySource(); err != nil {
		return "", err
	}

	return ensureBinary(ctx, cfg)
}

// ensureBinary returns the path to a ClickHouse binary, downloading it if necessary.
func ensureBinary(ctx context.Context, cfg Config) (string, error) {
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
		return ensureCustomArchiveFromURL(ctx, cfg)
	}

	return ensureStandardBinary(ctx, cfg)
}

// ensureCustomArchiveFromPath extracts a ClickHouse binary from a local archive.
func ensureCustomArchiveFromPath(cfg Config) (string, error) {
	if _, err := os.Stat(cfg.customArchivePath); err != nil {
		return "", fmt.Errorf("embedded-clickhouse: custom archive not found: %w", err)
	}

	// One read computes both digests: they verify the archive and the SHA256 doubles
	// as the content-addressed cache key. (The archive must be hashed even on a warm
	// cache, because the key is derived from its content.)
	sums, err := fileDigests(cfg.customArchivePath)
	if err != nil {
		return "", err
	}

	if err := verifyCustomArchive(sums, cfg); err != nil {
		return "", err
	}

	dir, err := cacheDir(cfg.cachePath)
	if err != nil {
		return "", err
	}

	binPath := customCachedBinaryPath(dir, sums.sha256)

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
func ensureCustomArchiveFromURL(ctx context.Context, cfg Config) (string, error) {
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

	opts := downloadOptions(userHeaders(cfg, true), maxDownloadBytes)

	sums, err := downloadFile(ctx, cfg.customArchiveURL, archivePath, opts)
	if err != nil {
		return "", err
	}

	if err := verifyCustomArchive(sums, cfg); err != nil {
		return "", err
	}

	if err := extractClickHouseBinary(archivePath, binPath); err != nil {
		return "", err
	}

	logf(cfg.logger, "Done.\n")

	return binPath, nil
}

// ensureStandardBinary handles the standard GitHub release download path.
func ensureStandardBinary(ctx context.Context, cfg Config) (string, error) {
	dir, err := cacheDir(cfg.cachePath)
	if err != nil {
		return "", err
	}

	binPath := cachedBinaryPath(dir, cfg.version, cfg.binaryRepositoryURL)

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
		if err := downloadAndExtract(ctx, cfg, url, asset, binPath); err != nil {
			return "", err
		}
	case assetRawBinary:
		if err := downloadRawBinary(ctx, cfg, asset, url, binPath); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("%w: %d", ErrUnknownAssetType, asset.assetType)
	}

	logf(cfg.logger, "Done.\n")

	return binPath, nil
}

func downloadAndExtract(ctx context.Context, cfg Config, url string, asset platformAsset, binPath string) error {
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

	headers := userHeaders(cfg, cfg.binaryRepositoryURL != "")

	sums, err := downloadFile(ctx, url, archivePath, downloadOptions(headers, maxDownloadBytes))
	if err != nil {
		return err
	}

	// Verify SHA512 for archives.
	check := checksumSource{
		url:          sha512URL(cfg.binaryRepositoryURL, cfg.version, asset),
		filename:     asset.filename,
		allowMissing: cfg.allowMissingChecksum,
		headers:      headers,
	}

	if err := verifySHA512(ctx, sums.sha512, check, cfg.logger); err != nil {
		return err
	}

	return extractClickHouseBinary(archivePath, binPath)
}

func downloadRawBinary(ctx context.Context, cfg Config, asset platformAsset, url, binPath string) error {
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

	headers := userHeaders(cfg, cfg.binaryRepositoryURL != "")

	sums, err := downloadFile(ctx, url, tmp, downloadOptions(headers, maxDownloadBytes))
	if err != nil {
		return err
	}

	// Raw binaries (macOS) are published without a .sha512 upstream, so a missing
	// checksum is expected here and must not fail the download — unlike archives,
	// which always ship one and are verified strictly. A checksum that IS present
	// is still verified regardless.
	check := checksumSource{
		url:          sha512URL(cfg.binaryRepositoryURL, cfg.version, asset),
		filename:     asset.filename,
		allowMissing: true,
		headers:      headers,
	}

	if err := verifySHA512(ctx, sums.sha512, check, cfg.logger); err != nil {
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

// downloadFile downloads rawURL to destPath and returns the file's digests, computed
// while the data streams in (no second read of the file).
func downloadFile(ctx context.Context, rawURL, destPath string, opts fetchOptions) (digests, error) {
	out, err := os.Create(destPath)
	if err != nil {
		return digests{}, fmt.Errorf("embedded-clickhouse: create %s: %w", destPath, err)
	}

	hasher := newDigester()

	status, err := fetch(ctx, rawURL, io.MultiWriter(out, hasher.writer()), opts)
	if closeErr := out.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("embedded-clickhouse: close %s: %w", destPath, closeErr)
	}

	if err == nil && status != http.StatusOK {
		err = fmt.Errorf("%w: %s: HTTP %d", ErrDownloadFailed, redactURL(rawURL), status)
	}

	if err != nil {
		os.Remove(destPath)
		return digests{}, err
	}

	return hasher.sum(), nil
}

// fetch GETs rawURL and, on HTTP 200, copies the body into dst. A non-200 status is
// returned with a nil error and nothing written, so callers decide what it means.
//
// The download is aborted with ErrDownloadStalled if no data arrives for opts.stall
// (including while waiting for the response headers), and with ErrDownloadTooLarge
// once more than opts.maxBytes would be written. Every error masks credentials in
// the URL.
func fetch(ctx context.Context, rawURL string, dst io.Writer, opts fetchOptions) (int, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	stall := time.AfterFunc(opts.stall, func() { cancel(ErrDownloadStalled) })
	defer stall.Stop()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, fmt.Errorf("embedded-clickhouse: download %s: %w", redactURL(rawURL), redactURLError(err))
	}

	maps.Copy(req.Header, opts.headers)

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, fetchError(ctx, rawURL, opts.stall, redactURLError(err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil
	}

	if resp.ContentLength > opts.maxBytes {
		return 0, fmt.Errorf("%w: %s: %d bytes (limit %d)", ErrDownloadTooLarge, redactURL(rawURL),
			resp.ContentLength, opts.maxBytes)
	}

	body := &stallResetReader{r: resp.Body, timer: stall, timeout: opts.stall}

	n, err := io.Copy(dst, io.LimitReader(body, opts.maxBytes+1))
	if err != nil {
		return 0, fetchError(ctx, rawURL, opts.stall, err)
	}

	if n > opts.maxBytes {
		return 0, fmt.Errorf("%w: %s: more than %d bytes", ErrDownloadTooLarge, redactURL(rawURL), opts.maxBytes)
	}

	return resp.StatusCode, nil
}

// fetchError wraps a transfer error, reporting a stall as ErrDownloadStalled rather
// than as the bare "context canceled" the transport surfaces.
func fetchError(ctx context.Context, rawURL string, stall time.Duration, err error) error {
	if cause := context.Cause(ctx); errors.Is(cause, ErrDownloadStalled) {
		return fmt.Errorf("%w: %s: no data for %v", ErrDownloadStalled, redactURL(rawURL), stall)
	}

	return fmt.Errorf("embedded-clickhouse: download %s: %w", redactURL(rawURL), err)
}

// stallResetReader re-arms the stall timer every time data arrives.
type stallResetReader struct {
	r       io.Reader
	timer   *time.Timer
	timeout time.Duration
}

func (s *stallResetReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if n > 0 {
		s.timer.Reset(s.timeout)
	}

	return n, err //nolint:wrapcheck // transparent reader; io.EOF must pass through unwrapped
}

// checksumSource describes where to fetch a .sha512 file and how strictly to treat it.
type checksumSource struct {
	url          string
	filename     string
	allowMissing bool
	headers      http.Header
}

// verifySHA512 fetches the published checksum and compares it with actualSHA512
// (already computed during the download).
func verifySHA512(ctx context.Context, actualSHA512 string, src checksumSource, logger io.Writer) error {
	var body bytes.Buffer

	status, err := fetch(ctx, src.url, &body, downloadOptions(src.headers, maxChecksumBytes))
	if err != nil {
		return fmt.Errorf("embedded-clickhouse: download SHA512: %w", err)
	}

	if status != http.StatusOK {
		if !src.allowMissing {
			// Fail closed: a missing checksum is a verification failure by default.
			return fmt.Errorf("%w: %s: HTTP %d", ErrSHA512Unavailable, src.filename, status)
		}

		// Opt-in fallback: skip verification but warn the caller.
		logf(logger, "embedded-clickhouse: SHA512 not available for %s (HTTP %d), skipping verification\n",
			src.filename, status)

		return nil
	}

	expectedHash, err := parseSHA512(body.String(), src.filename)
	if err != nil {
		return err
	}

	if actualSHA512 != expectedHash {
		return fmt.Errorf("%w: %s: expected %s, got %s", ErrSHA512Mismatch, src.filename, expectedHash, actualSHA512)
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

// Hex-encoded digest lengths.
const (
	sha256HexLen = 64
	sha512HexLen = 128
)

// isSHA512Hex reports whether s is exactly 128 hexadecimal digits (a SHA512 digest).
func isSHA512Hex(s string) bool {
	if len(s) != sha512HexLen {
		return false
	}

	_, err := hex.DecodeString(s)

	return err == nil
}

// verifyCustomArchive checks the archive's digests against the user-provided SHA256
// and/or SHA512. If neither hash is configured, verification is skipped.
func verifyCustomArchive(sums digests, cfg Config) error {
	if cfg.sha256 != "" && sums.sha256 != strings.ToLower(cfg.sha256) {
		return fmt.Errorf("%w: expected %s, got %s", ErrSHA256Mismatch, cfg.sha256, sums.sha256)
	}

	if cfg.sha512hash != "" && sums.sha512 != strings.ToLower(cfg.sha512hash) {
		return fmt.Errorf("%w: expected %s, got %s", ErrSHA512Mismatch, cfg.sha512hash, sums.sha512)
	}

	return nil
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
	if urlErr, ok := errors.AsType[*url.Error](err); ok {
		return &url.Error{
			Op:  urlErr.Op,
			URL: redactURL(urlErr.URL),
			Err: urlErr.Err,
		}
	}

	return err
}
