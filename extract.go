package embeddedclickhouse

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// isClickHouseBinaryPath returns true if the tar entry path looks like
// the main ClickHouse server binary (e.g., "*/usr/bin/clickhouse" or "*/bin/clickhouse").
// This avoids matching bash-completion scripts and other files also named "clickhouse".
func isClickHouseBinaryPath(name string) bool {
	// Normalize to forward slashes for matching.
	clean := filepath.ToSlash(filepath.Clean(name))

	return strings.HasSuffix(clean, "/usr/bin/clickhouse") ||
		strings.HasSuffix(clean, "/bin/clickhouse") ||
		clean == "usr/bin/clickhouse" ||
		clean == "bin/clickhouse" ||
		clean == "clickhouse"
}

// ErrBinaryTooLarge is returned when the binary inside an archive exceeds maxBinaryBytes.
var ErrBinaryTooLarge = errors.New("embedded-clickhouse: binary in archive exceeds size limit")

// maxBinaryBytes caps the extracted binary. A ClickHouse binary is well under 1 GiB;
// the cap stops a hostile or corrupt archive (e.g. a gzip bomb) from filling the disk.
const maxBinaryBytes int64 = 4 << 30

// extractClickHouseBinary extracts the clickhouse binary from a .tgz archive.
// It looks for the file at a bin/ path (e.g., usr/bin/clickhouse).
func extractClickHouseBinary(archivePath, destPath string) error {
	return extractClickHouseBinaryLimit(archivePath, destPath, maxBinaryBytes)
}

// extractClickHouseBinaryLimit is extractClickHouseBinary with an explicit size cap.
func extractClickHouseBinaryLimit(archivePath, destPath string, maxBytes int64) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("embedded-clickhouse: open archive: %w", err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("embedded-clickhouse: gzip reader: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return fmt.Errorf("embedded-clickhouse: tar reader: %w", err)
		}

		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		if !isClickHouseBinaryPath(hdr.Name) {
			continue
		}

		// tar.Reader never yields more than hdr.Size bytes for an entry, so checking
		// the header bounds what is written to disk.
		if hdr.Size > maxBytes {
			return fmt.Errorf("%w: %s is %d bytes (limit %d)", ErrBinaryTooLarge, hdr.Name, hdr.Size, maxBytes)
		}

		return writeExecutable(tr, destPath)
	}

	return fmt.Errorf("%w: %s", ErrBinaryNotFound, archivePath)
}

// writeExecutable writes reader content to destPath atomically via a temp file.
func writeExecutable(r io.Reader, destPath string) error {
	// Reject ".." path segments before Clean can fold them away. Matching the substring
	// would wrongly reject legitimate names such as "/data/ci..runner", and checking
	// after Clean would miss "/safe/dir/../escape" entirely. Cache paths are absolute
	// (see cacheDir), so a ".." segment here can only come from a crafted path.
	if slices.Contains(strings.Split(filepath.ToSlash(destPath), "/"), "..") {
		return fmt.Errorf("%w: %s", ErrInvalidPath, destPath)
	}

	destPath = filepath.Clean(destPath)

	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("embedded-clickhouse: create directory: %w", err)
	}

	// Unique temp name so concurrent writers never truncate each other's in-flight file.
	out, err := os.CreateTemp(filepath.Dir(destPath), filepath.Base(destPath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("embedded-clickhouse: create temp file: %w", err)
	}

	tmp := out.Name()

	if _, err := io.Copy(out, r); err != nil {
		out.Close()
		os.Remove(tmp)

		return fmt.Errorf("embedded-clickhouse: write binary: %w", err)
	}

	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("embedded-clickhouse: close temp file: %w", err)
	}

	// os.CreateTemp creates the file with 0600; restore the executable bit before the
	// atomic rename so the cached binary is runnable (load-bearing: tests assert this).
	if err := os.Chmod(tmp, 0o755); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("embedded-clickhouse: chmod temp file: %w", err)
	}

	if err := os.Rename(tmp, destPath); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("embedded-clickhouse: rename temp file: %w", err)
	}

	return nil
}
