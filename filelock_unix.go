//go:build !windows

package embeddedclickhouse

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// fileLock is a cross-process advisory lock backed by flock(2).
type fileLock struct {
	f *os.File
}

// acquireLock opens (creating if needed) the lock file at lockPath and takes an
// exclusive advisory lock via flock(2). flock locks are associated with the open
// file description, so each acquireLock call (which performs its own open) serializes
// both across processes AND across goroutines within a single process. It blocks until
// the lock is available.
func acquireLock(lockPath string) (*fileLock, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644) //nolint:mnd // standard lock-file perms
	if err != nil {
		return nil, fmt.Errorf("embedded-clickhouse: open lock %s: %w", lockPath, err)
	}

	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("embedded-clickhouse: acquire lock %s: %w", lockPath, err)
	}

	return &fileLock{f: f}, nil
}

// release unlocks and closes the lock file. The lock file itself is never removed,
// since removing it would defeat the flock-based serialization (another process could
// create a fresh file and lock that instead while this one is still held).
func (l *fileLock) release() error {
	_ = unix.Flock(int(l.f.Fd()), unix.LOCK_UN)
	return l.f.Close() //nolint:wrapcheck // simple Close error; caller context is clear
}
