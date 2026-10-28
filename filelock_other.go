//go:build windows

package embeddedclickhouse

// fileLock is a stub on platforms without flock(2) support. The package builds, but
// acquireLock always fails with ErrLockingUnsupported.
type fileLock struct{}

// acquireLock is unsupported on this platform and always returns ErrLockingUnsupported.
func acquireLock(_ string) (*fileLock, error) {
	return nil, ErrLockingUnsupported
}

// release is a no-op on this platform.
func (l *fileLock) release() error {
	return nil
}
