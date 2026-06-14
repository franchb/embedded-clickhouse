//go:build !windows

package embeddedclickhouse

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAcquireLock_Mutex verifies that a second acquire on the same lock path blocks
// until the first holder releases. Uses channels (not sleeps) for the positive proof.
func TestAcquireLock_Mutex(t *testing.T) {
	t.Parallel()

	lockPath := filepath.Join(t.TempDir(), "cache.lock")

	l1, err := acquireLock(lockPath)
	require.NoError(t, err)

	acquired := make(chan *fileLock, 1)
	errCh := make(chan error, 1)

	go func() {
		l2, err := acquireLock(lockPath)
		if err != nil {
			errCh <- err
			return
		}

		acquired <- l2
	}()

	// While l1 is held, the second acquire must not succeed.
	select {
	case <-acquired:
		t.Fatal("second acquire succeeded while first lock was held")
	case err := <-errCh:
		t.Fatalf("second acquire errored: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected: still blocked.
	}

	// Release l1; the blocked goroutine must now acquire the lock.
	require.NoError(t, l1.release())

	select {
	case l2 := <-acquired:
		require.NoError(t, l2.release())
	case err := <-errCh:
		t.Fatalf("second acquire errored after release: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("second acquire did not proceed after first lock was released")
	}
}

// TestAcquireLock_Sequential verifies that the lock can be acquired and released
// repeatedly in sequence without blocking.
func TestAcquireLock_Sequential(t *testing.T) {
	t.Parallel()

	lockPath := filepath.Join(t.TempDir(), "cache.lock")

	for range 5 {
		l, err := acquireLock(lockPath)
		require.NoError(t, err)
		require.NoError(t, l.release())
	}
}
