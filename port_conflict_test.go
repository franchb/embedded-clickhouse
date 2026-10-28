package embeddedclickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// holdPort binds a loopback port for the rest of the test and returns it, standing in
// for another process that took the port between allocation and the server's bind.
func holdPort(t *testing.T) uint32 {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { l.Close() })

	return uint32(l.Addr().(*net.TCPAddr).Port)
}

// errTestOther is a static stand-in for a start failure unrelated to ports.
var errTestOther = errors.New("embedded-clickhouse: test failure")

func TestRetryOnPortConflict(t *testing.T) {
	t.Parallel()

	conflict := fmt.Errorf("%w: test", ErrPortConflict)

	tests := []struct {
		name      string
		retry     bool
		failures  []error // errors returned by successive attempts; then success
		wantCalls int
		wantErr   error
	}{
		{"success first time", true, nil, 1, nil},
		{"conflict then success", true, []error{conflict}, 2, nil},
		{"conflict on every attempt", true, []error{conflict, conflict, conflict}, maxStartAttempts, ErrPortConflict},
		{"retry disabled", false, []error{conflict}, 1, ErrPortConflict},
		{"other error is not retried", true, []error{errTestOther}, 1, errTestOther},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			calls := 0

			err := retryOnPortConflict(t.Context(), tt.retry, io.Discard, func() error {
				calls++
				if calls <= len(tt.failures) {
					return tt.failures[calls-1]
				}

				return nil
			})

			assert.Equal(t, tt.wantCalls, calls)

			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

// TestRetryOnPortConflict_StopsWhenContextDone: once the start budget is spent, a
// conflict is returned rather than retried into a context that is already over.
func TestRetryOnPortConflict_StopsWhenContextDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	calls := 0

	err := retryOnPortConflict(ctx, true, io.Discard, func() error {
		calls++

		return ErrPortConflict
	})

	require.ErrorIs(t, err, ErrPortConflict)
	assert.Equal(t, 1, calls)
}

// --- Integration tests (skipped in short mode) ---

// TestIntegration_TCPPortTaken reproduces issue #51: when another process holds the
// native TCP port, Start must fail with ErrPortConflict instead of reporting a server
// whose TCPAddr belongs to someone else. Without listen_host in the config, ClickHouse
// only logs the failed bind and Start succeeded.
func TestIntegration_TCPPortTaken(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	s := NewServer(DefaultConfig().
		TCPPort(holdPort(t)).
		Logger(io.Discard).
		StartTimeout(60 * time.Second))

	err := s.Start()
	if err == nil {
		_ = s.Stop()
	}

	require.ErrorIs(t, err, ErrPortConflict)
	require.ErrorIs(t, err, ErrServerExited)
}

// TestIntegration_HTTPPortServedByOtherServer is the mirror case of issue #51: another
// ClickHouse server already answers on the HTTP port, so its readiness answer must not
// be mistaken for this server's.
func TestIntegration_HTTPPortServedByOtherServer(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	other := NewServerForTest(t, DefaultConfig().Logger(io.Discard))

	_, otherHTTP, err := net.SplitHostPort(other.HTTPAddr())
	require.NoError(t, err)

	var httpPort uint32

	_, err = fmt.Sscan(otherHTTP, &httpPort)
	require.NoError(t, err)

	s := NewServer(DefaultConfig().
		HTTPPort(httpPort).
		Logger(io.Discard).
		StartTimeout(60 * time.Second))

	err = s.Start()
	if err == nil {
		_ = s.Stop()
	}

	require.ErrorIs(t, err, ErrPortConflict)
}

// TestIntegration_ConcurrentStartsOwnTheirPorts starts several servers at once, the
// setup in which issue #51 was seen, and checks that every server's native TCP port
// leads to that same server: asked over TCPAddr, it reports its own HTTP port.
func TestIntegration_ConcurrentStartsOwnTheirPorts(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const servers = 4

	started := make([]*EmbeddedClickHouse, servers)

	var wg sync.WaitGroup

	for i := range started {
		wg.Go(func() {
			s := NewServer(DefaultConfig().Logger(io.Discard))
			if err := s.Start(); err != nil {
				t.Errorf("server %d: %v", i, err)

				return
			}

			t.Cleanup(func() { _ = s.Stop() })

			started[i] = s
		})
	}

	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}

	for i, s := range started {
		db, err := sql.Open("clickhouse", s.DSN())
		require.NoError(t, err)

		var httpPort uint16

		err = db.QueryRowContext(t.Context(), "SELECT getServerPort('http_port')").Scan(&httpPort)
		db.Close()
		require.NoError(t, err, "server %d", i)

		assert.True(t, strings.HasSuffix(s.HTTPAddr(), fmt.Sprintf(":%d", httpPort)),
			"server %d: native port %s is served by the server with HTTP port %d, not %s",
			i, s.TCPAddr(), httpPort, s.HTTPAddr())
	}
}
