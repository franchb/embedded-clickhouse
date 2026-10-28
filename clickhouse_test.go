package embeddedclickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewServer_DefaultConfig(t *testing.T) {
	t.Parallel()

	s := NewServer()
	require.NotNil(t, s)
	assert.Equal(t, DefaultVersion, s.config.version)
}

func TestNewServer_CustomConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig().Version(V25_3).TCPPort(19000)
	s := NewServer(cfg)
	assert.Equal(t, V25_3, s.config.version)
	assert.Equal(t, uint32(19000), s.config.tcpPort)
}

func TestEmbeddedClickHouse_StopBeforeStart(t *testing.T) {
	t.Parallel()

	s := NewServer()
	err := s.Stop()
	assert.ErrorIs(t, err, ErrServerNotStarted)
}

func TestEmbeddedClickHouse_Accessors(t *testing.T) {
	t.Parallel()

	s := &EmbeddedClickHouse{
		started: true,
		node:    node{proc: nil, dir: "", ownsDir: false, tcpPort: 19000, httpPort: 18123, stopTimeout: 0},
	}

	assert.True(t, s.Started())
	assert.Equal(t, "127.0.0.1:19000", s.TCPAddr())
	assert.Equal(t, "127.0.0.1:18123", s.HTTPAddr())
	assert.Equal(t, "clickhouse://127.0.0.1:19000/default", s.DSN())
	assert.Equal(t, "http://127.0.0.1:18123", s.HTTPURL())
}

// TestEmbeddedClickHouse_AccessorsWhenNotRunning pins that accessors never hand out
// a well-formed but bogus "127.0.0.1:0" address before Start or after Stop.
func TestEmbeddedClickHouse_AccessorsWhenNotRunning(t *testing.T) {
	t.Parallel()

	s := NewServer()

	assert.False(t, s.Started())
	assert.Empty(t, s.TCPAddr())
	assert.Empty(t, s.HTTPAddr())
	assert.Empty(t, s.DSN())
	assert.Empty(t, s.HTTPURL())
}

func TestResolveServerPorts(t *testing.T) {
	t.Parallel()

	tcp, http, err := resolveServerPorts(0, 0)
	require.NoError(t, err)
	assert.NotZero(t, tcp)
	assert.NotZero(t, http)
	assert.NotEqual(t, tcp, http)

	tcp, http, err = resolveServerPorts(19000, 18123)
	require.NoError(t, err)
	assert.Equal(t, uint32(19000), tcp)
	assert.Equal(t, uint32(18123), http)

	// A user-fixed port must never be handed out again for the other protocol.
	for range 50 {
		fixed, err := allocatePort()
		require.NoError(t, err)

		tcp, http, err = resolveServerPorts(fixed, 0)
		require.NoError(t, err)
		assert.Equal(t, fixed, tcp)
		assert.NotEqual(t, fixed, http)
	}
}

// TestStop_AbortsInFlightStart proves Stop does not block behind a slow Start (the
// lock is not held across the readiness wait) and that it aborts that start.
func TestStop_AbortsInFlightStart(t *testing.T) {
	t.Parallel()

	fake := writeSleepingBinary(t)
	httpPort := servePingStatus(t, http.StatusServiceUnavailable)

	s := NewServer(
		DefaultConfig().
			BinaryPath(fake).
			Logger(io.Discard).
			HTTPPort(httpPort).
			StartTimeout(time.Minute),
	)

	startErr := make(chan error, 1)

	go func() { startErr <- s.Start() }()

	// Wait until the start is in flight; accessors must not block meanwhile.
	require.Eventually(t, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()

		return s.gate.inFlight()
	}, 5*time.Second, 10*time.Millisecond)

	assert.Empty(t, s.DSN(), "accessors must answer while Start is in flight")
	require.ErrorIs(t, s.Start(), ErrServerAlreadyStarted)

	stopped := make(chan error, 1)

	go func() { stopped <- s.Stop() }()

	select {
	case err := <-stopped:
		require.ErrorIs(t, err, ErrServerNotStarted)
	case <-time.After(10 * time.Second):
		t.Fatal("Stop blocked behind an in-flight Start")
	}

	require.ErrorIs(t, <-startErr, context.Canceled)
	assert.False(t, s.Started())
}

// recordingTB captures Errorf calls and Cleanup registrations so a test can assert on
// what a helper reports during cleanup.
type recordingTB struct {
	testing.TB

	errs     []string
	cleanups []func()
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

func (r *recordingTB) Cleanup(fn func()) {
	r.cleanups = append(r.cleanups, fn)
}

// TestNewServerForTest_ExplicitStopIsNotAFailure: a test that stops the server itself
// must not be failed by the registered cleanup finding it already stopped.
func TestNewServerForTest_ExplicitStopIsNotAFailure(t *testing.T) {
	t.Parallel()

	rec := &recordingTB{TB: t, errs: nil, cleanups: nil}

	s := NewServerForTest(rec, DefaultConfig().
		BinaryPath(writeSleepingBinary(t)).
		Logger(io.Discard).
		HTTPPort(servePingStatus(t, http.StatusOK)))

	require.NoError(t, s.Stop())

	for _, fn := range rec.cleanups {
		fn()
	}

	assert.Empty(t, rec.errs)
}

// TestStop_ZeroStopTimeoutUsesDefault: a zero StopTimeout must resolve to the default
// for Stop too, not only for Start. With an unresolved 0, Stop would SIGKILL instantly
// and report ErrStopTimeout instead of letting the server exit on SIGTERM.
func TestStop_ZeroStopTimeoutUsesDefault(t *testing.T) {
	t.Parallel()

	s := NewServer(DefaultConfig().
		BinaryPath(writeSleepingBinary(t)).
		Logger(io.Discard).
		HTTPPort(servePingStatus(t, http.StatusOK)).
		StopTimeout(0))

	require.NoError(t, s.Start())
	require.NoError(t, s.Stop())
}

func TestStartContext_Canceled(t *testing.T) {
	t.Parallel()

	fake := writeSleepingBinary(t)
	httpPort := servePingStatus(t, http.StatusServiceUnavailable)

	s := NewServer(DefaultConfig().BinaryPath(fake).Logger(io.Discard).HTTPPort(httpPort))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, s.StartContext(ctx), context.DeadlineExceeded)
	assert.False(t, s.Started())
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	require.EqualError(t, ErrServerNotStarted, "embedded-clickhouse: server has not been started")
	require.EqualError(t, ErrServerAlreadyStarted, "embedded-clickhouse: server is already started")
	require.EqualError(t, ErrUnsupportedPlatform, "embedded-clickhouse: unsupported platform")
}

// TestStart_ChildDiesImmediately proves that when the server process exits during
// startup, Start fails fast with ErrServerExited instead of burning the whole
// StartTimeout, and that the cleanup path (which calls stopProcess on the already-dead
// process) returns promptly without deadlocking on the single-shot Wait.
func TestStart_ChildDiesImmediately(t *testing.T) {
	t.Parallel()

	fake := writeFakeBinary(t, 3)

	// Hold a listener answering non-200 on /ping and pin HTTPPort to it, so the
	// readiness probe deterministically fails (the fake binary never binds a port)
	// and no sibling t.Parallel() test can be assigned the port and answer 200.
	httpPort := servePingStatus(t, http.StatusServiceUnavailable)

	s := NewServer(
		DefaultConfig().
			BinaryPath(fake).
			Logger(io.Discard).
			HTTPPort(httpPort).
			StartTimeout(5 * time.Second),
	)

	done := make(chan error, 1)

	start := time.Now()

	go func() {
		done <- s.Start()
	}()

	var err error

	select {
	case err = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return; stopProcess likely deadlocked on a second Wait")
	}

	elapsed := time.Since(start)

	require.ErrorIs(t, err, ErrServerExited)
	assert.Less(t, elapsed, 2*time.Second, "Start should fail fast, not burn the StartTimeout")

	// The server never became ready, so an explicit Stop reports not-started.
	assert.ErrorIs(t, s.Stop(), ErrServerNotStarted)
}

// servePingStatus serves the given status on /ping from a held listener and returns
// its port. Holding the listener keeps sibling parallel tests from being assigned it.
func servePingStatus(t *testing.T, status int) uint32 {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := &http.Server{Handler: mux}

	go srv.Serve(l)

	t.Cleanup(func() { srv.Close() })

	return uint32(l.Addr().(*net.TCPAddr).Port)
}

// writeSleepingBinary writes a fake server that stays alive until signalled.
func writeSleepingBinary(t *testing.T) string {
	t.Helper()

	// exec replaces the shell, so SIGTERM reaches sleep directly and stops it.
	return writeScriptBinary(t, "#!/bin/sh\nexec sleep 300\n")
}

// writeStubbornBinary writes a fake server that ignores SIGTERM, so stopping it always
// runs into the StopTimeout. Ignored dispositions survive exec, so sleep ignores it too.
func writeStubbornBinary(t *testing.T) string {
	t.Helper()

	return writeScriptBinary(t, "#!/bin/sh\ntrap '' TERM\nexec sleep 300\n")
}

func writeScriptBinary(t *testing.T, script string) string {
	t.Helper()

	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("/bin/sh not available")
	}

	path := filepath.Join(t.TempDir(), "fake-clickhouse.sh")

	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	return path
}

// --- Integration tests (skipped in short mode) ---

func TestIntegration_StartStop(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	s := NewServer(
		DefaultConfig().
			Version(V25_3).
			Logger(io.Discard),
	)

	require.NoError(t, s.Start())

	// Double-start should fail.
	require.ErrorIs(t, s.Start(), ErrServerAlreadyStarted)

	// HTTP ping should work.
	client := &http.Client{Timeout: time.Second}
	assert.True(t, ping(context.Background(), client, s.HTTPURL()+"/ping"))

	require.NoError(t, s.Stop())

	// Double-stop should fail.
	assert.ErrorIs(t, s.Stop(), ErrServerNotStarted)
}

func TestIntegration_SQLQueries(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	s := NewServer(
		DefaultConfig().
			Version(V25_3).
			Logger(io.Discard),
	)

	require.NoError(t, s.Start())
	defer s.Stop()

	// Connect via clickhouse-go driver.
	db, err := sql.Open("clickhouse", s.DSN())
	require.NoError(t, err)

	defer db.Close()

	ctx := context.Background()

	// SELECT 1
	var one int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
	assert.Equal(t, 1, one)

	// CREATE TABLE, INSERT, SELECT round-trip.
	_, err = db.ExecContext(ctx, `
		CREATE TABLE test_table (
			id UInt64,
			name String
		) ENGINE = MergeTree()
		ORDER BY id
	`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, "INSERT INTO test_table (id, name) VALUES (1, 'alice'), (2, 'bob')")
	require.NoError(t, err)

	rows, err := db.QueryContext(ctx, "SELECT id, name FROM test_table ORDER BY id")
	require.NoError(t, err)

	defer rows.Close()

	type row struct {
		id   uint64
		name string
	}

	var got []row

	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.id, &r.name))
		got = append(got, r)
	}

	require.NoError(t, rows.Err())

	expected := []row{{1, "alice"}, {2, "bob"}}
	assert.Equal(t, expected, got)
}

func TestIntegration_ParallelInstances(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	s1 := NewServer(DefaultConfig().Version(V25_3).Logger(io.Discard))
	s2 := NewServer(DefaultConfig().Version(V25_3).Logger(io.Discard))

	require.NoError(t, s1.Start())
	defer s1.Stop()

	require.NoError(t, s2.Start())
	defer s2.Stop()

	// Both should be on different ports.
	assert.NotEqual(t, s1.TCPAddr(), s2.TCPAddr())
	assert.NotEqual(t, s1.HTTPAddr(), s2.HTTPAddr())

	// Both should respond to ping.
	client := &http.Client{Timeout: time.Second}
	assert.True(t, ping(context.Background(), client, s1.HTTPURL()+"/ping"))
	assert.True(t, ping(context.Background(), client, s2.HTTPURL()+"/ping"))

	// Queries should work independently.
	db1, err := sql.Open("clickhouse", s1.DSN())
	require.NoError(t, err)

	defer db1.Close()

	db2, err := sql.Open("clickhouse", s2.DSN())
	require.NoError(t, err)

	defer db2.Close()

	var v1, v2 int
	require.NoError(t, db1.QueryRow("SELECT 42").Scan(&v1))
	require.NoError(t, db2.QueryRow("SELECT 99").Scan(&v2))
	assert.Equal(t, 42, v1)
	assert.Equal(t, 99, v2)
}

func TestIntegration_NewServerForTest(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	s := NewServerForTest(t, DefaultConfig().Version(V25_3).Logger(io.Discard))

	db, err := sql.Open("clickhouse", s.DSN())
	require.NoError(t, err)

	defer db.Close()

	var result int
	require.NoError(t, db.QueryRow("SELECT 1+1").Scan(&result))
	assert.Equal(t, 2, result)
}

func TestIntegration_HTTPInterface(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	s := NewServer(DefaultConfig().Version(V25_3).Logger(io.Discard))

	require.NoError(t, s.Start())
	defer s.Stop()

	// GET /ping
	resp, err := http.Get(s.HTTPURL() + "/ping")
	require.NoError(t, err)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Ok.\n", string(body))

	// Query via HTTP
	resp, err = http.Get(s.HTTPURL() + "/?query=" + "SELECT%201")
	require.NoError(t, err)

	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "1\n", string(body))
}
