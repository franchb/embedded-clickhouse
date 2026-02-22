package embeddedclickhouse

import (
	"context"
	"database/sql"
	"io"
	"net/http"
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
		tcpPort:  19000,
		httpPort: 18123,
	}

	assert.Equal(t, "127.0.0.1:19000", s.TCPAddr())
	assert.Equal(t, "127.0.0.1:18123", s.HTTPAddr())
	assert.Equal(t, "clickhouse://127.0.0.1:19000/default", s.DSN())
	assert.Equal(t, "http://127.0.0.1:18123", s.HTTPURL())
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	require.EqualError(t, ErrServerNotStarted, "embedded-clickhouse: server has not been started")
	require.EqualError(t, ErrServerAlreadyStarted, "embedded-clickhouse: server is already started")
	require.EqualError(t, ErrUnsupportedPlatform, "embedded-clickhouse: unsupported platform")
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
