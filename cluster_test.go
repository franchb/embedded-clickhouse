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

// --- Unit tests (no binary needed) ---

func TestNewCluster_DefaultConfig(t *testing.T) {
	t.Parallel()

	cl := NewCluster(3)
	require.NotNil(t, cl)
	assert.Equal(t, 3, cl.replicas)
	assert.Equal(t, DefaultVersion, cl.config.version)
	assert.GreaterOrEqual(t, cl.config.startTimeout, defaultClusterStartTimeout)
}

func TestNewCluster_CustomConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig().Version(V25_3).StartTimeout(180 * time.Second)
	cl := NewCluster(3, cfg)
	assert.Equal(t, V25_3, cl.config.version)
	assert.Equal(t, 180*time.Second, cl.config.startTimeout)
}

func TestNewCluster_BumpsStartTimeout(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig().StartTimeout(10 * time.Second)
	cl := NewCluster(3, cfg)
	assert.Equal(t, defaultClusterStartTimeout, cl.config.startTimeout)
}

func TestCluster_StopBeforeStart(t *testing.T) {
	t.Parallel()

	cl := NewCluster(3)
	err := cl.Stop()
	assert.ErrorIs(t, err, ErrClusterNotStarted)
}

func TestCluster_InvalidReplicaCount(t *testing.T) {
	t.Parallel()

	cl := NewCluster(1)
	err := cl.Start()
	assert.ErrorIs(t, err, ErrInvalidReplicaCount)
}

func TestCluster_ClusterName(t *testing.T) {
	t.Parallel()

	cl := NewCluster(3)
	assert.Equal(t, "test_cluster", cl.ClusterName())
}

func TestCluster_NodeOutOfRange(t *testing.T) {
	t.Parallel()

	cl := &Cluster{
		started: true,
		nodes: []*EmbeddedClickHouse{
			{tcpPort: 1},
			{tcpPort: 2},
		},
	}

	assert.Panics(t, func() { cl.Node(5) })
	assert.Panics(t, func() { cl.Node(-1) })
}

func TestCluster_DSNBeforeStart(t *testing.T) {
	t.Parallel()

	cl := NewCluster(3)

	assert.Panics(t, func() { cl.DSN() })
}

func TestCluster_NodesBeforeStart(t *testing.T) {
	t.Parallel()

	cl := NewCluster(3)
	assert.Nil(t, cl.Nodes())
}

func TestCluster_ClusterManagedGuard(t *testing.T) {
	t.Parallel()

	node := &EmbeddedClickHouse{clusterManaged: true}
	require.ErrorIs(t, node.Start(), ErrClusterManaged)
	require.ErrorIs(t, node.Stop(), ErrClusterManaged)
}

func TestAllocateClusterNodePorts(t *testing.T) {
	t.Parallel()

	np, err := allocateClusterNodePorts()
	require.NoError(t, err)
	assert.NotZero(t, np.TCP)
	assert.NotZero(t, np.HTTP)
	assert.NotZero(t, np.Interserver)
	assert.NotZero(t, np.Keeper)
	assert.NotZero(t, np.KeeperRaft)

	// All ports should be distinct.
	ports := []uint32{np.TCP, np.HTTP, np.Interserver, np.Keeper, np.KeeperRaft}
	seen := make(map[uint32]bool, len(ports))

	for _, p := range ports {
		if seen[p] {
			t.Errorf("duplicate port: %d", p)
		}

		seen[p] = true
	}
}

// --- Integration tests (skipped in short mode) ---

func TestIntegration_ClusterStartStop(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cl := NewCluster(3, DefaultConfig().Logger(io.Discard))
	require.NoError(t, cl.Start())

	// Double-start should fail.
	require.ErrorIs(t, cl.Start(), ErrClusterAlreadyStarted)

	// All 3 nodes should respond to ping.
	client := &http.Client{Timeout: time.Second}

	for i, node := range cl.Nodes() {
		assert.True(t, ping(context.Background(), client, node.HTTPURL()+"/ping"),
			"node %d not responding", i)
	}

	// Node accessors.
	assert.NotEmpty(t, cl.DSN())
	assert.NotEmpty(t, cl.Node(0).TCPAddr())
	assert.NotEmpty(t, cl.Node(1).TCPAddr())
	assert.NotEmpty(t, cl.Node(2).TCPAddr())

	// All nodes should have different TCP ports.
	assert.NotEqual(t, cl.Node(0).TCPAddr(), cl.Node(1).TCPAddr())
	assert.NotEqual(t, cl.Node(1).TCPAddr(), cl.Node(2).TCPAddr())

	require.NoError(t, cl.Stop())

	// Double-stop should fail.
	assert.ErrorIs(t, cl.Stop(), ErrClusterNotStarted)
}

func TestIntegration_ClusterReplication(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cl := NewCluster(3, DefaultConfig().Logger(io.Discard))
	require.NoError(t, cl.Start())

	defer func() {
		require.NoError(t, cl.Stop())
	}()

	ctx := context.Background()

	db0, err := sql.Open("clickhouse", cl.Node(0).DSN())
	require.NoError(t, err)

	defer db0.Close()

	db1, err := sql.Open("clickhouse", cl.Node(1).DSN())
	require.NoError(t, err)

	defer db1.Close()

	// Create a ReplicatedMergeTree table ON CLUSTER.
	_, err = db0.ExecContext(ctx, `
		CREATE TABLE test_repl ON CLUSTER 'test_cluster' (
			id UInt64,
			name String
		) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_repl', '{replica}')
		ORDER BY id
	`)
	require.NoError(t, err)

	// Insert on node 0.
	_, err = db0.ExecContext(ctx, "INSERT INTO test_repl (id, name) VALUES (1, 'alice'), (2, 'bob')")
	require.NoError(t, err)

	// Sync replica on node 1.
	_, err = db1.ExecContext(ctx, "SYSTEM SYNC REPLICA test_repl")
	require.NoError(t, err)

	// Read from node 1.
	var count int
	require.NoError(t, db1.QueryRowContext(ctx, "SELECT count() FROM test_repl").Scan(&count))
	assert.Equal(t, 2, count)
}

func TestIntegration_ClusterNewClusterForTest(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cl := NewClusterForTest(t, 2, DefaultConfig().Logger(io.Discard))

	db, err := sql.Open("clickhouse", cl.DSN())
	require.NoError(t, err)

	defer db.Close()

	var result int
	require.NoError(t, db.QueryRow("SELECT 1+1").Scan(&result))
	assert.Equal(t, 2, result)
}
