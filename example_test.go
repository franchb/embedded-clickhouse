package embeddedclickhouse_test

import (
	"io"
	"testing"
	"time"

	embeddedclickhouse "github.com/franchb/embedded-clickhouse"
)

// ExampleNewServer demonstrates the minimal Start/Stop usage pattern.
func ExampleNewServer() {
	if testing.Short() {
		return
	}

	ch := embeddedclickhouse.NewServer()
	if err := ch.Start(); err != nil {
		panic(err)
	}
	defer ch.Stop()

	// ch.DSN()     => "clickhouse://127.0.0.1:<port>/default"
	// ch.HTTPURL() => "http://127.0.0.1:<port>"

	// Output:
}

// ExampleNewServerForTest demonstrates the per-test helper pattern.
func ExampleNewServerForTest() {
	if testing.Short() {
		return
	}

	// In a real test, pass *testing.T here. The server starts automatically
	// and t.Cleanup registers Stop() for teardown.
	//
	// ch := embeddedclickhouse.NewServerForTest(t)
	// db, _ := sql.Open("clickhouse", ch.DSN())

	// Output:
}

// ExampleConfig_Settings demonstrates builder chaining and Settings usage.
func ExampleConfig_Settings() {
	cfg := embeddedclickhouse.DefaultConfig().
		Version(embeddedclickhouse.V25_3).
		TCPPort(19000).
		HTTPPort(18123).
		StartTimeout(60 * time.Second).
		Logger(io.Discard).
		Settings(map[string]string{
			"max_threads":             "2",
			"max_server_memory_usage": "2147483648", // 2 GiB
		})

	_ = cfg
	// Output:
}

// ExampleNewCluster demonstrates starting a 3-node ClickHouse cluster for replication testing.
func ExampleNewCluster() {
	if testing.Short() {
		return
	}

	cluster := embeddedclickhouse.NewCluster(3, embeddedclickhouse.DefaultConfig().Logger(io.Discard))
	if err := cluster.Start(); err != nil {
		panic(err)
	}
	defer cluster.Stop()

	// cluster.DSN()          => DSN for node 0
	// cluster.Node(0).DSN()  => same as above
	// cluster.Node(1).DSN()  => DSN for node 1
	// cluster.ClusterName()  => "test_cluster"
	//
	// Use ON CLUSTER queries with ReplicatedMergeTree:
	//   CREATE TABLE t ON CLUSTER 'test_cluster' (id UInt64)
	//     ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/t', '{replica}')
	//     ORDER BY id

	// Output:
}

// ExampleNewClusterForTest demonstrates the per-test cluster helper pattern.
func ExampleNewClusterForTest() {
	if testing.Short() {
		return
	}

	// In a real test, pass *testing.T here. The cluster starts automatically
	// and t.Cleanup registers Stop() for teardown.
	//
	// cluster := embeddedclickhouse.NewClusterForTest(t, 3)
	// db, _ := sql.Open("clickhouse", cluster.DSN())

	// Output:
}

// ExampleEmbeddedClickHouse_DSN documents the DSN accessor.
func ExampleEmbeddedClickHouse_DSN() {
	if testing.Short() {
		return
	}

	ch := embeddedclickhouse.NewServer()
	if err := ch.Start(); err != nil {
		panic(err)
	}
	defer ch.Stop()

	dsn := ch.DSN()
	_ = dsn // "clickhouse://127.0.0.1:<port>/default"
	// Output:
}
