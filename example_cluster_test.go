package embeddedclickhouse_test

import (
	"database/sql"
	"fmt"
	"io"
	"testing"

	_ "github.com/ClickHouse/clickhouse-go/v2"

	embeddedclickhouse "github.com/franchb/embedded-clickhouse"
)

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

// ExampleCluster_replicatedTable demonstrates a full ReplicatedMergeTree workflow:
// create a table ON CLUSTER, insert on one node, sync and read from another.
func ExampleCluster_replicatedTable() {
	if testing.Short() {
		fmt.Println(2)
		return
	}

	cluster := embeddedclickhouse.NewCluster(2, embeddedclickhouse.DefaultConfig().Logger(io.Discard))
	if err := cluster.Start(); err != nil {
		panic(err)
	}
	defer cluster.Stop()

	db0, err := sql.Open("clickhouse", cluster.Node(0).DSN())
	if err != nil {
		panic(err)
	}
	defer db0.Close()

	db1, err := sql.Open("clickhouse", cluster.Node(1).DSN())
	if err != nil {
		panic(err)
	}
	defer db1.Close()

	// Create a replicated table on all nodes.
	_, err = db0.Exec(`
		CREATE TABLE example_repl ON CLUSTER 'test_cluster' (
			id UInt64,
			name String
		) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/example_repl', '{replica}')
		ORDER BY id
	`)
	if err != nil {
		panic(err)
	}

	// Insert on node 0.
	_, err = db0.Exec("INSERT INTO example_repl (id, name) VALUES (1, 'alice'), (2, 'bob')")
	if err != nil {
		panic(err)
	}

	// Sync and read from node 1.
	_, err = db1.Exec("SYSTEM SYNC REPLICA example_repl")
	if err != nil {
		panic(err)
	}

	var count int
	if err := db1.QueryRow("SELECT count() FROM example_repl").Scan(&count); err != nil {
		panic(err)
	}

	fmt.Println(count)
	// Output: 2
}
