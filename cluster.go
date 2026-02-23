package embeddedclickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	defaultClusterStartTimeout = 180 * time.Second
	keeperQuorumPollInterval   = 500 * time.Millisecond
	minReplicas                = 2
)

// ErrClusterNotStarted is returned when accessing cluster resources before Start.
var ErrClusterNotStarted = errors.New("embedded-clickhouse: cluster has not been started")

// ErrClusterAlreadyStarted is returned by Start when the cluster is already running.
var ErrClusterAlreadyStarted = errors.New("embedded-clickhouse: cluster is already started")

// ErrInvalidReplicaCount is returned when the replica count is less than 2.
var ErrInvalidReplicaCount = errors.New("embedded-clickhouse: replica count must be at least 2")

// ErrKeeperNotReady is returned when the embedded Keeper quorum is not established within the timeout.
var ErrKeeperNotReady = errors.New("embedded-clickhouse: keeper quorum not ready")

// ErrNodeOutOfRange is returned when Node() is called with an index outside [0, replicas).
var ErrNodeOutOfRange = errors.New("embedded-clickhouse: node index out of range")

// Cluster manages a multi-replica ClickHouse cluster using embedded Keeper for coordination.
// All replicas run on localhost with auto-allocated ports. The cluster presents a single
// shard with N replicas, suitable for testing ReplicatedMergeTree tables with ON CLUSTER queries.
type Cluster struct {
	config   Config
	replicas int

	mu      sync.RWMutex
	started bool
	nodes   []*EmbeddedClickHouse
}

// NewCluster creates a new Cluster with the given number of replicas.
// If no config is provided, DefaultConfig() is used with a 120s start timeout.
func NewCluster(replicas int, config ...Config) *Cluster {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	} else {
		cfg = DefaultConfig()
	}

	if cfg.startTimeout < defaultClusterStartTimeout {
		cfg.startTimeout = defaultClusterStartTimeout
	}

	return &Cluster{
		config:   cfg,
		replicas: replicas,
	}
}

// NewClusterForTest creates a cluster, starts it, and registers tb.Cleanup(cluster.Stop).
// Calls tb.Fatal on Start() error.
func NewClusterForTest(tb testing.TB, replicas int, config ...Config) *Cluster {
	tb.Helper()

	cl := NewCluster(replicas, config...)

	if err := cl.Start(); err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err := cl.Stop(); err != nil {
			tb.Errorf("embedded-clickhouse: cluster stop failed: %v", err)
		}
	})

	return cl
}

// Start launches all cluster nodes and waits for Keeper quorum.
func (c *Cluster) Start() error { //nolint:funlen // multi-phase orchestrator
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return ErrClusterAlreadyStarted
	}

	if c.replicas < minReplicas {
		return fmt.Errorf("%w: got %d", ErrInvalidReplicaCount, c.replicas)
	}

	cleanups := make([]func(), 0)
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	success := false

	defer func() {
		if !success {
			cleanup()
		}
	}()

	// Resolve binary once (shared across all nodes).
	binPath, err := ensureBinary(c.config)
	if err != nil {
		return err
	}

	// Allocate all ports upfront.
	ports := make([]clusterNodePorts, c.replicas)

	for i := range c.replicas {
		np, allocErr := allocateClusterNodePorts()
		if allocErr != nil {
			return allocErr
		}

		ports[i] = np
	}

	// Build shared topology.
	topo := buildClusterTopology(ports, c.config.settings)

	// Start each node.
	nodes := make([]*EmbeddedClickHouse, c.replicas)

	logger := c.config.logger
	if logger == nil {
		logger = os.Stdout
	}

	for i := range c.replicas {
		tmpDir, mkErr := os.MkdirTemp("", fmt.Sprintf("embedded-clickhouse-cluster-%d-*", i))
		if mkErr != nil {
			return fmt.Errorf("embedded-clickhouse: create temp dir for node %d: %w", i, mkErr)
		}

		cleanups = append(cleanups, func() { os.RemoveAll(tmpDir) })

		configPath, cfgErr := writeClusterNodeConfig(tmpDir, i, topo)
		if cfgErr != nil {
			return cfgErr
		}

		cmd, startErr := startProcess(binPath, configPath, logger)
		if startErr != nil {
			return fmt.Errorf("embedded-clickhouse: start node %d: %w", i, startErr)
		}

		cleanups = append(cleanups, func() {
			stopProcess(cmd, c.config.stopTimeout) //nolint:errcheck
		})

		nodes[i] = &EmbeddedClickHouse{
			config:          c.config,
			started:         true,
			cmd:             cmd,
			tmpDir:          tmpDir,
			tcpPort:         ports[i].TCP,
			httpPort:        ports[i].HTTP,
			interserverPort: ports[i].Interserver,
			keeperPort:      ports[i].Keeper,
			keeperRaftPort:  ports[i].KeeperRaft,
			clusterManaged:  true,
		}
	}

	// Wait for all nodes to respond to /ping.
	ctx, cancel := context.WithTimeout(context.Background(), c.config.startTimeout)
	defer cancel()

	for i, node := range nodes {
		if err := waitForReady(ctx, node.httpPort); err != nil {
			return fmt.Errorf("embedded-clickhouse: node %d not ready: %w", i, err)
		}
	}

	// Wait for Keeper quorum.
	if err := waitForKeeperQuorum(ctx, nodes[0].httpPort); err != nil {
		return err
	}

	c.nodes = nodes
	c.started = true
	success = true

	return nil
}

// Stop gracefully shuts down all cluster nodes in reverse order.
func (c *Cluster) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return ErrClusterNotStarted
	}

	var errs []error

	// Stop in reverse order.
	for i := len(c.nodes) - 1; i >= 0; i-- {
		node := c.nodes[i]

		node.mu.Lock()

		if err := stopProcess(node.cmd, c.config.stopTimeout); err != nil {
			errs = append(errs, fmt.Errorf("node %d: %w", i, err))
		}

		if node.tmpDir != "" {
			if err := os.RemoveAll(node.tmpDir); err != nil {
				errs = append(errs, fmt.Errorf("node %d: remove temp dir: %w", i, err))
			}
		}

		node.started = false
		node.cmd = nil
		node.mu.Unlock()
	}

	c.started = false
	c.nodes = nil

	return errors.Join(errs...)
}

// Node returns the i-th node (0-indexed). Panics if the cluster is not started or index is out of range.
func (c *Cluster) Node(index int) *EmbeddedClickHouse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.started {
		panic(ErrClusterNotStarted)
	}

	if index < 0 || index >= len(c.nodes) {
		panic(fmt.Sprintf("%v: %d (replicas: %d)", ErrNodeOutOfRange, index, len(c.nodes)))
	}

	return c.nodes[index]
}

// Nodes returns all cluster nodes. Returns nil if the cluster is not started.
func (c *Cluster) Nodes() []*EmbeddedClickHouse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.nodes
}

// DSN returns the DSN for the first node (shortcut for Node(0).DSN()).
func (c *Cluster) DSN() string {
	return c.Node(0).DSN()
}

// ClusterName returns the cluster name used in ON CLUSTER queries.
func (c *Cluster) ClusterName() string {
	return "test_cluster"
}

// allocateClusterNodePorts allocates the 5 ports needed for a single cluster node.
func allocateClusterNodePorts() (clusterNodePorts, error) {
	tcp, err := allocatePort()
	if err != nil {
		return clusterNodePorts{}, err
	}

	httpPort, err := allocatePort()
	if err != nil {
		return clusterNodePorts{}, err
	}

	interserver, err := allocatePort()
	if err != nil {
		return clusterNodePorts{}, err
	}

	keeper, err := allocatePort()
	if err != nil {
		return clusterNodePorts{}, err
	}

	keeperRaft, err := allocatePort()
	if err != nil {
		return clusterNodePorts{}, err
	}

	return clusterNodePorts{
		TCP:         tcp,
		HTTP:        httpPort,
		Interserver: interserver,
		Keeper:      keeper,
		KeeperRaft:  keeperRaft,
	}, nil
}

// waitForKeeperQuorum polls system.zookeeper via the HTTP interface until it succeeds
// or the context is cancelled.
func waitForKeeperQuorum(ctx context.Context, httpPort uint32) error {
	query := "SELECT 1 FROM system.zookeeper WHERE path = '/' LIMIT 1"
	checkURL := fmt.Sprintf("http://127.0.0.1:%d/?query=%s", httpPort, url.QueryEscape(query))

	client := &http.Client{Timeout: healthRequestTimeout}

	if keeperReady(ctx, client, checkURL) {
		return nil
	}

	ticker := time.NewTicker(keeperQuorumPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrKeeperNotReady, ctx.Err())
		case <-ticker.C:
			if keeperReady(ctx, client, checkURL) {
				return nil
			}
		}
	}
}

func keeperReady(ctx context.Context, client *http.Client, checkURL string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, checkURL, nil)
	if err != nil {
		return false
	}

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	return resp.StatusCode == http.StatusOK
}
