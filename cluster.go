package embeddedclickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"
)

const (
	defaultClusterStartTimeout = 240 * time.Second
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

// ErrClusterUnsupportedOption is returned by Cluster.Start when the config sets a data
// path or explicit port; in cluster mode these are auto-managed and cannot be honored.
var ErrClusterUnsupportedOption = errors.New(
	"embedded-clickhouse: ports and data path are auto-managed in cluster mode",
)

// Cluster manages a multi-replica ClickHouse cluster using embedded Keeper for coordination.
// All replicas run on localhost with auto-allocated ports. The cluster presents a single
// shard with N replicas, suitable for testing ReplicatedMergeTree tables with ON CLUSTER queries.
type Cluster struct {
	config   Config
	replicas int

	mu      sync.RWMutex
	started bool
	gate    startGate
	nodes   []*EmbeddedClickHouse
}

// NewCluster creates a new Cluster with the given number of replicas.
// If StartTimeout is 0 (the DefaultConfig value), defaultClusterStartTimeout is used.
func NewCluster(replicas int, config ...Config) *Cluster {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	} else {
		cfg = DefaultConfig()
	}

	if cfg.startTimeout <= 0 {
		cfg.startTimeout = defaultClusterStartTimeout
	}

	return &Cluster{
		config:   cfg,
		replicas: replicas,
	}
}

// NewClusterForTest creates a cluster, starts it, and registers tb.Cleanup(cluster.Stop).
// Calls tb.Fatal on Start() error. A test may call Stop itself; the cleanup then
// finds the cluster already stopped and does not report that as a failure.
func NewClusterForTest(tb testing.TB, replicas int, config ...Config) *Cluster {
	tb.Helper()

	cl := NewCluster(replicas, config...)

	if err := cl.Start(); err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err := cl.Stop(); err != nil && !errors.Is(err, ErrClusterNotStarted) {
			tb.Errorf("embedded-clickhouse: cluster stop failed: %v", err)
		}
	})

	return cl
}

// Start launches all cluster nodes and waits for Keeper quorum.
// It is equivalent to StartContext(context.Background()).
func (c *Cluster) Start() error {
	return c.StartContext(context.Background())
}

// StartContext is like Start, but aborts when ctx is done. StartTimeout still bounds
// the readiness wait. A concurrent Stop also aborts an in-progress start.
func (c *Cluster) StartContext(ctx context.Context) error {
	c.mu.Lock()

	if err := c.checkStartableLocked(); err != nil {
		c.mu.Unlock()
		return err
	}

	ctx = c.gate.begin(ctx)
	cfg, replicas := c.config, c.replicas
	c.mu.Unlock()

	nodes, err := startClusterNodes(ctx, cfg, replicas)

	c.mu.Lock()

	wake := c.gate.end()

	if err == nil {
		c.nodes = nodes
		c.started = true
	}

	c.mu.Unlock()
	wake()

	return err
}

// startClusterNodes resolves the binary, launches every replica, and waits for all
// of them and for Keeper quorum. On error, every launched node is torn down.
func startClusterNodes(ctx context.Context, cfg Config, replicas int) ([]*EmbeddedClickHouse, error) {
	cfg, err := cfg.resolve(defaultClusterStartTimeout, clusterManagedKeys)
	if err != nil {
		return nil, err
	}

	// Resolve binary once (shared across all nodes).
	binPath, err := ensureBinary(ctx, cfg)
	if err != nil {
		return nil, err
	}

	// Allocate all ports upfront.
	ports := make([]clusterNodePorts, replicas)

	for i := range replicas {
		np, allocErr := allocateClusterNodePorts()
		if allocErr != nil {
			return nil, allocErr
		}

		ports[i] = np
	}

	topo := buildClusterTopology(ports, cfg.settings)

	// All nodes share one writer, so it must tolerate concurrent writes.
	output := sharedOutput(cfg.logger)

	launched := make([]*node, 0, replicas)

	success := false

	defer func() {
		if !success {
			for _, n := range slices.Backward(launched) {
				_ = n.teardown(cfg.stopTimeout)
			}
		}
	}()

	for i := range replicas {
		nd, launchErr := launchNode(nodeSpec{
			binPath:     binPath,
			dataPath:    "",
			tempPattern: fmt.Sprintf("embedded-clickhouse-cluster-%d-*", i),
			tcpPort:     ports[i].TCP,
			httpPort:    ports[i].HTTP,
			writeConfig: func(dir string) (string, error) {
				return writeClusterNodeConfig(dir, i, topo)
			},
			output: output,
		})
		if launchErr != nil {
			return nil, fmt.Errorf("embedded-clickhouse: start node %d: %w", i, launchErr)
		}

		launched = append(launched, nd)
	}

	readyCtx, cancel := context.WithTimeout(ctx, cfg.startTimeout)
	defer cancel()

	// Wait for all nodes to respond to /ping.
	if err := waitForAllNodesReady(readyCtx, launched); err != nil {
		return nil, err
	}

	// Wait for Keeper quorum.
	if err := waitForKeeperQuorum(readyCtx, launched[0].httpPort); err != nil {
		return nil, err
	}

	nodes := make([]*EmbeddedClickHouse, replicas)
	for i, n := range launched {
		nodes[i] = &EmbeddedClickHouse{config: cfg, started: true, node: *n, clusterManaged: true}
	}

	success = true

	return nodes, nil
}

// Stop shuts down all cluster nodes concurrently, so teardown takes as long as the
// slowest node rather than the sum of all of them. If a Start is still in progress,
// Stop aborts it first and waits for it to unwind.
//
// Node handles obtained from Node or Nodes are invalidated: their address accessors
// return "" and their own Start/Stop keep returning ErrClusterManaged. Restarting the
// cluster creates fresh nodes.
func (c *Cluster) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.gate.abort(&c.mu)

	if !c.started {
		return ErrClusterNotStarted
	}

	errs := make([]error, len(c.nodes))

	var wg sync.WaitGroup

	for i, member := range c.nodes {
		wg.Go(func() {
			member.mu.Lock()
			defer member.mu.Unlock()

			member.started = false

			if err := member.node.teardown(c.config.stopTimeout); err != nil {
				errs[i] = fmt.Errorf("node %d: %w", i, err)
			}
		})
	}

	wg.Wait()

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

// Nodes returns a copy of the cluster's node list. Returns nil if the cluster is not started.
func (c *Cluster) Nodes() []*EmbeddedClickHouse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return slices.Clone(c.nodes)
}

// DSN returns the DSN for the first node (shortcut for Node(0).DSN()).
func (c *Cluster) DSN() string {
	return c.Node(0).DSN()
}

// ClusterName returns the cluster name used in ON CLUSTER queries.
func (c *Cluster) ClusterName() string {
	return "test_cluster"
}

// checkStartableLocked validates that Start may proceed. Caller holds c.mu.
func (c *Cluster) checkStartableLocked() error {
	if c.started || c.gate.inFlight() {
		return ErrClusterAlreadyStarted
	}

	if c.replicas < minReplicas {
		return fmt.Errorf("%w: got %d", ErrInvalidReplicaCount, c.replicas)
	}

	// Cluster mode auto-allocates all ports and uses temp data dirs per node. The
	// single-node DataPath/TCPPort/HTTPPort options cannot be honored here (a node
	// needs five ports, and Stop assumes ephemeral temp dirs), so reject them rather
	// than silently ignore them.
	if c.config.dataPath != "" || c.config.tcpPort != 0 || c.config.httpPort != 0 {
		return ErrClusterUnsupportedOption
	}

	return nil
}

// portsPerClusterNode is the number of distinct ports each cluster node needs:
// TCP, HTTP, interserver, Keeper, and Keeper Raft.
const portsPerClusterNode = 5

// allocateClusterNodePorts allocates the distinct ports needed for a single
// cluster node. The ports must not collide with one another, so they are
// allocated as a batch with allocatePorts (which holds all listeners open at
// once); allocating them via separate allocatePort calls could hand back the
// same just-freed ephemeral port twice.
func allocateClusterNodePorts() (clusterNodePorts, error) {
	ports, err := allocatePorts(portsPerClusterNode)
	if err != nil {
		return clusterNodePorts{}, err
	}

	return clusterNodePorts{
		TCP:         ports[0],
		HTTP:        ports[1],
		Interserver: ports[2],
		Keeper:      ports[3],
		KeeperRaft:  ports[4],
	}, nil
}

// waitForAllNodesReady waits for every node's /ping endpoint to respond, in parallel.
// If any node's process exits (or otherwise fails) during startup, the first error
// cancels the shared context so the remaining nodes stop polling immediately instead
// of burning the full start timeout. Cancellation is triggered only after a real error
// is recorded, so the genuine failure (e.g. ErrServerExited) is the first error enqueued
// and is what gets returned — never a sibling's "context canceled" artifact.
// Returns the first error reported by any node, or nil if all are ready.
func waitForAllNodesReady(ctx context.Context, nodes []*node) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	readyErrs := make(chan error, len(nodes))

	var wg sync.WaitGroup
	for i, node := range nodes {
		wg.Add(1)

		go func(i int, port uint32, p *process) {
			defer wg.Done()

			if err := waitForReadyOrExit(ctx, port, p); err != nil {
				readyErrs <- fmt.Errorf("embedded-clickhouse: node %d not ready: %w", i, err)

				cancel() // stop sibling waits as soon as one node fails
			}
		}(i, node.httpPort, node.proc)
	}

	wg.Wait()
	close(readyErrs)

	if err, ok := <-readyErrs; ok {
		return err
	}

	return nil
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
