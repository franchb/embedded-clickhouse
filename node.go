package embeddedclickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// node is the runtime state of one running ClickHouse server process. Both the
// single-node and the cluster lifecycles create nodes with launchNode and tear them
// down with node.teardown, so the two paths cannot drift apart.
type node struct {
	proc        *process
	dir         string // server working directory: config, data, logs
	ownsDir     bool   // dir was created by launchNode and is removed by teardown
	tcpPort     uint32
	httpPort    uint32
	stopTimeout time.Duration // resolved StopTimeout, so Stop never sees an unresolved 0
}

// nodeSpec describes how to launch a node.
type nodeSpec struct {
	binPath string
	// dataPath is a persistent working directory. Empty means a fresh temporary
	// directory (named after tempPattern) that the node owns and removes on teardown.
	dataPath    string
	tempPattern string
	tcpPort     uint32
	httpPort    uint32
	stopTimeout time.Duration
	writeConfig func(dir string) (string, error)
	output      io.Writer
}

// launchNode prepares the node's directory, writes its config, and starts the server
// process. It does not wait for readiness. On error, everything it created is removed.
func launchNode(spec nodeSpec) (*node, error) {
	nd := &node{
		proc:        nil,
		dir:         "",
		ownsDir:     false,
		tcpPort:     spec.tcpPort,
		httpPort:    spec.httpPort,
		stopTimeout: spec.stopTimeout,
	}

	if spec.dataPath != "" {
		if err := os.MkdirAll(spec.dataPath, 0o755); err != nil {
			return nil, fmt.Errorf("embedded-clickhouse: create data dir: %w", err)
		}

		nd.dir = spec.dataPath
	} else {
		dir, err := os.MkdirTemp("", spec.tempPattern)
		if err != nil {
			return nil, fmt.Errorf("embedded-clickhouse: create temp dir: %w", err)
		}

		nd.dir, nd.ownsDir = dir, true
	}

	configPath, err := spec.writeConfig(nd.dir)
	if err != nil {
		_ = nd.removeDir()
		return nil, err
	}

	proc, err := startProcess(spec.binPath, configPath, spec.output)
	if err != nil {
		_ = nd.removeDir()
		return nil, err
	}

	nd.proc = proc

	return nd, nil
}

// teardown stops the server process, removes an owned working directory, and resets
// the node to its zero state so stale handles report no address. Safe on a zero node.
func (n *node) teardown() error {
	var errs []error

	if err := stopProcess(n.proc, n.stopTimeout); err != nil {
		errs = append(errs, err)
	}

	if err := n.removeDir(); err != nil {
		errs = append(errs, err)
	}

	n.proc = nil
	n.dir = ""
	n.ownsDir = false
	n.tcpPort = 0
	n.httpPort = 0

	return errors.Join(errs...)
}

func (n *node) removeDir() error {
	if !n.ownsDir || n.dir == "" {
		return nil
	}

	if err := os.RemoveAll(n.dir); err != nil {
		return fmt.Errorf("embedded-clickhouse: remove temp dir: %w", err)
	}

	return nil
}

// startGate tracks an in-flight Start so the owner's mutex is not held across the
// download and readiness wait. That keeps accessors responsive during Start and lets
// Stop abort a start that is still in progress. All methods require the owner's mutex.
type startGate struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (g *startGate) inFlight() bool {
	return g.done != nil
}

// begin marks a start as in flight and returns the context the start must honor.
func (g *startGate) begin(parent context.Context) context.Context {
	ctx, cancel := context.WithCancel(parent)
	g.cancel = cancel
	g.done = make(chan struct{})

	return ctx
}

// end clears the in-flight start. The returned function releases the start's context
// and wakes any abort waiting on it; call it after unlocking the owner's mutex.
func (g *startGate) end() func() {
	cancel, done := g.cancel, g.done
	g.cancel, g.done = nil, nil

	return func() {
		cancel()
		close(done)
	}
}

// abort cancels any in-flight start and waits for it to finish. It releases mu while
// waiting and re-acquires it before returning, so the caller must re-check its state.
func (g *startGate) abort(mu *sync.RWMutex) {
	for g.done != nil {
		cancel, done := g.cancel, g.done

		mu.Unlock()
		cancel()
		<-done
		mu.Lock()
	}
}
