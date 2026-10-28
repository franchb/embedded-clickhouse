package embeddedclickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	healthPollInterval   = 100 * time.Millisecond
	healthRequestTimeout = 2 * time.Second
)

// pollUntil runs check immediately and then every interval until check reports done
// or fails, or ctx ends (returning ctx.Err()). A receive on wake (for example a
// process-exit channel being closed) triggers an extra check right away instead of
// waiting for the next tick; a nil wake channel never fires.
func pollUntil(ctx context.Context, interval time.Duration, wake <-chan struct{}, check func() (bool, error)) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if done, err := check(); err != nil || done {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err() //nolint:wrapcheck // callers add their own context
		case <-wake:
			wake = nil // a closed channel stays ready; check once, then rely on the ticker
		case <-ticker.C:
		}
	}
}

// exitError builds the startup error for a node whose process has exited. Call only
// once nd.proc.done is closed (so nd.proc.waitErr is safe to read). If another process
// now holds one of the node's ports, a failed bind is the likely reason the server
// exited, so the error also matches ErrPortConflict.
func exitError(nd *node) error {
	err := ErrServerExited
	if nd.proc.waitErr != nil {
		err = fmt.Errorf("%w: %w", ErrServerExited, nd.proc.waitErr)
	}

	if port, taken := takenPort(nd.ports); taken {
		return fmt.Errorf("%w: port %d is in use by another process: %w", ErrPortConflict, port, err)
	}

	return err
}

// takenPort reports the first of ports that another process is listening on. It is
// only meaningful once the node's own process has exited and released its ports.
func takenPort(ports []uint32) (uint32, bool) {
	for _, port := range ports {
		//nolint:noctx // ephemeral bind-and-close; context is meaningless
		l, err := net.Listen("tcp", net.JoinHostPort(loopbackHost, strconv.FormatUint(uint64(port), 10)))
		if err == nil {
			l.Close()

			continue
		}

		if errors.Is(err, syscall.EADDRINUSE) {
			return port, true
		}
	}

	return 0, false
}

// waitForReadyOrExit polls the node's HTTP interface until the server answers a query,
// the context is cancelled, or the server process exits. The query asks the server for
// its native TCP port, so readiness also proves that the server answering on nd.httpPort
// is this node: an answer naming another port means a different server took the HTTP
// port, and waitForReadyOrExit returns ErrPortConflict.
//
// If the process exits before becoming ready, it returns ErrServerExited (wrapping the
// underlying wait error, if any) immediately instead of burning the entire start timeout.
// Process exit always wins over a readiness response, so a child that has already died
// is never reported ready.
//
// A ready answer also means the node holds its TCP port: the config sets listen_host,
// so ClickHouse aborts startup when any listener fails to bind instead of serving
// without it.
func waitForReadyOrExit(ctx context.Context, nd *node) error {
	probeURL := fmt.Sprintf("http://127.0.0.1:%d/?query=%s", nd.httpPort, url.QueryEscape(tcpPortQuery))
	client := &http.Client{Timeout: healthRequestTimeout}

	// exited reports the process-exit error if the child has already exited, else nil.
	exited := func() error {
		select {
		case <-nd.proc.done:
			return exitError(nd)
		default:
			return nil
		}
	}

	// check reports (ready, error): error if the process exited (checked both before and
	// after the probe, so a child that dies around the probe is never reported ready) or
	// another server answered; ready is true only when this node answered.
	check := func() (bool, error) {
		if err := exited(); err != nil {
			return false, err
		}

		tcpPort, ok := serverTCPPort(ctx, client, probeURL)
		if !ok {
			return false, nil
		}

		if err := exited(); err != nil {
			return false, err
		}

		if tcpPort != nd.tcpPort {
			return false, fmt.Errorf("%w: HTTP port %d is served by another ClickHouse server "+
				"(its tcp_port is %d, want %d)", ErrPortConflict, nd.httpPort, tcpPort, nd.tcpPort)
		}

		return true, nil
	}

	err := pollUntil(ctx, healthPollInterval, nd.proc.done, check)
	if err != nil && ctx.Err() != nil {
		// Prefer the specific exit error if the process died around the deadline.
		if exitErr := exited(); exitErr != nil {
			return exitErr
		}

		return fmt.Errorf("embedded-clickhouse: server did not become ready: %w", err)
	}

	return err
}

// tcpPortQuery asks a server for the native TCP port it was configured with.
const tcpPortQuery = "SELECT getServerPort('tcp_port')"

// maxProbeBody bounds how much of a probe response is read; a port number is tiny.
const maxProbeBody = 64

// serverTCPPort runs tcpPortQuery at probeURL and returns the port the server reports.
// ok is false while the server is not answering queries yet, or when whatever answers
// is not a ClickHouse server.
func serverTCPPort(ctx context.Context, client *http.Client, probeURL string) (uint32, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
	if err != nil {
		return 0, false
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxProbeBody))
	if err != nil || resp.StatusCode != http.StatusOK {
		return 0, false
	}

	port, err := strconv.ParseUint(strings.TrimSpace(string(body)), 10, 16)
	if err != nil {
		return 0, false
	}

	return uint32(port), true
}

// ping reports whether a GET of url answers HTTP 200. It probes the Keeper quorum.
func ping(ctx context.Context, client *http.Client, target string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
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
