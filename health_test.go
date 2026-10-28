package embeddedclickhouse

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

// errTestProcExit is a static stand-in for a process wait error in tests
// (a static sentinel rather than an inline errors.New, per err113).
var errTestProcExit = errors.New("embedded-clickhouse: test process exited")

// liveProc returns a process stand-in that never exits.
func liveProc() *process {
	return &process{cmd: nil, done: make(chan struct{}), waitErr: nil}
}

func TestPollUntil(t *testing.T) {
	t.Parallel()

	calls := 0

	err := pollUntil(t.Context(), time.Millisecond, nil, func() (bool, error) {
		calls++
		return calls == 3, nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err = %v, calls = %d; want nil, 3", err, calls)
	}

	// wake triggers a check without waiting for the (very long) interval.
	wake := make(chan struct{})
	close(wake)

	calls = 0

	err = pollUntil(t.Context(), time.Hour, wake, func() (bool, error) {
		calls++
		return calls == 2, nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("wake: err = %v, calls = %d; want nil, 2", err, calls)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	err = pollUntil(ctx, time.Millisecond, nil, func() (bool, error) { return false, nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

// probeNode returns a node stand-in for waitForReadyOrExit.
func probeNode(proc *process, tcpPort, httpPort uint32, ports ...uint32) *node {
	return &node{
		proc:        proc,
		dir:         "",
		ownsDir:     false,
		tcpPort:     tcpPort,
		httpPort:    httpPort,
		ports:       ports,
		stopTimeout: 0,
	}
}

// exitedProc returns a process stand-in that has already exited with errTestProcExit.
func exitedProc() *process {
	done := make(chan struct{})
	close(done)

	return &process{cmd: nil, done: done, waitErr: errTestProcExit}
}

// serveProbeBody answers every request with status and body from a held listener and
// returns its port. Holding the listener keeps sibling parallel tests from being
// assigned the port.
func serveProbeBody(t *testing.T, status int, body string) uint32 {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	})}

	go srv.Serve(l)

	t.Cleanup(func() { srv.Close() })

	return uint32(l.Addr().(*net.TCPAddr).Port)
}

// serveProbe stands in for a ClickHouse HTTP interface: it answers the readiness query
// with status and, on 200, reports tcpPort as the server's native port, the way
// ClickHouse answers tcpPortQuery. It returns the HTTP port.
func serveProbe(t *testing.T, status int, tcpPort uint32) uint32 {
	t.Helper()

	return serveProbeBody(t, status, fmt.Sprintf("%d\n", tcpPort))
}

func TestWaitForReady_ImmediateSuccess(t *testing.T) {
	t.Parallel()

	const tcpPort = 19000

	httpPort := serveProbe(t, http.StatusOK, tcpPort)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := waitForReadyOrExit(ctx, probeNode(liveProc(), tcpPort, httpPort))
	if err != nil {
		t.Fatal(err)
	}
}

func TestWaitForReady_Timeout(t *testing.T) {
	t.Parallel()

	// Use a port that nothing is listening on.
	port, err := allocatePort()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err = waitForReadyOrExit(ctx, probeNode(liveProc(), 19000, port))
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestWaitForReady_DelayedStart(t *testing.T) {
	t.Parallel()

	const tcpPort = 19000

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	port := uint32(l.Addr().(*net.TCPAddr).Port)
	l.Close() // Close immediately so nothing is listening yet.

	// Start serving after a delay.
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, "%d\n", tcpPort)
	})

	srvCh := make(chan *http.Server, 1)

	go func() {
		time.Sleep(200 * time.Millisecond)

		l2, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return
		}

		srv := &http.Server{Handler: handler}
		srvCh <- srv

		srv.Serve(l2)
	}()

	t.Cleanup(func() {
		select {
		case srv := <-srvCh:
			srv.Close()
		default:
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = waitForReadyOrExit(ctx, probeNode(liveProc(), tcpPort, port))
	if err != nil {
		t.Fatal(err)
	}
}

func TestWaitForReadyOrExit_ReadyWins(t *testing.T) {
	t.Parallel()

	const tcpPort = 19000

	// The process's done channel is never closed, so readiness must win.
	httpPort := serveProbe(t, http.StatusOK, tcpPort)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := waitForReadyOrExit(ctx, probeNode(liveProc(), tcpPort, httpPort)); err != nil {
		t.Fatalf("waitForReadyOrExit = %v, want nil", err)
	}
}

func TestWaitForReadyOrExit_EarlyExit(t *testing.T) {
	t.Parallel()

	// Hold a listener that answers non-200 for the whole test: the readiness probe
	// then deterministically fails and the port stays bound, so a sibling
	// t.Parallel() test cannot be reassigned it and answer 200 (the
	// ephemeral-port-reuse flake that allocatePort() would expose).
	httpPort := serveProbe(t, http.StatusServiceUnavailable, 0)

	// Generous context: the early-exit path, not the deadline, must end the wait.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	err := waitForReadyOrExit(ctx, probeNode(exitedProc(), 19000, httpPort))
	elapsed := time.Since(start)

	if !errors.Is(err, ErrServerExited) {
		t.Fatalf("waitForReadyOrExit = %v, want ErrServerExited", err)
	}

	// The node's port list is empty, so nothing it owns can be reported as taken.
	if errors.Is(err, ErrPortConflict) {
		t.Fatalf("waitForReadyOrExit = %v, want no ErrPortConflict", err)
	}

	if elapsed > 2*time.Second {
		t.Errorf("waitForReadyOrExit took %v, want fast early-exit", elapsed)
	}
}

// TestWaitForReadyOrExit_ForeignServer is the HTTP half of issue #51: a different
// ClickHouse server answering on the node's HTTP port reports its own tcp_port, and the
// wait must fail with ErrPortConflict instead of declaring the wrong server ready.
func TestWaitForReadyOrExit_ForeignServer(t *testing.T) {
	t.Parallel()

	httpPort := serveProbe(t, http.StatusOK, 29000)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := waitForReadyOrExit(ctx, probeNode(liveProc(), 19000, httpPort))
	if !errors.Is(err, ErrPortConflict) {
		t.Fatalf("waitForReadyOrExit = %v, want ErrPortConflict", err)
	}
}

// TestWaitForReadyOrExit_NotClickHouse: something that answers 200 but not with a port
// number is not a ready ClickHouse server, so the wait keeps polling until its deadline.
func TestWaitForReadyOrExit_NotClickHouse(t *testing.T) {
	t.Parallel()

	httpPort := serveProbeBody(t, http.StatusOK, "Ok.\n")

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := waitForReadyOrExit(ctx, probeNode(liveProc(), 19000, httpPort))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForReadyOrExit = %v, want context.DeadlineExceeded", err)
	}
}

// TestWaitForReadyOrExit_ExitOnTakenPort is the TCP half of issue #51: a server that
// exits while another process holds one of its ports lost a bind, and the error says so.
func TestWaitForReadyOrExit_ExitOnTakenPort(t *testing.T) {
	t.Parallel()

	httpPort := serveProbe(t, http.StatusServiceUnavailable, 0)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	tcpPort := uint32(l.Addr().(*net.TCPAddr).Port)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = waitForReadyOrExit(ctx, probeNode(exitedProc(), tcpPort, httpPort, tcpPort))
	if !errors.Is(err, ErrPortConflict) || !errors.Is(err, ErrServerExited) || !errors.Is(err, errTestProcExit) {
		t.Fatalf("waitForReadyOrExit = %v, want ErrPortConflict wrapping ErrServerExited and the wait error", err)
	}
}

func TestTakenPort(t *testing.T) {
	t.Parallel()

	if port, taken := takenPort(nil); taken {
		t.Fatalf("takenPort(nil) = %d, true; want false", port)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	held := uint32(l.Addr().(*net.TCPAddr).Port)

	if port, taken := takenPort([]uint32{held}); !taken || port != held {
		t.Fatalf("takenPort = %d, %v; want %d, true", port, taken, held)
	}
}

func TestPing(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Ok.\n")
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := &http.Server{Handler: mux}

	go srv.Serve(l)
	defer srv.Close()

	client := &http.Client{Timeout: time.Second}
	url := fmt.Sprintf("http://127.0.0.1:%d/ping", l.Addr().(*net.TCPAddr).Port)

	if !ping(context.Background(), client, url) {
		t.Error("ping should return true")
	}
}
