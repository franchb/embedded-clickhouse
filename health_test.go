package embeddedclickhouse

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"strings"
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

// testInstanceID is the instance ID that probe stubs report for the node under test.
const testInstanceID = "TESTINSTANCE"

// probeNode returns a node stand-in for waitForReadyOrExit, with testInstanceID.
func probeNode(proc *process, httpPort uint32, ports ...uint32) *node {
	return &node{
		proc:        proc,
		instanceID:  testInstanceID,
		dir:         "",
		ownsDir:     false,
		tcpPort:     0,
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
// with status and, on 200, reports instanceID, the way ClickHouse answers instanceQuery.
// It returns the HTTP port.
func serveProbe(t *testing.T, status int, instanceID string) uint32 {
	t.Helper()

	return serveProbeBody(t, status, instanceID+"\n")
}

func TestWaitForReady_ImmediateSuccess(t *testing.T) {
	t.Parallel()

	httpPort := serveProbe(t, http.StatusOK, testInstanceID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := waitForReadyOrExit(ctx, probeNode(liveProc(), httpPort))
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

	err = waitForReadyOrExit(ctx, probeNode(liveProc(), port))
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestWaitForReady_DelayedStart(t *testing.T) {
	t.Parallel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	port := uint32(l.Addr().(*net.TCPAddr).Port)
	l.Close() // Close immediately so nothing is listening yet.

	// Start serving after a delay.
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, testInstanceID)
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

	err = waitForReadyOrExit(ctx, probeNode(liveProc(), port))
	if err != nil {
		t.Fatal(err)
	}
}

func TestWaitForReadyOrExit_ReadyWins(t *testing.T) {
	t.Parallel()

	// The process's done channel is never closed, so readiness must win.
	httpPort := serveProbe(t, http.StatusOK, testInstanceID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := waitForReadyOrExit(ctx, probeNode(liveProc(), httpPort)); err != nil {
		t.Fatalf("waitForReadyOrExit = %v, want nil", err)
	}
}

func TestWaitForReadyOrExit_EarlyExit(t *testing.T) {
	t.Parallel()

	// Hold a listener that answers non-200 for the whole test: the readiness probe
	// then deterministically fails and the port stays bound, so a sibling
	// t.Parallel() test cannot be reassigned it and answer 200 (the
	// ephemeral-port-reuse flake that allocatePort() would expose).
	httpPort := serveProbe(t, http.StatusServiceUnavailable, "")

	// Generous context: the early-exit path, not the deadline, must end the wait.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	err := waitForReadyOrExit(ctx, probeNode(exitedProc(), httpPort))
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

// TestWaitForReadyOrExit_ForeignServer is the HTTP half of issue #51: whatever else
// answers on the node's HTTP port, another ClickHouse server (which reports its own
// instance ID, or none) or not ClickHouse at all, the wait must fail with
// ErrPortConflict instead of declaring the wrong server ready.
func TestWaitForReadyOrExit_ForeignServer(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"other instance":           "OTHERINSTANCE\n",
		"ClickHouse without macro": "",
		"not ClickHouse":           "Ok.\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			httpPort := serveProbeBody(t, http.StatusOK, body)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err := waitForReadyOrExit(ctx, probeNode(liveProc(), httpPort))
			if !errors.Is(err, ErrPortConflict) {
				t.Fatalf("waitForReadyOrExit = %v, want ErrPortConflict", err)
			}
		})
	}
}

// TestWaitForReadyOrExit_ExitOnTakenPort is the TCP half of issue #51: a server that
// exits while another process holds one of its ports lost a bind, and the error says so.
func TestWaitForReadyOrExit_ExitOnTakenPort(t *testing.T) {
	t.Parallel()

	httpPort := serveProbe(t, http.StatusServiceUnavailable, "")

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	tcpPort := uint32(l.Addr().(*net.TCPAddr).Port)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = waitForReadyOrExit(ctx, probeNode(exitedProc(), httpPort, tcpPort))
	if !errors.Is(err, ErrPortConflict) || !errors.Is(err, ErrServerExited) || !errors.Is(err, errTestProcExit) {
		t.Fatalf("waitForReadyOrExit = %v, want ErrPortConflict wrapping ErrServerExited and the wait error", err)
	}
}

// TestWaitForReadyOrExit_ExitWithNetworkError: a server that exits with NETWORK_ERROR
// failed to bind a listener even when the port's holder has since released it.
func TestWaitForReadyOrExit_ExitWithNetworkError(t *testing.T) {
	t.Parallel()

	waitErr := exec.Command(writeFakeBinary(t, networkErrorExitCode)).Run()

	done := make(chan struct{})
	close(done)

	proc := &process{cmd: nil, done: done, waitErr: waitErr}
	httpPort := serveProbe(t, http.StatusServiceUnavailable, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := waitForReadyOrExit(ctx, probeNode(proc, httpPort))
	if !errors.Is(err, ErrPortConflict) || !errors.Is(err, ErrServerExited) {
		t.Fatalf("waitForReadyOrExit = %v, want ErrPortConflict wrapping ErrServerExited", err)
	}
}

// TestExitError_IllegalInstruction: a server killed by SIGILL, as 26.6+ x86-64 builds
// are on a CPU without AVX2, gets an error that names the likely cause.
func TestExitError_IllegalInstruction(t *testing.T) {
	t.Parallel()

	waitErr := exec.Command(writeScriptBinary(t, "#!/bin/sh\nkill -ILL $$\n")).Run()

	done := make(chan struct{})
	close(done)

	err := exitError(probeNode(&process{cmd: nil, done: done, waitErr: waitErr}, 0))
	if !errors.Is(err, ErrServerExited) || !strings.Contains(err.Error(), "x86-64-v3") {
		t.Fatalf("exitError = %v, want ErrServerExited naming x86-64-v3", err)
	}

	plain := exitError(probeNode(&process{cmd: nil, done: done, waitErr: exec.Command(writeFakeBinary(t, 1)).Run()}, 0))
	if strings.Contains(plain.Error(), "x86-64-v3") {
		t.Errorf("exit status 1 should not mention the CPU: %v", plain)
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
