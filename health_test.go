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

func TestWaitForReady_ImmediateSuccess(t *testing.T) {
	t.Parallel()

	// Start a server that responds to /ping with 200.
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Ok.\n")
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	port := uint32(l.Addr().(*net.TCPAddr).Port)

	srv := &http.Server{Handler: mux}

	go srv.Serve(l)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = waitForReady(ctx, port)
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

	err = waitForReady(ctx, port)
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
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Ok.\n")
	})

	srvCh := make(chan *http.Server, 1)

	go func() {
		time.Sleep(200 * time.Millisecond)

		l2, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return
		}

		srv := &http.Server{Handler: mux}
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

	err = waitForReady(ctx, port)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWaitForReadyOrExit_ReadyWins(t *testing.T) {
	t.Parallel()

	// Serve /ping with 200. p.done is never closed, so readiness must win.
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Ok.\n")
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	port := uint32(l.Addr().(*net.TCPAddr).Port)

	srv := &http.Server{Handler: mux}

	go srv.Serve(l)
	defer srv.Close()

	// A process whose done channel is never closed: it must not influence the result.
	proc := &process{cmd: nil, done: make(chan struct{}), waitErr: nil}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := waitForReadyOrExit(ctx, port, proc); err != nil {
		t.Fatalf("waitForReadyOrExit = %v, want nil", err)
	}
}

func TestWaitForReadyOrExit_EarlyExit(t *testing.T) {
	t.Parallel()

	// Hold a listener that answers non-200 on /ping for the whole test: the
	// readiness probe then deterministically fails and the port stays bound, so a
	// sibling t.Parallel() test cannot be reassigned it and answer 200 (the
	// ephemeral-port-reuse flake that allocatePort() would expose).
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	port := uint32(l.Addr().(*net.TCPAddr).Port)

	srv := &http.Server{Handler: mux}

	go srv.Serve(l)
	defer srv.Close()

	// A process that has already exited with a non-nil error.
	done := make(chan struct{})
	close(done)

	proc := &process{cmd: nil, done: done, waitErr: errTestProcExit}

	// Generous context: the early-exit path, not the deadline, must end the wait.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	err = waitForReadyOrExit(ctx, port, proc)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrServerExited) {
		t.Fatalf("waitForReadyOrExit = %v, want ErrServerExited", err)
	}

	if elapsed > 2*time.Second {
		t.Errorf("waitForReadyOrExit took %v, want fast early-exit", elapsed)
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
