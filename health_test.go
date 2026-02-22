package embeddedclickhouse

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

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
