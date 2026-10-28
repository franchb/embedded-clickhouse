package embeddedclickhouse

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	healthPollInterval   = 100 * time.Millisecond
	healthRequestTimeout = 2 * time.Second
)

// waitForReady polls the ClickHouse HTTP /ping endpoint until it returns HTTP 200 or the context is cancelled.
func waitForReady(ctx context.Context, httpPort uint32) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/ping", httpPort)
	client := &http.Client{Timeout: healthRequestTimeout}

	// Immediate poll to avoid unnecessary 100ms latency when the server is already up.
	if ping(ctx, client, url) {
		return nil
	}

	ticker := time.NewTicker(healthPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("embedded-clickhouse: server did not become ready: %w", ctx.Err())
		case <-ticker.C:
			if ping(ctx, client, url) {
				return nil
			}
		}
	}
}

// exitError builds the startup error for an exited server process. Call only
// once proc.done is closed (so proc.waitErr is safe to read).
func exitError(proc *process) error {
	if proc.waitErr != nil {
		return fmt.Errorf("%w: %w", ErrServerExited, proc.waitErr)
	}

	return ErrServerExited
}

// waitForReadyOrExit polls the ClickHouse HTTP /ping endpoint until it returns HTTP 200,
// the context is cancelled, or the server process exits. If the process exits before
// becoming ready, it returns ErrServerExited (wrapping the underlying wait error, if any)
// immediately instead of burning the entire start timeout. Process exit always wins over
// a readiness response, so a child that has already died is never reported ready (even if
// another process answers /ping on a user-fixed port).
func waitForReadyOrExit(ctx context.Context, httpPort uint32, proc *process) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/ping", httpPort)
	client := &http.Client{Timeout: healthRequestTimeout}

	// exited reports the process-exit error if the child has already exited, else nil.
	exited := func() error {
		select {
		case <-proc.done:
			return exitError(proc)
		default:
			return nil
		}
	}

	// check reports (ready, error): error if the process exited (checked both before and
	// after the ping, so a child that dies around the probe is never reported ready);
	// ready is true only on a /ping 200.
	check := func() (bool, error) {
		if err := exited(); err != nil {
			return false, err
		}

		if !ping(ctx, client, url) {
			return false, nil
		}

		if err := exited(); err != nil {
			return false, err
		}

		return true, nil
	}

	// Immediate poll to avoid unnecessary 100ms latency when the server is already up.
	if ready, err := check(); err != nil || ready {
		return err
	}

	ticker := time.NewTicker(healthPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Prefer the specific exit error if the process died around the deadline.
			if err := exited(); err != nil {
				return err
			}

			return fmt.Errorf("embedded-clickhouse: server did not become ready: %w", ctx.Err())
		case <-proc.done:
			return exitError(proc)
		case <-ticker.C:
			if ready, err := check(); err != nil || ready {
				return err
			}
		}
	}
}

func ping(ctx context.Context, client *http.Client, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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
