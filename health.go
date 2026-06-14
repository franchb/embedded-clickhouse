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

// waitForReadyOrExit polls the ClickHouse HTTP /ping endpoint until it returns HTTP 200,
// the context is cancelled, or the server process exits. If the process exits before
// becoming ready, it returns ErrServerExited (wrapping the underlying wait error, if any)
// immediately instead of burning the entire start timeout.
// exitError builds the startup error for an exited server process. Call only
// once proc.done is closed (so proc.waitErr is safe to read).
func exitError(proc *process) error {
	if proc.waitErr != nil {
		return fmt.Errorf("%w: %w", ErrServerExited, proc.waitErr)
	}

	return ErrServerExited
}

func waitForReadyOrExit(ctx context.Context, httpPort uint32, proc *process) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/ping", httpPort)
	client := &http.Client{Timeout: healthRequestTimeout}

	// exited reports the process-exit error if the child has already exited, else
	// nil. A child that has exited must never be reported ready, even if another
	// process answers /ping on the same (user-fixed) port — process exit wins.
	exited := func() error {
		select {
		case <-proc.done:
			return exitError(proc)
		default:
			return nil
		}
	}

	// Process exit takes precedence over an immediate readiness response.
	if err := exited(); err != nil {
		return err
	}

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
		case <-proc.done:
			return exitError(proc)
		case <-ticker.C:
			if err := exited(); err != nil {
				return err
			}

			if ping(ctx, client, url) {
				return nil
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
