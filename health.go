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

	err := pollUntil(ctx, healthPollInterval, proc.done, check)
	if err != nil && ctx.Err() != nil {
		// Prefer the specific exit error if the process died around the deadline.
		if exitErr := exited(); exitErr != nil {
			return exitErr
		}

		return fmt.Errorf("embedded-clickhouse: server did not become ready: %w", err)
	}

	return err
}

// ping reports whether a GET of url answers HTTP 200. It is the single readiness
// probe for both the server /ping endpoint and the Keeper quorum query.
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
