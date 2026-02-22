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
