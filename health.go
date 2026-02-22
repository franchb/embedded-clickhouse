package embeddedclickhouse

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

const healthPollInterval = 100 * time.Millisecond

// waitForReady polls the ClickHouse HTTP /ping endpoint until it responds with "Ok.\n" or the context is cancelled.
func waitForReady(ctx context.Context, httpPort uint32) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/ping", httpPort)
	client := &http.Client{Timeout: healthPollInterval}

	ticker := time.NewTicker(healthPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("embedded-clickhouse: server did not become ready: %w", ctx.Err())
		case <-ticker.C:
			if ping(client, url) {
				return nil
			}
		}
	}
}

func ping(client *http.Client, url string) bool {
	resp, err := client.Get(url) //nolint:noctx
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	return resp.StatusCode == http.StatusOK
}
