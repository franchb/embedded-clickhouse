//go:build windows

package embeddedclickhouse

import (
	"fmt"
	"io"
	"runtime"
	"time"
)

// startProcess is unsupported on Windows: ClickHouse publishes no Windows server build,
// and process-group signalling has no direct equivalent. The stub keeps the package
// compiling so cross-platform consumers get ErrUnsupportedPlatform at runtime instead
// of a build failure.
func startProcess(_, _ string, _ io.Writer) (*process, error) {
	return nil, fmt.Errorf("%w: %s/%s", ErrUnsupportedPlatform, runtime.GOOS, runtime.GOARCH)
}

// stopProcess is a no-op on Windows, where startProcess never succeeds.
func stopProcess(_ *process, _ time.Duration) error {
	return nil
}
