package embeddedclickhouse

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"syscall"
	"time"
)

// allocatePort finds a free TCP port by binding to :0 and immediately closing.
func allocatePort() (uint32, error) {
	//nolint:noctx // ephemeral bind-and-close; context is meaningless
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("embedded-clickhouse: allocate port: %w", err)
	}

	tcpAddr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		l.Close()
		return 0, fmt.Errorf("%w: %T", ErrUnexpectedAddrType, l.Addr())
	}

	port := uint32(tcpAddr.Port)

	l.Close()

	return port, nil
}

// startProcess launches the ClickHouse server process.
func startProcess(binaryPath, configPath string, logger io.Writer) (*exec.Cmd, error) {
	//nolint:noctx // lifecycle managed via SIGTERM/SIGKILL, not context
	cmd := exec.Command(binaryPath, "server", "--config-file="+configPath)
	cmd.Stdout = logger
	cmd.Stderr = logger
	// Set process group so we can kill the whole group on stop.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("embedded-clickhouse: start process: %w", err)
	}

	return cmd, nil
}

// stopProcess sends SIGTERM and waits for graceful shutdown, then SIGKILL if needed.
func stopProcess(cmd *exec.Cmd, timeout time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}

	// Send SIGTERM to the process group.
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		return nil //nolint:nilerr // Getpgid fails when process is already gone — not an error.
	}

	_ = syscall.Kill(-pgid, syscall.SIGTERM)

	done := make(chan error, 1)

	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-time.After(timeout):
		// Force kill after timeout.
		_ = syscall.Kill(-pgid, syscall.SIGKILL)

		<-done

		return ErrStopTimeout
	case err := <-done:
		if err != nil {
			// Exit status from SIGTERM is expected, not an error.
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				if exitErr.ExitCode() == -1 || exitErr.ExitCode() == 143 {
					return nil
				}

				return err
			}
			// Non-ExitError (e.g., I/O error waiting on process): surface it.
			return err
		}

		return nil
	}
}
