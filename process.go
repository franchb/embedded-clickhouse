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

	// Note: there is an inherent TOCTOU race between releasing this listener
	// and ClickHouse binding to the same port. This is unavoidable with
	// bind-and-release port allocation, but is safe in practice because
	// the port is allocated on loopback and ClickHouse binds quickly.
	l.Close()

	return port, nil
}

// process wraps a started ClickHouse server command together with a single-shot
// wait goroutine. cmd.Wait() is called exactly once (in startProcess); the result
// is published via waitErr and broadcast by closing done. Both the startup monitor
// (waitForReadyOrExit) and stopProcess observe completion by reading from done,
// avoiding the "Wait was already called" error and the single-delivery-channel
// deadlock that a second Wait or a buffered-channel handoff would cause.
type process struct {
	cmd     *exec.Cmd
	done    chan struct{} // closed exactly once, after waitErr is set
	waitErr error         // safe to read only after <-done (happens-before via close)
}

// startProcess launches the ClickHouse server process and starts the single Wait goroutine.
func startProcess(binaryPath, configPath string, logger io.Writer) (*process, error) {
	//nolint:noctx // lifecycle managed via SIGTERM/SIGKILL, not context
	cmd := exec.Command(binaryPath, "server", "--config-file="+configPath)
	cmd.Stdout = logger
	cmd.Stderr = logger
	// Set process group so we can kill the whole group on stop.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("embedded-clickhouse: start process: %w", err)
	}

	proc := &process{cmd: cmd, done: make(chan struct{}), waitErr: nil}

	go func() {
		proc.waitErr = cmd.Wait()
		close(proc.done)
	}()

	return proc, nil
}

// stopProcess sends SIGTERM and waits for graceful shutdown, then SIGKILL if needed.
// It never calls cmd.Wait() — that is owned by the goroutine started in startProcess.
// Instead it observes completion via proc.done and classifies proc.waitErr.
func stopProcess(proc *process, timeout time.Duration) error {
	if proc == nil || proc.cmd == nil || proc.cmd.Process == nil {
		return nil
	}

	// If the process has already exited (e.g. it died during startup and the
	// cleanup path is now stopping it), skip signaling: the PID has been reaped
	// and could be recycled to an unrelated process group.
	select {
	case <-proc.done:
		return classifyWaitErr(proc.waitErr)
	default:
	}

	// Send SIGTERM to the process group.
	pgid, err := syscall.Getpgid(proc.cmd.Process.Pid)
	if err != nil {
		return nil //nolint:nilerr // Getpgid fails when process is already gone — not an error.
	}

	_ = syscall.Kill(-pgid, syscall.SIGTERM)

	select {
	case <-time.After(timeout):
		// Force kill after timeout, then wait for the Wait goroutine to finish.
		_ = syscall.Kill(-pgid, syscall.SIGKILL)

		<-proc.done

		return ErrStopTimeout
	case <-proc.done:
		return classifyWaitErr(proc.waitErr)
	}
}

// classifyWaitErr maps cmd.Wait()'s error to a stop result. An exit caused by our
// SIGTERM/SIGKILL (exit code -1 for signals, or 143 = 128+SIGTERM) is expected and
// reported as success; any other exit or I/O error is surfaced.
func classifyWaitErr(err error) error {
	if err == nil {
		return nil
	}

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
