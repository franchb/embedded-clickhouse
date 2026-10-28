//go:build !windows

package embeddedclickhouse

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"syscall"
	"time"
)

// exitCodeSIGTERM is the conventional shell exit status for a process terminated by
// SIGTERM (128 + signal number 15).
const exitCodeSIGTERM = 143

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
		// The process is already gone — it may have exited in the race between the
		// non-blocking check above and now. Drain the Wait result and classify it
		// rather than masking a recorded abnormal exit with a nil return.
		<-proc.done

		return classifyWaitErr(proc.waitErr)
	}

	_ = syscall.Kill(-pgid, syscall.SIGTERM)

	select {
	case <-time.After(timeout):
		// If the process exited right at the deadline, prefer the real exit
		// classification over a timeout.
		select {
		case <-proc.done:
			return classifyWaitErr(proc.waitErr)
		default:
		}

		// Force kill after timeout, then wait for the Wait goroutine to finish.
		_ = syscall.Kill(-pgid, syscall.SIGKILL)

		<-proc.done

		return ErrStopTimeout
	case <-proc.done:
		return classifyWaitErr(proc.waitErr)
	}
}

// classifyWaitErr maps cmd.Wait()'s error to a stop result. Only a clean exit, a
// termination by SIGTERM (the signal stopProcess sends), or exit status 143 (128+SIGTERM,
// used by wrappers that translate the signal) count as success. Every other outcome is
// surfaced, including death by any other signal: os/exec reports ExitCode() == -1 for
// every signal, so treating -1 as clean would hide a SIGSEGV/SIGABRT/OOM kill that
// happened before Stop was called.
func classifyWaitErr(err error) error {
	if err == nil {
		return nil
	}

	exitErr, ok := errors.AsType[*exec.ExitError](err)
	if !ok {
		// Non-ExitError (e.g., I/O error waiting on process): surface it.
		return err
	}

	if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok {
		if ws.Signaled() && ws.Signal() == syscall.SIGTERM {
			return nil
		}

		if ws.Exited() && ws.ExitStatus() == exitCodeSIGTERM {
			return nil
		}
	}

	return err
}
