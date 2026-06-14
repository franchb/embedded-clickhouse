package embeddedclickhouse

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// writeFakeBinary writes an executable shell script to t.TempDir() that ignores its
// arguments and exits with the given code. It skips the test on platforms without
// /bin/sh (e.g. Windows). Returns the absolute path to the script.
func writeFakeBinary(t *testing.T, exitCode int) string {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("fake /bin/sh binary not supported on windows")
	}

	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("/bin/sh not available")
	}

	path := filepath.Join(t.TempDir(), "fake-clickhouse.sh")

	script := "#!/bin/sh\nexit " + itoa(exitCode) + "\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	return path
}

// itoa avoids pulling strconv into the test for a single small integer.
func itoa(num int) string {
	if num == 0 {
		return "0"
	}

	neg := num < 0
	if neg {
		num = -num
	}

	var buf [20]byte

	i := len(buf)
	for num > 0 {
		i--
		buf[i] = byte('0' + num%10)
		num /= 10
	}

	if neg {
		i--
		buf[i] = '-'
	}

	return string(buf[i:])
}

func TestAllocatePort(t *testing.T) {
	t.Parallel()

	port, err := allocatePort()
	if err != nil {
		t.Fatal(err)
	}

	if port == 0 {
		t.Error("port should not be 0")
	}

	if port > 65535 {
		t.Errorf("port %d out of range", port)
	}
}

func TestAllocatePort_Unique(t *testing.T) {
	t.Parallel()

	ports := make(map[uint32]bool)

	for range 10 {
		port, err := allocatePort()
		if err != nil {
			t.Fatal(err)
		}

		if ports[port] {
			// Port reuse is technically possible but unlikely in 10 allocations.
			t.Logf("warning: port %d was allocated twice", port)
		}

		ports[port] = true
	}
}

func TestStopProcess_NilCmd(t *testing.T) {
	t.Parallel()

	// nil *process and a zero-value *process (no cmd) must both be no-ops.
	if err := stopProcess(nil, 0); err != nil {
		t.Errorf("stopProcess(nil) = %v, want nil", err)
	}

	if err := stopProcess(&process{cmd: nil, done: nil, waitErr: nil}, 0); err != nil {
		t.Errorf("stopProcess(&process{}) = %v, want nil", err)
	}
}

func TestStartProcess_ExitErrorCaptured(t *testing.T) {
	t.Parallel()

	fake := writeFakeBinary(t, 3)

	proc, err := startProcess(fake, "ignored-config", io.Discard)
	if err != nil {
		t.Fatalf("startProcess: %v", err)
	}

	// The single Wait goroutine must publish the exit status via waitErr after done closes.
	<-proc.done

	var exitErr *exec.ExitError
	if !errors.As(proc.waitErr, &exitErr) {
		t.Fatalf("proc.waitErr = %v, want *exec.ExitError", proc.waitErr)
	}

	if exitErr.ExitCode() != 3 {
		t.Errorf("exit code = %d, want 3", exitErr.ExitCode())
	}

	// stopProcess on an already-exited (and reaped) process must not call Wait again
	// and must return promptly. Once the process is gone, Getpgid fails and stop is a
	// no-op (nil); the important guarantee is that it does not hang on a second Wait.
	stopDone := make(chan error, 1)

	go func() { stopDone <- stopProcess(proc, time.Second) }()

	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("stopProcess hung; likely a second cmd.Wait or single-delivery deadlock")
	}
}
