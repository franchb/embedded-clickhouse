package embeddedclickhouse

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
)

// loopbackHost is the address every server listens on and every client dials.
const loopbackHost = "127.0.0.1"

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

	// Note: there is an inherent TOCTOU race between releasing this listener and
	// ClickHouse binding to the same port, and the window is wide: ClickHouse loads
	// its config and initializes before it binds. Another process can take the port
	// in between. The server config sets listen_host, so ClickHouse then exits instead
	// of running without the listener; waitForReadyOrExit reports that as
	// ErrPortConflict, and Start retries with fresh ports (see retryOnPortConflict).
	l.Close()

	return port, nil
}

// allocatePorts finds count distinct free TCP ports. Unlike calling allocatePort
// in a loop, it keeps every listener open until all of them are bound, so the
// kernel cannot reassign a just-freed ephemeral port to a later iteration. This
// makes the returned ports distinct by construction rather than by chance.
//
// The same TOCTOU caveat, and its handling, documented on allocatePort applies once
// the listeners are released here.
func allocatePorts(count int) ([]uint32, error) {
	listeners := make([]net.Listener, 0, count)

	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()

	ports := make([]uint32, 0, count)

	for range count {
		//nolint:noctx // ephemeral bind-and-close; context is meaningless
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("embedded-clickhouse: allocate port: %w", err)
		}

		listeners = append(listeners, l)

		tcpAddr, ok := l.Addr().(*net.TCPAddr)
		if !ok {
			return nil, fmt.Errorf("%w: %T", ErrUnexpectedAddrType, l.Addr())
		}

		ports = append(ports, uint32(tcpAddr.Port))
	}

	return ports, nil
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

// processOutput returns the writer to use as a server process's stdout/stderr.
// A nil logger discards the output: Logger(nil) is an explicit request for silence,
// so it must not fall back to os.Stdout (which would re-enable the noisiest stream).
func processOutput(logger io.Writer) io.Writer {
	if logger == nil {
		return io.Discard
	}

	return logger
}

// sharedOutput returns a writer that is safe to hand to several server processes at once.
//
// For a writer that is not an *os.File, os/exec copies each child's output through its
// own goroutine, so N processes sharing one writer means N goroutines calling Write
// concurrently. Most writers (bytes.Buffer, testing adapters) are not safe for that,
// so they are wrapped in a mutex. *os.File is handed to the child as a file descriptor
// (no copy goroutine), and io.Discard is stateless, so both pass through unchanged.
func sharedOutput(logger io.Writer) io.Writer {
	out := processOutput(logger)

	if _, ok := out.(*os.File); ok || out == io.Discard {
		return out
	}

	return &syncWriter{mu: sync.Mutex{}, w: out}
}

// syncWriter serializes Write calls to an underlying writer.
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.w.Write(p) //nolint:wrapcheck // transparent pass-through writer
}
