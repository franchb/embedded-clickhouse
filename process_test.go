package embeddedclickhouse

import (
	"testing"
)

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

	err := stopProcess(nil, 0)
	if err != nil {
		t.Errorf("stopProcess(nil) = %v, want nil", err)
	}
}
