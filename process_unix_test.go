//go:build !windows

package embeddedclickhouse

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitErrOf runs a shell snippet and returns the error from cmd.Wait.
func waitErrOf(t *testing.T, script string) error {
	t.Helper()

	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}

	cmd := exec.Command("sh", "-c", script)
	require.NoError(t, cmd.Start())

	return cmd.Wait() //nolint:wrapcheck // the raw *exec.ExitError is what is under test
}

func TestClassifyWaitErr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		script  string
		wantErr bool
	}{
		{name: "clean exit", script: "exit 0", wantErr: false},
		{name: "killed by SIGTERM", script: "kill -TERM $$", wantErr: false},
		{name: "exit status 143", script: "exit 143", wantErr: false},
		{name: "crashed with SIGSEGV", script: "kill -SEGV $$", wantErr: true},
		{name: "aborted with SIGABRT", script: "kill -ABRT $$", wantErr: true},
		{name: "killed by SIGKILL (e.g. OOM killer)", script: "kill -KILL $$", wantErr: true},
		{name: "non-zero exit", script: "exit 3", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := classifyWaitErr(waitErrOf(t, tc.script))
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
